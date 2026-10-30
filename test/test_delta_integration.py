"""Real Delta tables: the MERGE statements, idempotent rejects, the country split, reconcile.
Skipped automatically when the Spark session cannot create Delta tables.
Needs:  pip install delta-spark==3.2.0  (with pyspark 3.5.x)  or a Databricks cluster."""
from datetime import date, datetime

import pytest

from helpers import (
    bronze_redemption, make_bronze_redemptions, run_redemption_pipeline, txn,
)
from pipeline_utils.silver import country_tables as ct
from pipeline_utils.silver import process_redemptions_batch as prb

pytestmark = pytest.mark.delta

D1, D2, D3 = date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 10)


# ===================================================== silver_redemptions upsert

RED_SCHEMA = (
    "member_id STRING, txn_id STRING, txn_date DATE, partner STRING, miles_redeemed INT, status STRING, "
    "feed_date DATE, is_member_id_orphan BOOLEAN, file_date DATE, ingestion_timestamp TIMESTAMP, "
    "source_file_name STRING"
)


def red_row(member_id="1", txn_id="T1", miles=100, status="PENDING", file_date=D1, orphan=False):
    return (member_id, txn_id, date(2024, 3, 15), "AMAZON", miles, status, date(2024, 6, 1),
            orphan, file_date, datetime(2024, 6, 1, 10), "f.json")


@pytest.fixture()
def red_env(delta_spark, db, monkeypatch):
    delta_spark.sql(f"""CREATE TABLE {db}.silver_redemptions (
        member_id STRING, txn_id STRING, txn_date DATE, partner STRING, miles_redeemed INT, status STRING,
        feed_date DATE, is_member_id_orphan BOOLEAN, file_date DATE, ingestion_timestamp TIMESTAMP,
        source_file_name STRING) USING DELTA""")
    delta_spark.sql(f"""CREATE TABLE {db}.redemption_rejects (
        member_id STRING, reject_reason STRING, _rescued_data STRING, source_file_name STRING,
        file_date DATE, ingestion_timestamp TIMESTAMP, rejected_at TIMESTAMP) USING DELTA""")
    monkeypatch.setattr(prb, "REDEMPTIONS_TABLE", f"{db}.silver_redemptions")
    monkeypatch.setattr(prb, "REJECTS_TABLE", f"{db}.redemption_rejects")
    return delta_spark, db


def red_table(spark, db):
    return {(r["member_id"], r["txn_id"]): r.asDict() for r in spark.table(f"{db}.silver_redemptions").collect()}


def upsert(spark, *rows):
    prb.upsert_redemptions(spark, spark.createDataFrame(list(rows), RED_SCHEMA))


def test_upsert_inserts_new_rows(red_env):
    spark, db = red_env
    upsert(spark, red_row(txn_id="T1"), red_row(txn_id="T2"))
    assert set(red_table(spark, db)) == {("1", "T1"), ("1", "T2")}


def test_upsert_updates_when_newer_or_same_file_date(red_env):
    spark, db = red_env
    upsert(spark, red_row(miles=100, file_date=D1))
    upsert(spark, red_row(miles=200, file_date=D2))
    assert red_table(spark, db)[("1", "T1")]["miles_redeemed"] == 200
    upsert(spark, red_row(miles=300, file_date=D2))                  # same file_date: allowed (rerun safe)
    assert red_table(spark, db)[("1", "T1")]["miles_redeemed"] == 300


def test_upsert_ignores_older_file_date(red_env):
    spark, db = red_env
    upsert(spark, red_row(miles=500, file_date=D2))
    upsert(spark, red_row(miles=1, file_date=D1))
    row = red_table(spark, db)[("1", "T1")]
    assert row["miles_redeemed"] == 500 and row["file_date"] == D2


def test_upsert_rerun_is_idempotent(red_env):
    spark, db = red_env
    rows = [red_row(txn_id="T1"), red_row(txn_id="T2")]
    upsert(spark, *rows)
    first = red_table(spark, db)
    upsert(spark, *rows)
    assert red_table(spark, db) == first


def test_same_txn_id_for_two_members_are_two_rows(red_env):
    spark, db = red_env
    upsert(spark, red_row(member_id="1", txn_id="T1"), red_row(member_id="2", txn_id="T1"))
    upsert(spark, red_row(member_id="2", txn_id="T1", miles=7, file_date=D2))
    table = red_table(spark, db)
    assert len(table) == 2
    assert table[("1", "T1")]["miles_redeemed"] == 100 and table[("2", "T1")]["miles_redeemed"] == 7


def test_orphan_flag_can_be_cleared_by_a_later_file(red_env):
    spark, db = red_env
    upsert(spark, red_row(orphan=True, file_date=D1))
    upsert(spark, red_row(orphan=False, file_date=D2))
    assert red_table(spark, db)[("1", "T1")]["is_member_id_orphan"] is False


# ================================================================ rejects table

def _reject_df(spark):
    bronze = make_bronze_redemptions(spark, [bronze_redemption(txns=[txn("T1", status="BAD")])])
    validated = run_redemption_pipeline(bronze)
    return prb.build_reject_table_rows(validated.where("reject_reason != ''"))


def test_rejects_same_batch_id_is_written_once(red_env):
    spark, db = red_env
    prb.append_to_rejects_table(_reject_df(spark), batch_id=1)
    prb.append_to_rejects_table(_reject_df(spark), batch_id=1)       # retry of the same batch
    assert spark.table(f"{db}.redemption_rejects").count() == 1


def test_rejects_new_batch_id_appends(red_env):
    spark, db = red_env
    prb.append_to_rejects_table(_reject_df(spark), batch_id=1)
    prb.append_to_rejects_table(_reject_df(spark), batch_id=2)
    assert spark.table(f"{db}.redemption_rejects").count() == 2


# ============================================================== whole redemption batch

def test_redemption_batch_end_to_end_and_retry(red_env, monkeypatch):
    spark, db = red_env
    monkeypatch.setattr(prb, "read_known_member_ids",
                        lambda session: spark.createDataFrame([("1001",)], "member_id STRING"))
    bronze = make_bronze_redemptions(spark, [
        bronze_redemption(member_id="1001", txns=[txn("T1", miles="100"), txn("T2", miles="bad")], file_date=D1),
        bronze_redemption(member_id="1001", txns=[txn("T1", miles="150")], file_date=D2),     # newer duplicate
        bronze_redemption(member_id="2002", txns=[txn("T3")], file_date=D1),                  # orphan
    ])
    prb.load_redemption_batch_to_silver(bronze, batch_id=1)
    table = red_table(spark, db)
    assert set(table) == {("1001", "T1"), ("2002", "T3")}
    assert table[("1001", "T1")]["miles_redeemed"] == 150
    assert table[("1001", "T1")]["is_member_id_orphan"] is False
    assert table[("2002", "T3")]["is_member_id_orphan"] is True
    assert spark.table(f"{db}.redemption_rejects").count() == 1

    prb.load_redemption_batch_to_silver(bronze, batch_id=1)                 # the same batch again
    assert red_table(spark, db) == table
    assert spark.table(f"{db}.redemption_rejects").count() == 1

    older = make_bronze_redemptions(spark, [bronze_redemption(txns=[txn("T1", miles="1")], file_date=date(2024, 5, 1))])
    prb.load_redemption_batch_to_silver(older, batch_id=2)                  # late, older file
    assert red_table(spark, db)[("1001", "T1")]["miles_redeemed"] == 150


# ============================================================ country split (members)

LATEST_SCHEMA = (
    "member_name STRING, member_id STRING, enrollment_date DATE, last_flight_date DATE, tier_code STRING, "
    "agent_name STRING, state STRING, country_code STRING, post_code INT, dob DATE, is_active STRING, "
    "age INT, stale_member BOOLEAN, file_date DATE, ingestion_timestamp TIMESTAMP, source_file_name STRING"
)


def member(name, country="IN", file_date=D1, state="KA", member_id="1"):
    return (name, member_id, date(2020, 1, 1), date(2024, 5, 1), "GLD", "Agent", state, country, 560001,
            date(1985, 5, 3), "Y", 39, False, file_date, datetime(2024, 6, 1, 10), "m.csv")


@pytest.fixture()
def split_env(delta_spark, db, monkeypatch):
    spark = delta_spark
    spark.sql(f"CREATE TABLE {db}.member_country_lookup "
              f"(member_name STRING, member_id STRING, country_code STRING, file_date DATE) USING DELTA")
    tables = {"IN": f"{db}.member_in", "US": f"{db}.member_us", "UK": f"{db}.member_uk"}
    monkeypatch.setattr(ct, "MEMBER_LOOKUP_TABLE", f"{db}.member_country_lookup")
    monkeypatch.setattr(ct, "get_country_target_tables", lambda session: tables)
    return spark, tables, db


def split(spark, *members, **kwargs):
    return ct.split_into_country_tables(spark, spark.createDataFrame(list(members), LATEST_SCHEMA), **kwargs)


def names(spark, table):
    return {r["member_name"]: r.asDict() for r in spark.table(table).collect()} \
        if spark.catalog.tableExists(table) else {}


def lookup(spark, db):
    return {r["member_name"]: (r["country_code"], r["file_date"])
            for r in spark.table(f"{db}.member_country_lookup").collect()}


def test_new_members_go_to_their_country_and_the_lookup(split_env):
    spark, tables, db = split_env
    stats = split(spark, member("John", "IN"), member("Jane", "US"))
    assert (stats["rows_applied"], stats["movers"], stats["country_tables"]) == (2, 0, 2)
    assert set(names(spark, tables["IN"])) == {"John"} and set(names(spark, tables["US"])) == {"Jane"}
    assert lookup(spark, db) == {"John": ("IN", D1), "Jane": ("US", D1)}


def test_lookup_stores_member_id_for_the_redemption_orphan_check(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "IN", member_id="10"), member("Jane", "US", member_id="20"))
    ids = {r["member_name"]: r["member_id"] for r in spark.table(f"{db}.member_country_lookup").collect()}
    assert ids == {"John": "10", "Jane": "20"}
    split(spark, member("John", "US", D2, member_id="10"))               # a move keeps the id
    ids = {r["member_name"]: r["member_id"] for r in spark.table(f"{db}.member_country_lookup").collect()}
    assert ids["John"] == "10"


def test_mover_leaves_the_old_country(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "IN", D1))
    stats = split(spark, member("John", "US", D2))
    assert (stats["rows_applied"], stats["movers"]) == (1, 1)
    assert names(spark, tables["IN"]) == {} and set(names(spark, tables["US"])) == {"John"}
    assert lookup(spark, db)["John"] == ("US", D2)


def test_older_file_than_the_lookup_is_skipped(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "US", D2))
    stats = split(spark, member("John", "IN", D1))                    # late file from before the move
    assert stats["rows_applied"] == 0 and stats["country_tables"] == 0
    assert names(spark, tables["IN"]) == {} and set(names(spark, tables["US"])) == {"John"}
    assert lookup(spark, db)["John"] == ("US", D2)


def test_same_country_update_newer_wins_older_ignored(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "IN", D2, state="MH"))
    split(spark, member("John", "IN", D1, state="TN"))
    assert names(spark, tables["IN"])["John"]["state"] == "MH"
    split(spark, member("John", "IN", D3, state="GA"))
    assert names(spark, tables["IN"])["John"]["state"] == "GA" and len(names(spark, tables["IN"])) == 1


def test_rerun_of_the_same_batch_changes_nothing(split_env):
    spark, tables, db = split_env
    batch = [member("John", "IN"), member("Jane", "US")]
    split(spark, *batch)
    before = (names(spark, tables["IN"]), names(spark, tables["US"]), lookup(spark, db))
    split(spark, *batch)
    assert (names(spark, tables["IN"]), names(spark, tables["US"]), lookup(spark, db)) == before


def test_retry_after_a_crash_between_upsert_and_lookup_repairs_the_mover(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "IN", D1))
    # crash simulation: the new country row was written, the old row was not deleted, lookup not updated
    ct.create_country_table_if_missing(spark, tables["US"])
    spark.createDataFrame([member("John", "US", D2)], LATEST_SCHEMA).write.insertInto(tables["US"])
    assert "John" in names(spark, tables["IN"]) and "John" in names(spark, tables["US"])

    split(spark, member("John", "US", D2))                             # the retry
    assert names(spark, tables["IN"]) == {} and set(names(spark, tables["US"])) == {"John"}
    assert lookup(spark, db)["John"] == ("US", D2)


def test_unknown_country_fails_the_batch_before_anything_is_written(split_env):
    spark, tables, db = split_env
    with pytest.raises(ValueError, match="without a country table mapping"):
        split(spark, member("John", "IN"), member("Zed", "ZZ"))
    assert lookup(spark, db) == {} and names(spark, tables["IN"]) == {}


def test_many_countries_in_parallel_and_worker_override(split_env):
    spark, tables, db = split_env
    stats = split(spark, member("A", "IN"), member("B", "US"), member("C", "UK"))
    assert stats["country_tables"] == 3 and stats["workers"] == 3
    assert all(len(names(spark, t)) == 1 for t in tables.values())
    stats = split(spark, member("D", "IN"), member("E", "US"), max_workers=1)
    assert stats["workers"] == 1 and set(names(spark, tables["IN"])) == {"A", "D"}


def test_the_move_does_not_duplicate_unchanged_members(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "IN"))
    split(spark, member("John", "IN", D2))
    split(spark, member("John", "IN", D3))
    assert spark.table(tables["IN"]).count() == 1 and spark.table(f"{db}.member_country_lookup").count() == 1


# ---------------------------------------------------- the MERGE statements alone

def test_delete_mover_never_deletes_a_newer_row(split_env):
    spark, tables, db = split_env
    ct.create_country_table_if_missing(spark, tables["IN"])
    spark.createDataFrame([member("John", "IN", D3)], LATEST_SCHEMA).write.insertInto(tables["IN"])
    older = spark.createDataFrame([("John", D1)], "member_name STRING, file_date DATE")
    ct._merge_with_temp_view(spark, older, ct.DELETE_MOVER_SQL, tables["IN"])
    assert "John" in names(spark, tables["IN"])
    same = spark.createDataFrame([("John", D3)], "member_name STRING, file_date DATE")
    ct._merge_with_temp_view(spark, same, ct.DELETE_MOVER_SQL, tables["IN"])
    assert names(spark, tables["IN"]) == {}


def test_country_upsert_never_overwrites_a_newer_row(split_env):
    spark, tables, db = split_env
    ct.create_country_table_if_missing(spark, tables["IN"])
    spark.createDataFrame([member("John", "IN", D3, state="GA")], LATEST_SCHEMA).write.insertInto(tables["IN"])
    older = spark.createDataFrame([member("John", "IN", D1, state="TN")], LATEST_SCHEMA)
    ct._merge_with_temp_view(spark, older, ct.UPSERT_COUNTRY_SQL, tables["IN"])
    rows = names(spark, tables["IN"])
    assert len(rows) == 1 and rows["John"]["state"] == "GA"


def test_lookup_merge_never_overwrites_a_newer_row(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "US", D3))
    ct.update_member_country_lookup(spark, spark.createDataFrame([member("John", "IN", D1)], LATEST_SCHEMA))
    assert lookup(spark, db)["John"] == ("US", D3)


def test_temp_views_are_cleaned_up(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "IN"), member("Jane", "US"))
    assert [t.name for t in spark.catalog.listTables() if t.name.startswith("_split_src_")] == []


# --------------------------------------------------------------------- reconcile

def reconcile(spark):
    return {r["country_code"]: r.asDict() for r in ct.reconcile_lookup_with_country_tables(spark).collect()}


def test_reconcile_is_zero_after_normal_runs_and_skips_unused_countries(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "IN"), member("Jane", "US"), member("Kim", "IN"))
    split(spark, member("John", "US", D2))                              # John moves IN -> US
    result = reconcile(spark)
    assert set(result) == {"IN", "US"}                                  # UK never used
    assert all(r["difference"] == 0 for r in result.values())
    assert result["US"]["lookup_rows"] == 2 and result["US"]["table_rows"] == 2
    assert result["IN"]["lookup_rows"] == 1 and result["IN"]["table_rows"] == 1


def test_reconcile_detects_a_stray_row_and_an_unmapped_country(split_env):
    spark, tables, db = split_env
    split(spark, member("John", "IN"))
    spark.createDataFrame([member("Stray", "IN")], LATEST_SCHEMA).write.insertInto(tables["IN"])
    spark.sql(f"INSERT INTO {db}.member_country_lookup (member_name, country_code, file_date) "
              f"VALUES ('Ghost', 'ZZ', DATE'2024-06-01')")
    result = reconcile(spark)
    assert result["IN"]["difference"] == 1
    assert result["ZZ"]["country_table"] is None and result["ZZ"]["lookup_rows"] == 1