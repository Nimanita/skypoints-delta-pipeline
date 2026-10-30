"""The two foreachBatch functions, with the table writers replaced by recorders.
Checks the wiring: what goes to rejects, what goes to the upsert / split, the order, the counts,
and that the caches are released even when a write fails. (Real table behaviour: test_delta_integration.py)"""
import time
from datetime import date
from unittest.mock import MagicMock

import pytest

from helpers import (
    FILE_DATE, bronze_member, bronze_redemption, known_countries, make_bronze_members,
    make_bronze_redemptions, run_redemption_pipeline, txn,
)
from pipeline_utils.silver import process_member_batch as pmb
from pipeline_utils.silver import process_redemptions_batch as prb


def _cache_is_empty(spark, wait=5.0):
    manager = spark._jsparkSession.sharedState().cacheManager()
    deadline = time.time() + wait
    while time.time() < deadline:            # unpersist() is not blocking
        if manager.isEmpty():
            return True
        time.sleep(0.1)
    return manager.isEmpty()


# =========================================================== redemptions batch

@pytest.fixture()
def rec(monkeypatch, spark):
    """Replace the writers and the member lookup; `calls` records what the batch did, in order."""
    calls = []
    monkeypatch.setattr(prb, "append_to_rejects_table",
                        lambda df, batch_id: calls.append(("rejects", batch_id, [r.asDict() for r in df.collect()])))
    monkeypatch.setattr(prb, "upsert_redemptions",
                        lambda session, df: calls.append(("upsert", [r.asDict() for r in df.collect()])))
    monkeypatch.setattr(prb, "read_known_member_ids",
                        lambda session: spark.createDataFrame([("1001",)], "member_id STRING"))
    return calls


def test_redemption_batch_splits_good_and_bad_rows(spark, rec):
    bronze = make_bronze_redemptions(spark, [
        bronze_redemption(member_id="1001", txns=[txn("T1"), txn("T2", miles="oops")]),
        bronze_redemption(member_id="2002", txns=[txn("T3")]),            # unknown member -> orphan
    ])
    prb.load_redemption_batch_to_silver(bronze, batch_id=7)

    (kind1, batch_id, rejects), (kind2, upserted) = rec
    assert (kind1, batch_id, kind2) == ("rejects", 7, "upsert")           # rejects first, batch_id passed on
    assert [r["reject_reason"] for r in rejects] == ["miles_redeemed:INVALID_INT"]
    assert rejects[0]["member_id"] == "1001" and '"txn_id":"T2"' in rejects[0]["_rescued_data"]
    assert {(r["member_id"], r["txn_id"], r["is_member_id_orphan"]) for r in upserted} == {
        ("1001", "T1", False), ("2002", "T3", True)}


def test_redemption_batch_dedupes_before_the_upsert(spark, rec):
    bronze = make_bronze_redemptions(spark, [
        bronze_redemption(txns=[txn("T1", miles="100")], file_date=date(2024, 6, 1)),
        bronze_redemption(txns=[txn("T1", miles="999")], file_date=date(2024, 6, 2)),
    ])
    prb.load_redemption_batch_to_silver(bronze, batch_id=1)
    upserted = rec[1][1]
    assert len(upserted) == 1 and upserted[0]["miles_redeemed"] == 999


def test_redemption_batch_all_rejected_still_runs_with_empty_upsert(spark, rec):
    bronze = make_bronze_redemptions(spark, [bronze_redemption(txns=[txn("T1", status="NOPE")])])
    prb.load_redemption_batch_to_silver(bronze, batch_id=2)
    assert len(rec[0][2]) == 1 and rec[1][1] == []


def test_redemption_batch_with_no_transactions_does_not_fail(spark, rec):
    bronze = make_bronze_redemptions(spark, [bronze_redemption(txns=[])])
    prb.load_redemption_batch_to_silver(bronze, batch_id=3)
    assert rec[0][2] == [] and rec[1][1] == []


def test_redemption_batch_prints_the_counts(spark, rec, capsys):
    bronze = make_bronze_redemptions(spark, [
        bronze_redemption(member_id="1001", txns=[txn("T1"), txn("T1", miles="5"), txn("T2", miles="x")]),
        bronze_redemption(member_id="2002", txns=[txn("T3")]),
    ])
    prb.load_redemption_batch_to_silver(bronze, batch_id=9)
    out = capsys.readouterr().out
    assert ("batch 9: transactions in=4 | rejected=1 | superseded duplicates=1 | "
            "upserted=2 | orphan member_id=1") in out


def test_redemption_batch_same_input_same_output(spark, rec):
    bronze = make_bronze_redemptions(spark, [bronze_redemption(txns=[txn("T1"), txn("T2")])])
    prb.load_redemption_batch_to_silver(bronze, batch_id=4)
    prb.load_redemption_batch_to_silver(bronze, batch_id=4)
    key = lambda r: (r["member_id"], r["txn_id"])
    assert sorted(rec[1][1], key=key) == sorted(rec[3][1], key=key)       # a retry sends the same rows


def test_redemption_batch_releases_caches(spark, rec):
    prb.load_redemption_batch_to_silver(
        make_bronze_redemptions(spark, [bronze_redemption()]), batch_id=5)
    assert _cache_is_empty(spark)


def test_redemption_batch_failure_in_rejects_stops_before_upsert_and_releases_caches(spark, monkeypatch, rec):
    def broken(df, batch_id):
        raise RuntimeError("rejects write failed")
    monkeypatch.setattr(prb, "append_to_rejects_table", broken)
    with pytest.raises(RuntimeError, match="rejects write failed"):
        prb.load_redemption_batch_to_silver(
            make_bronze_redemptions(spark, [bronze_redemption()]), batch_id=6)
    assert not any(c[0] == "upsert" for c in rec)
    assert _cache_is_empty(spark)


def test_redemption_batch_failure_in_upsert_releases_caches(spark, monkeypatch, rec):
    def broken(session, df):
        raise RuntimeError("merge failed")
    monkeypatch.setattr(prb, "upsert_redemptions", broken)
    with pytest.raises(RuntimeError, match="merge failed"):
        prb.load_redemption_batch_to_silver(
            make_bronze_redemptions(spark, [bronze_redemption()]), batch_id=6)
    assert _cache_is_empty(spark)


# --------------------------------------------------- redemption helper functions

def test_build_reject_table_rows_columns(spark):
    validated = run_redemption_pipeline(
        make_bronze_redemptions(spark, [bronze_redemption(txns=[txn("T1", status="BAD")])]))
    out = prb.build_reject_table_rows(validated.where("reject_reason != ''"))
    assert out.columns == ["member_id", "reject_reason", "_rescued_data", "source_file_name",
                           "file_date", "ingestion_timestamp", "rejected_at"]
    row = out.collect()[0]
    assert row["reject_reason"] == "status:INVALID_VALUE" and '"txn_id":"T1"' in row["_rescued_data"]
    assert row["rejected_at"] is not None


def test_append_to_rejects_table_is_idempotent_by_batch_id():
    df = MagicMock()
    prb.append_to_rejects_table(df, batch_id=42)
    writer = df.write.format.return_value.mode.return_value
    df.write.format.assert_called_with("delta")
    df.write.format.return_value.mode.assert_called_with("append")
    writer.option.assert_any_call("txnAppId", prb.REJECTS_APP_ID)
    writer.option.return_value.option.assert_any_call("txnVersion", 42)
    writer.option.return_value.option.return_value.saveAsTable.assert_called_with(prb.REJECTS_TABLE)


def test_upsert_redemptions_runs_merge_and_drops_the_temp_view(spark):
    latest = spark.createDataFrame(
        [("1", "T1", None, "P", 1, "PENDING", None, False, FILE_DATE, None, "f")],
        "member_id STRING, txn_id STRING, txn_date DATE, partner STRING, miles_redeemed INT, status STRING, "
        "feed_date DATE, is_member_id_orphan BOOLEAN, file_date DATE, ingestion_timestamp TIMESTAMP, "
        "source_file_name STRING")
    session = MagicMock()
    prb.upsert_redemptions(session, latest)
    sql = session.sql.call_args[0][0]
    assert prb.REDEMPTIONS_TABLE in sql
    assert "ON t.member_id = s.member_id AND t.txn_id = s.txn_id" in sql
    assert "s.file_date >= t.file_date" in sql
    view = session.catalog.dropTempView.call_args[0][0]
    assert view in sql and view.startswith("_redemptions_src_")


def test_upsert_redemptions_drops_the_view_even_when_the_merge_fails(spark):
    latest = spark.createDataFrame(
        [("1", "T1", None, "P", 1, "PENDING", None, False, FILE_DATE, None, "f")],
        "member_id STRING, txn_id STRING, txn_date DATE, partner STRING, miles_redeemed INT, status STRING, "
        "feed_date DATE, is_member_id_orphan BOOLEAN, file_date DATE, ingestion_timestamp TIMESTAMP, "
        "source_file_name STRING")
    session = MagicMock()
    session.sql.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        prb.upsert_redemptions(session, latest)
    session.catalog.dropTempView.assert_called_once()


def test_redemption_columns_match_the_silver_table_order():
    assert prb.REDEMPTION_COLS == [
        "member_id", "txn_id", "txn_date", "partner", "miles_redeemed", "status", "feed_date",
        "is_member_id_orphan", "file_date", "ingestion_timestamp", "source_file_name"]


def test_read_known_member_ids_reads_only_the_lookup(spark, monkeypatch):
    spark.createDataFrame([("a", "10", "IN", FILE_DATE), ("b", "20", "US", FILE_DATE)],
                          "member_name STRING, member_id STRING, country_code STRING, file_date DATE"
                          ).createOrReplaceTempView("lookup_test")
    monkeypatch.setattr(prb, "MEMBER_LOOKUP_TABLE", "lookup_test")
    out = prb.read_known_member_ids(spark)
    assert out.columns == ["member_id"]
    assert {r["member_id"] for r in out.collect()} == {"10", "20"}
    spark.catalog.dropTempView("lookup_test")


# ================================================================ member batch

@pytest.fixture()
def mrec(monkeypatch, spark):
    calls = []
    monkeypatch.setattr(pmb, "read_known_country_codes", lambda session: known_countries(spark, ["IN", "US"]))
    monkeypatch.setattr(pmb, "append_to_rejects_table",
                        lambda df, batch_id: calls.append(("rejects", batch_id, [r.asDict() for r in df.collect()])))

    def fake_split(session, latest_df):
        calls.append(("split", [r.asDict() for r in latest_df.collect()]))
        return {"rows_applied": latest_df.count(), "movers": 0, "country_tables": 1,
                "workers": 1, "merge_seconds": 0.1}
    monkeypatch.setattr(pmb, "split_into_country_tables", fake_split)
    return calls


def test_member_batch_rejects_then_split(spark, mrec, capsys):
    bronze = make_bronze_members(spark, [
        bronze_member(member_name="John Smith"),
        bronze_member(member_name="Jane Doe", member_id="2", state="K1"),            # bad
        bronze_member(member_name="John Smith", file_date=date(2024, 6, 5)),          # newer duplicate
    ])
    pmb.load_member_batch_to_silver(bronze, batch_id=11)

    assert [c[0] for c in mrec] == ["rejects", "split"]
    assert mrec[0][1] == 11
    assert [r["reject_reason"] for r in mrec[0][2]] == ["state:INVALID_FORMAT"]
    split_rows = mrec[1][1]
    assert len(split_rows) == 1 and split_rows[0]["file_date"] == date(2024, 6, 5)
    assert {"age", "stale_member"} <= set(split_rows[0])
    out = capsys.readouterr().out
    assert "batch 11: in=3 | rejected=1 | superseded duplicates=1" in out
    assert "applied=1" in out


def test_member_batch_all_rejected(spark, mrec):
    bronze = make_bronze_members(spark, [bronze_member(country="FR")])
    pmb.load_member_batch_to_silver(bronze, batch_id=12)
    assert mrec[0][2][0]["reject_reason"] == "country_code:NOT_IN_REFERENCE"
    assert mrec[1][1] == []


def test_member_batch_releases_caches_on_failure(spark, monkeypatch, mrec):
    def broken(session, latest_df):
        raise RuntimeError("split failed")
    monkeypatch.setattr(pmb, "split_into_country_tables", broken)
    with pytest.raises(RuntimeError, match="split failed"):
        pmb.load_member_batch_to_silver(make_bronze_members(spark, [bronze_member()]), batch_id=13)
    assert _cache_is_empty(spark)


def test_member_build_reject_table_rows_columns(spark):
    from helpers import run_member_pipeline
    validated = run_member_pipeline(make_bronze_members(spark, [bronze_member(tier="XXX")]),
                                    known_countries(spark, ["IN"]))
    out = pmb.build_reject_table_rows(validated.where("reject_reason != ''"))
    assert out.columns == ["member_name", "member_id", "reject_reason", "_rescued_data",
                           "source_file_name", "file_date", "ingestion_timestamp", "rejected_at"]
