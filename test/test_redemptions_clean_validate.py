"""clean_and_validate_redemptions: flatten, trim, convert, validate, dedupe, orphan flag."""
import json
from datetime import date, datetime

import pytest

from helpers import (
    FILE_DATE, INGEST_TS, bronze_redemption, make_bronze_redemptions, one_redemption,
    reasons, run_redemption_pipeline, txn,
)
from pipeline_utils.silver import clean_and_validate_redemptions as cr


# ------------------------------------------------------------------- flatten

def test_flatten_one_row_per_transaction(spark):
    df = make_bronze_redemptions(spark, [bronze_redemption(txns=[txn("T1"), txn("T2"), txn("T3")])])
    out = cr.flatten_redemptions(df)
    assert out.count() == 3
    assert {r["txn_id"] for r in out.collect()} == {"T1", "T2", "T3"}
    assert set(out.columns) == set(cr.BRONZE_COLS)


def test_flatten_empty_and_null_arrays_give_no_rows(spark):
    df = make_bronze_redemptions(spark, [
        bronze_redemption(member_id="1", txns=[]),
        bronze_redemption(member_id="2", txns=[txn("T9")]),
    ])
    assert [r["member_id"] for r in cr.flatten_redemptions(df).collect()] == ["2"]

    null_array = spark.createDataFrame([("3", "01062024", None, None, "f", FILE_DATE, INGEST_TS)],
                                       df.schema)
    assert cr.flatten_redemptions(null_array).count() == 0


def test_flatten_keeps_header_columns_on_every_transaction(spark):
    df = make_bronze_redemptions(spark, [bronze_redemption(txns=[txn("T1"), txn("T2")], source="x.json")])
    for r in cr.flatten_redemptions(df).collect():
        assert (r["member_id"], r["feed_date"], r["source_file_name"], r["file_date"]) == \
               ("1001", "01062024", "x.json", FILE_DATE)


# ---------------------------------------------------------------- trim / blank

def test_trim_and_blank_to_null(spark):
    row = one_redemption(spark, txn_id="  T1  ", partner="   ", status=" COMPLETED ")
    assert row["txn_id"] == "T1"
    assert row["partner"] is None
    assert row["status"] == "COMPLETED"


def test_helper_text_columns_exist_before_validation_and_bronze_row_is_dropped_after(spark):
    df = make_bronze_redemptions(spark, [bronze_redemption()])
    trimmed = df.transform(cr.flatten_redemptions).transform(cr.trim_and_rename_bronze_columns)
    assert {"txn_date_text", "feed_date_text", "miles_redeemed_text", "bronze_row"} <= set(trimmed.columns)
    validated = run_redemption_pipeline(df)
    assert "bronze_row" not in validated.columns


# ------------------------------------------------------------ date conversion

@pytest.mark.parametrize("text, expected", [
    ("15032024", date(2024, 3, 15)),   # ddMMyyyy
    ("20240315", date(2024, 3, 15)),   # yyyyMMdd
    ("03152024", date(2024, 3, 15)),   # MMddyyyy
    ("01022024", date(2024, 2, 1)),    # ambiguous: ddMMyyyy wins over MMddyyyy
    ("29022024", date(2024, 2, 29)),   # leap day
])
def test_txn_date_valid_formats(spark, text, expected):
    row = one_redemption(spark, txn_date=text)
    assert row["txn_date"] == expected
    assert row["reject_reason"] == ""


@pytest.mark.parametrize("text", [
    "31022024",      # 31 Feb
    "29022023",      # 29 Feb, not a leap year
    "2024-03-15",    # not 8 digits
    "1503202",       # 7 digits
    "150320244",     # 9 digits
    "abcdefgh",
    "15031899",      # before MIN_YEAR
    "15032999",      # in the future
    "00012024",      # day 0
])
def test_txn_date_invalid_is_rejected(spark, text):
    row = one_redemption(spark, txn_date=text)
    assert row["txn_date"] is None
    assert reasons(row) == {"txn_date:INVALID_DATE"}


def test_feed_date_invalid_is_rejected(spark):
    df = make_bronze_redemptions(spark, [bronze_redemption(feed_date="99999999")])
    row = run_redemption_pipeline(df).collect()[0]
    assert reasons(row) == {"feed_date:INVALID_DATE"}


# ----------------------------------------------------------- miles conversion

@pytest.mark.parametrize("text, expected", [("100", 100), ("0", 0), ("2147483647", 2147483647)])
def test_miles_valid(spark, text, expected):
    row = one_redemption(spark, miles=text)
    assert row["miles_redeemed"] == expected and row["reject_reason"] == ""


@pytest.mark.parametrize("text", ["abc", "12.5", "1,000", "99999999999", "2147483648", "1e3"])
def test_miles_not_an_int_is_rejected(spark, text):
    row = one_redemption(spark, miles=text)
    assert row["miles_redeemed"] is None
    assert reasons(row) == {"miles_redeemed:INVALID_INT"}


def test_miles_negative_gets_its_own_reason(spark):
    row = one_redemption(spark, miles="-5")
    assert row["miles_redeemed"] == -5
    assert reasons(row) == {"miles_redeemed:NEGATIVE_VALUE"}


# ------------------------------------------------------------------ mandatory

@pytest.mark.parametrize("override, expected", [
    ({"txn_id": None}, "txn_id:MISSING"),
    ({"txn_id": "   "}, "txn_id:MISSING"),
    ({"txn_date": None}, "txn_date:MISSING"),
    ({"partner": None}, "partner:MISSING"),
    ({"miles": None}, "miles_redeemed:MISSING"),
    ({"status": None}, "status:MISSING"),
])
def test_missing_transaction_field(spark, override, expected):
    assert reasons(one_redemption(spark, **override)) == {expected}


@pytest.mark.parametrize("header, expected", [
    ({"member_id": None}, "member_id:MISSING"),
    ({"feed_date": None}, "feed_date:MISSING"),
    ({"file_date": None}, "file_date:MISSING"),
    ({"ingestion_ts": None}, "ingestion_timestamp:MISSING"),
    ({"source": None}, "source_file_name:MISSING"),
])
def test_missing_header_field(spark, header, expected):
    df = make_bronze_redemptions(spark, [bronze_redemption(**header)])
    assert reasons(run_redemption_pipeline(df).collect()[0]) == {expected}


# ---------------------------------------------------------------- max length

def test_too_long_values(spark):
    assert reasons(one_redemption(spark, txn_id="A" * 101)) == {"txn_id:TOO_LONG"}
    assert reasons(one_redemption(spark, txn_id="A" * 100)) == set()
    assert reasons(one_redemption(spark, partner="P" * 256)) == {"partner:TOO_LONG"}
    assert reasons(one_redemption(spark, partner="P" * 255)) == set()
    assert "status:TOO_LONG" in reasons(one_redemption(spark, status="S" * 51))

    df = make_bronze_redemptions(spark, [bronze_redemption(member_id="1" * 19)])
    assert reasons(run_redemption_pipeline(df).collect()[0]) == {"member_id:TOO_LONG"}
    df = make_bronze_redemptions(spark, [bronze_redemption(member_id="1" * 18)])
    assert run_redemption_pipeline(df).collect()[0]["reject_reason"] == ""


# -------------------------------------------------------------------- formats

@pytest.mark.parametrize("member_id", ["12AB", "10 01", "-1", "1.5"])
def test_member_id_must_be_digits(spark, member_id):
    df = make_bronze_redemptions(spark, [bronze_redemption(member_id=member_id)])
    assert reasons(run_redemption_pipeline(df).collect()[0]) == {"member_id:INVALID_FORMAT"}


@pytest.mark.parametrize("txn_id, ok", [
    ("T1", True), ("TXN-1_a", True), ("123", True),
    ("TXN 1", False), ("TXN#1", False), ("TXN/1", False),
])
def test_txn_id_format(spark, txn_id, ok):
    expected = set() if ok else {"txn_id:INVALID_FORMAT"}
    assert reasons(one_redemption(spark, txn_id=txn_id)) == expected


# --------------------------------------------------------------------- status

@pytest.mark.parametrize("status", ["PENDING", "COMPLETED", "REJECTED"])
def test_valid_status(spark, status):
    assert one_redemption(spark, status=status)["reject_reason"] == ""


@pytest.mark.parametrize("status", ["DONE", "completed", "Pending", "COMPLETE"])
def test_invalid_status_is_case_sensitive_exact_match(spark, status):
    assert reasons(one_redemption(spark, status=status)) == {"status:INVALID_VALUE"}


# ----------------------------------------------------------- reject_reason/json

def test_valid_row_has_empty_reason_and_no_json(spark):
    row = one_redemption(spark)
    assert row["reject_reason"] == "" and row["bronze_row_json"] is None


def test_several_failures_are_all_reported(spark):
    row = one_redemption(spark, txn_date="xx", miles="-5", status="DONE", partner=None)
    assert reasons(row) == {
        "txn_date:INVALID_DATE", "miles_redeemed:NEGATIVE_VALUE",
        "status:INVALID_VALUE", "partner:MISSING",
    }
    assert row["reject_reason"].count("; ") == 3


def test_reject_json_keeps_raw_untrimmed_values_and_null_keys(spark):
    row = one_redemption(spark, txn_id=" T1 ", status=" BAD ", partner=None)
    payload = json.loads(row["bronze_row_json"])
    assert payload["txn_id"] == " T1 " and payload["status"] == " BAD "
    assert "partner" in payload and payload["partner"] is None   # ignoreNullFields=false
    assert payload["member_id"] == "1001"


def test_only_bad_transactions_of_a_member_are_rejected(spark):
    df = make_bronze_redemptions(spark, [bronze_redemption(txns=[txn("T1"), txn("T2", miles="x")])])
    out = {r["txn_id"]: r["reject_reason"] for r in run_redemption_pipeline(df).collect()}
    assert out == {"T1": "", "T2": "miles_redeemed:INVALID_INT"}


# ------------------------------------------------------------------ dedupe

def _latest(spark, bronze_rows):
    validated = run_redemption_pipeline(make_bronze_redemptions(spark, bronze_rows))
    passed = validated.where("reject_reason = ''")
    return cr.keep_latest_record_per_transaction(passed)


def test_latest_by_file_date(spark):
    out = _latest(spark, [
        bronze_redemption(txns=[txn("T1", miles="100")], file_date=date(2024, 6, 1)),
        bronze_redemption(txns=[txn("T1", miles="300")], file_date=date(2024, 6, 3)),
        bronze_redemption(txns=[txn("T1", miles="200")], file_date=date(2024, 6, 2)),
    ]).collect()
    assert len(out) == 1 and out[0]["miles_redeemed"] == 300


def test_latest_by_ingestion_timestamp_when_file_date_ties(spark):
    out = _latest(spark, [
        bronze_redemption(txns=[txn("T1", miles="100")], ingestion_ts=datetime(2024, 6, 1, 9)),
        bronze_redemption(txns=[txn("T1", miles="200")], ingestion_ts=datetime(2024, 6, 1, 11)),
    ]).collect()
    assert len(out) == 1 and out[0]["miles_redeemed"] == 200


def test_complete_tie_is_deterministic_whatever_the_input_order(spark):
    a = bronze_redemption(txns=[txn("T1", miles="100", partner="AAA")])
    b = bronze_redemption(txns=[txn("T1", miles="200", partner="BBB")])
    first = _latest(spark, [a, b]).collect()
    second = _latest(spark, [b, a]).collect()
    assert len(first) == len(second) == 1
    assert first[0].asDict() == second[0].asDict()


def test_duplicate_inside_one_array_is_collapsed(spark):
    out = _latest(spark, [bronze_redemption(txns=[txn("T1", miles="1"), txn("T1", miles="2")])]).collect()
    assert len(out) == 1


def test_key_is_member_id_plus_txn_id(spark):
    out = _latest(spark, [
        bronze_redemption(member_id="1", txns=[txn("T1"), txn("T2")]),
        bronze_redemption(member_id="2", txns=[txn("T1")]),
    ]).collect()
    assert {(r["member_id"], r["txn_id"]) for r in out} == {("1", "T1"), ("1", "T2"), ("2", "T1")}


def test_latest_output_shape(spark):
    out = _latest(spark, [bronze_redemption()])
    assert set(out.columns) == {
        "member_id", "txn_id", "txn_date", "partner", "miles_redeemed", "status", "feed_date",
        "file_date", "ingestion_timestamp", "source_file_name",
    }


# -------------------------------------------------------------------- orphans

def _ids(spark, values):
    return spark.createDataFrame([(v,) for v in values], "member_id STRING")


def test_orphan_flag(spark):
    latest = _latest(spark, [bronze_redemption(member_id="1", txns=[txn("T1")]),
                             bronze_redemption(member_id="2", txns=[txn("T2")])])
    out = {r["member_id"]: r["is_member_id_orphan"]
           for r in cr.mark_orphan_members(latest, _ids(spark, ["1"])).collect()}
    assert out == {"1": False, "2": True}


def test_orphan_known_ids_with_duplicates_and_nulls_do_not_duplicate_rows(spark):
    latest = _latest(spark, [bronze_redemption(member_id="1", txns=[txn("T1")])])
    out = cr.mark_orphan_members(latest, _ids(spark, ["1", "1", None, "1"])).collect()
    assert len(out) == 1 and out[0]["is_member_id_orphan"] is False


def test_orphan_with_empty_member_table_marks_everything(spark):
    latest = _latest(spark, [bronze_redemption(member_id="1", txns=[txn("T1"), txn("T2")])])
    out = cr.mark_orphan_members(latest, _ids(spark, [])).collect()
    assert len(out) == 2 and all(r["is_member_id_orphan"] for r in out)


def test_orphan_null_in_known_ids_does_not_match_anything(spark):
    latest = _latest(spark, [bronze_redemption(member_id="7", txns=[txn("T1")])])
    out = cr.mark_orphan_members(latest, _ids(spark, [None])).collect()
    assert out[0]["is_member_id_orphan"] is True
