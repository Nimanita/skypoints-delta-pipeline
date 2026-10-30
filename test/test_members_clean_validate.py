"""clean_and_validate (members): trim, convert, validate, derived columns, latest per member."""
from datetime import date, datetime

import pytest

from helpers import (
    FILE_DATE, bronze_member, known_countries, make_bronze_members, one_member, reasons,
    run_member_pipeline,
)
from pipeline_utils.silver import clean_and_validate_member as cm
from pipeline_utils.silver.country_tables import CLEAN_COLS


# ---------------------------------------------------------------- trim / rename

def test_trim_blank_to_null_and_rename(spark):
    row = one_member(spark, member_name="  John Smith ", agent_name="   ", tier=" GLD ", country=" IN ")
    assert row["member_name"] == "John Smith"
    assert row["agent_name"] is None
    assert row["tier_code"] == "GLD"
    assert row["country_code"] == "IN"          # trimmed, then matched against the list
    assert row["reject_reason"] == ""


def test_valid_row_has_no_reason_and_no_json(spark):
    row = one_member(spark)
    assert row["reject_reason"] == "" and row["bronze_row_json"] is None
    assert "bronze_row" not in one_member(spark).asDict() and "is_known_country" not in row.asDict()


# ---------------------------------------------------------------------- dates

@pytest.mark.parametrize("text, expected", [
    ("15032020", date(2020, 3, 15)), ("20200315", date(2020, 3, 15)),
    ("03152020", date(2020, 3, 15)), ("01022020", date(2020, 2, 1)),
])
def test_enrollment_date_formats(spark, text, expected):
    assert one_member(spark, enrollment_date=text)["enrollment_date"] == expected


@pytest.mark.parametrize("text", ["31022020", "2020-01-01", "xx", "15031899", "15032999"])
def test_invalid_enrollment_date(spark, text):
    assert reasons(one_member(spark, enrollment_date=text)) == {"enrollment_date:INVALID_DATE"}


def test_dob_loses_leading_zero_is_padded(spark):
    assert one_member(spark, dob="3051985")["dob"] == date(1985, 5, 3)      # 03051985
    assert one_member(spark, dob="03051985")["dob"] == date(1985, 5, 3)


def test_dob_padding_does_not_cut_long_values(spark):
    assert reasons(one_member(spark, dob="030519851")) == {"dob:INVALID_DATE"}   # 9 digits


@pytest.mark.parametrize("field", ["dob", "last_flight_date"])
def test_optional_dates_may_be_blank(spark, field):
    row = one_member(spark, **{field: None})
    assert row["reject_reason"] == ""
    assert row[field] is None


@pytest.mark.parametrize("field", ["dob", "last_flight_date"])
def test_optional_dates_invalid_when_filled(spark, field):
    assert reasons(one_member(spark, **{field: "99999999"})) == {f"{field}:INVALID_DATE"}


# ------------------------------------------------------------------ post code

def test_post_code(spark):
    assert one_member(spark, post_code="560001")["post_code"] == 560001
    assert one_member(spark, post_code=None)["reject_reason"] == ""          # optional


@pytest.mark.parametrize("text", ["abc", "56 0001", "-1", "12345678901234", "2147483648"])
def test_post_code_invalid(spark, text):
    row = one_member(spark, post_code=text)
    assert row["post_code"] is None
    assert reasons(row) == {"post_code:INVALID_INT"}


# ------------------------------------------------------------------- country

def test_country_known_and_unknown(spark):
    assert one_member(spark, country="US")["reject_reason"] == ""
    assert reasons(one_member(spark, country="FR")) == {"country_code:NOT_IN_REFERENCE"}


def test_country_match_is_case_sensitive(spark):
    assert reasons(one_member(spark, country="in")) == {"country_code:NOT_IN_REFERENCE"}


def test_repeated_code_in_reference_table_does_not_duplicate_members(spark):
    df = make_bronze_members(spark, [bronze_member()])
    out = run_member_pipeline(df, known_countries(spark, ["IN", "IN", "IN"]))
    assert out.count() == 1


# ------------------------------------------------------------------ mandatory

@pytest.mark.parametrize("override, expected", [
    ({"member_name": None}, "member_name:MISSING"),
    ({"member_name": "  "}, "member_name:MISSING"),
    ({"member_id": None}, "member_id:MISSING"),
    ({"country": None}, "country_code:MISSING"),
    ({"file_date": None}, "file_date:MISSING"),
    ({"ingestion_timestamp": None}, "ingestion_timestamp:MISSING"),
    ({"source_file_name": None}, "source_file_name:MISSING"),
    ({"enrollment_date": None}, "enrollment_date:MISSING"),     # only MISSING, not also INVALID_DATE
])
def test_mandatory(spark, override, expected):
    assert reasons(one_member(spark, **override)) == {expected}


# ---------------------------------------------------------- length and format

@pytest.mark.parametrize("override, expected", [
    ({"member_name": "A" * 256}, "member_name:TOO_LONG"),
    ({"member_id": "1" * 19}, "member_id:TOO_LONG"),
    ({"agent_name": "A" * 256}, "agent_name:TOO_LONG"),
    ({"state": "ABCDEF"}, "state:TOO_LONG"),
    ({"is_active": "YY"}, "is_active:TOO_LONG"),
])
def test_too_long(spark, override, expected):
    assert reasons(one_member(spark, **override)) == {expected}


def test_exact_max_lengths_pass(spark):
    row = one_member(spark, member_name="A" * 255, member_id="1" * 18, state="ABCDE")
    assert row["reject_reason"] == ""


def test_country_and_tier_too_long_also_fail_their_other_rules(spark):
    assert reasons(one_member(spark, country="INDIA1")) == {"country_code:TOO_LONG", "country_code:NOT_IN_REFERENCE"}
    assert reasons(one_member(spark, tier="GOLDEN")) == {"tier_code:TOO_LONG", "tier_code:INVALID_VALUE"}


@pytest.mark.parametrize("override, expected", [
    ({"member_name": "John3"}, "member_name:INVALID_FORMAT"),
    ({"member_name": "Mary-Jane"}, "member_name:INVALID_FORMAT"),     # letters and spaces only
    ({"member_id": "10A1"}, "member_id:INVALID_FORMAT"),
    ({"state": "K1"}, "state:INVALID_FORMAT"),
    ({"agent_name": "Agent 007"}, "agent_name:INVALID_FORMAT"),
])
def test_invalid_format(spark, override, expected):
    assert reasons(one_member(spark, **override)) == {expected}


def test_null_optional_text_never_fails_format_length_or_tier(spark):
    row = one_member(spark, state=None, agent_name=None, tier=None, is_active=None)
    assert row["reject_reason"] == ""


@pytest.mark.parametrize("tier, ok", [("GLD", True), ("PLT", True), ("SLV", True),
                                      ("gld", False), ("BRZ", False)])
def test_tier_code(spark, tier, ok):
    expected = set() if ok else {"tier_code:INVALID_VALUE"}
    assert reasons(one_member(spark, tier=tier)) == expected


def test_multiple_failures_and_json(spark):
    row = one_member(spark, state="K1", tier="XXX", country="FR", dob="zz")
    assert reasons(row) == {"state:INVALID_FORMAT", "tier_code:INVALID_VALUE",
                            "country_code:NOT_IN_REFERENCE", "dob:INVALID_DATE"}
    assert row["bronze_row_json"] is not None and '"member_name":"John Smith"' in row["bronze_row_json"]


# ------------------------------------------------------- derived columns

def _derived(spark, dob, flight, file_date):
    df = spark.createDataFrame([(dob, flight, file_date)], "dob DATE, last_flight_date DATE, file_date DATE")
    return cm.add_age_and_stale_member(df).collect()[0]


def test_age_uses_file_date_and_floors(spark):
    assert _derived(spark, date(1985, 5, 3), None, date(2024, 5, 2))["age"] == 38
    assert _derived(spark, date(1985, 5, 3), None, date(2024, 5, 3))["age"] == 39
    assert _derived(spark, date(2000, 2, 29), None, date(2024, 2, 28))["age"] == 23


def test_age_is_null_without_dob(spark):
    assert _derived(spark, None, None, date(2024, 5, 2))["age"] is None


def test_stale_member_boundary_is_more_than_90_days(spark):
    assert _derived(spark, None, date(2024, 1, 1), date(2024, 3, 31))["stale_member"] is False   # 90 days
    assert _derived(spark, None, date(2024, 1, 1), date(2024, 4, 1))["stale_member"] is True     # 91 days
    assert _derived(spark, None, date(2024, 4, 1), date(2024, 4, 1))["stale_member"] is False


def test_stale_member_null_without_last_flight(spark):
    assert _derived(spark, None, None, date(2024, 4, 1))["stale_member"] is None


# -------------------------------------------------------- latest per member

def _latest(spark, rows):
    validated = run_member_pipeline(make_bronze_members(spark, rows), known_countries(spark, ["IN", "US"]))
    passed = validated.where("reject_reason = ''").transform(cm.add_age_and_stale_member)
    return cm.keep_latest_record_per_member(passed)


def test_latest_output_is_the_silver_shape(spark):
    out = _latest(spark, [bronze_member()])
    assert set(out.columns) == set(CLEAN_COLS)


def test_latest_by_file_date(spark):
    out = _latest(spark, [
        bronze_member(state="KA", file_date=date(2024, 6, 1)),
        bronze_member(state="MH", file_date=date(2024, 6, 3)),
        bronze_member(state="TN", file_date=date(2024, 6, 2)),
    ]).collect()
    assert len(out) == 1 and out[0]["state"] == "MH"


def test_latest_by_ingestion_timestamp_when_file_date_ties(spark):
    out = _latest(spark, [
        bronze_member(state="KA", ingestion_timestamp=datetime(2024, 6, 1, 9)),
        bronze_member(state="MH", ingestion_timestamp=datetime(2024, 6, 1, 12)),
    ]).collect()
    assert out[0]["state"] == "MH"


def test_latest_by_last_flight_date_and_null_loses(spark):
    out = _latest(spark, [
        bronze_member(state="KA", last_flight_date="01052024"),
        bronze_member(state="MH", last_flight_date="01062024"),
        bronze_member(state="TN", last_flight_date=None),
    ]).collect()
    assert out[0]["state"] == "MH"


def test_latest_full_tie_is_deterministic(spark):
    a, b = bronze_member(state="KA"), bronze_member(state="MH")
    first, second = _latest(spark, [a, b]).collect(), _latest(spark, [b, a]).collect()
    assert len(first) == len(second) == 1 and first[0].asDict() == second[0].asDict()


def test_latest_key_is_member_name(spark):
    out = _latest(spark, [
        bronze_member(member_name="John Smith", member_id="1"),
        bronze_member(member_name="Jane Doe", member_id="1"),     # same member_id, other name -> separate row
        bronze_member(member_name="John Smith", member_id="1"),   # duplicate name -> collapsed
    ]).collect()
    assert sorted(r["member_name"] for r in out) == ["Jane Doe", "John Smith"]
