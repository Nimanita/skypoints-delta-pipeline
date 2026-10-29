"""Clean and validate: text -> semicleaned columns, reject rules, derived columns, latest record per member
(design 5.2 / 5.3). Pure DataFrame functions: no tables are touched here."""
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array, broadcast, col, concat_ws, datediff, expr, floor, length, lit, lpad, max_by,
    months_between, nullif, struct, to_json, trim, try_to_timestamp, when, year, current_date
)

# ---- rules from the design doc (section 5.2) -----------------------------
STALE_DAYS = 90
DATE_FORMAT = "ddMMyyyy"
MEMBER_KEY = "member_name"          # one silver row per this column

MIN_YEAR = 1900  
# adjust to your data
# maximum length of each column
MAX_LEN = {
    "member_name": 255,
    "member_id": 18,
    "tier_code": 5,
    "agent_name": 255,
    "state": 5,
    "country_code": 5,
    "is_active": 1,
}

VALID_TIER_CODES = ["GLD", "PLT", "SLV"]

# column -> pattern the value must match
LETTERS_AND_SPACES = r"^[A-Za-z ]+$"
DIGITS_ONLY = r"^[0-9]+$"
FORMAT_RULES = {
    "member_name": LETTERS_AND_SPACES,
    "member_id": DIGITS_ONLY,
    "state": LETTERS_AND_SPACES,
    "agent_name": LETTERS_AND_SPACES,
}

# reject label -> column that must not be NULL
MANDATORY = {
    "member_name": "member_name",
    "member_id": "member_id",
    "country_code": "country_code",
    "file_date": "file_date",
    "ingestion_timestamp": "ingestion_timestamp",
    "source_file_name": "source_file_name",
    # judged on the text, because the converted date is also NULL when the date is invalid
    "enrollment_date": "enrollment_date_text",
}

# (converted column, its text column, error code)
CONVERSIONS = [
    ("enrollment_date", "enrollment_date_text", "INVALID_DATE"),
    ("last_flight_date", "last_flight_date_text", "INVALID_DATE"),
    ("dob", "dob_text", "INVALID_DATE"),
    ("post_code", "post_code_text", "INVALID_INT"),
]

# columns of the raw bronze row, kept as JSON in the reject table
BRONZE_COLS = [
    "member_id", "member_name", "country", "dob", "enrollment_date", "last_flight_date",
    "tier", "is_active", "agent_name", "state", "post_code",
    "_rescued_data", "source_file_name", "file_date", "ingestion_timestamp",
]

# silver column name -> bronze column name (these columns are only trimmed, blank -> NULL)
TEXT_COLUMNS = {
    "member_name": "member_name",
    "member_id": "member_id",
    "enrollment_date_text": "enrollment_date",
    "last_flight_date_text": "last_flight_date",
    "tier_code": "tier",
    "agent_name": "agent_name",
    "state": "state",
    "country_code": "country",
    "post_code_text": "post_code",
    "dob_text": "dob",
    "is_active": "is_active",
}

# columns copied from bronze without any change
PASS_THROUGH = ["file_date", "ingestion_timestamp", "source_file_name"]

# working columns that must not reach the silver output
HELPER_COLUMNS = [
    "enrollment_date_text", "last_flight_date_text", "dob_text", "post_code_text",
    "bronze_row_json", "reject_reason",
]


# ---- clean the text and convert to types ---------------------------------------

def trim_and_blank_to_null(column_name: str) -> Column:
    """Trim spaces. A value that is empty after trimming becomes NULL (design 5.2)."""
    return nullif(trim(col(column_name)), lit(""))


def trim_and_rename_bronze_columns(bronze_df: DataFrame) -> DataFrame:
    """Bronze row (all STRING) -> trimmed text, blanks as NULL, silver column names.
    Dates and post code stay as text (*_text columns) until convert_text_to_types().
    The untouched bronze row is kept as a struct in `bronze_row`. It is turned into JSON
    only for rejected rows (see validate_members)."""
    columns = []

    # trimmed columns, renamed to the silver names
    for silver_name, bronze_name in TEXT_COLUMNS.items():
        columns.append(trim_and_blank_to_null(bronze_name).alias(silver_name))

    # columns copied as they are
    for column_name in PASS_THROUGH:
        columns.append(col(column_name))

    # the raw bronze row
    columns.append(struct(BRONZE_COLS).alias("bronze_row"))

    return bronze_df.select(columns)



def valid_year(parsed: Column) -> Column:
    """Keep the parsed date only if its year is in a same range."""
    return when(
        year(parsed).between(MIN_YEAR, year(current_date())),
        parsed,
    )

def parse_strict_date(text_column: Column) -> Column:
    """Parse dates in ddMMyyyy, yyyyMMdd, or MMddyyyy format."""

    is_8_digits = text_column.rlike(r"^\d{8}$")

    parsed_ddMMyyyy = valid_year(try_to_timestamp(text_column, lit("ddMMyyyy")).cast("date"))
    parsed_yyyyMMdd = valid_year(try_to_timestamp(text_column, lit("yyyyMMdd")).cast("date"))
    parsed_MMddyyyy = valid_year(try_to_timestamp(text_column, lit("MMddyyyy")).cast("date"))

    return (
        when(
            is_8_digits,
            when(parsed_ddMMyyyy.isNotNull(), parsed_ddMMyyyy)
            .when(parsed_yyyyMMdd.isNotNull(), parsed_yyyyMMdd)
            .when(parsed_MMddyyyy.isNotNull(), parsed_MMddyyyy)
        )
    )
def convert_text_to_types(trimmed_df: DataFrame, known_country_codes_df: DataFrame) -> DataFrame:
    """Text -> DATE / INT, and mark known country codes (adds `is_known_country`).
    A value that cannot be converted becomes NULL; validate_members() rejects the row
    when the original text was not blank."""

    # dob arrives as DDMMYYYY but loses its leading zero (3051985 instead of 03051985).
    # Pad only 1-8 digit values, because lpad would cut a longer one.
    dob_padded = (
        when(col("dob_text").rlike(r"^\d{1,8}$"), lpad(col("dob_text"), 8, "0"))
        .otherwise(col("dob_text"))
    )

    # try_cast: a very long post code (for example 12 digits) becomes NULL and is rejected
    # as INVALID_INT, instead of failing the whole job.
    post_code = when(
        col("post_code_text").rlike(r"^\d+$"),
        expr("try_cast(post_code_text AS INT)"),
    )

    # exact, case-sensitive match against the fixed country list.
    # distinct() so a repeated code in the reference table cannot duplicate member rows.
    known_countries = (
        known_country_codes_df
        .select("country_code")
        .distinct()
        .withColumn("is_known_country", lit(True))
    )

    return (
        trimmed_df
        .withColumn("enrollment_date", parse_strict_date(col("enrollment_date_text")))
        .withColumn("last_flight_date", parse_strict_date(col("last_flight_date_text")))
        .withColumn("dob", parse_strict_date(dob_padded))
        .withColumn("post_code", post_code)
        .join(broadcast(known_countries), "country_code", "left")
    )


# ---- validate: reject rules ----------------------------------------------------

def validate_members(semicleaned_df: DataFrame) -> DataFrame:
    """All row-level validation rules in one place .

    Each rule gives a reject code "column:CHECK" when it fails, otherwise NULL.
    The failed codes are joined into `reject_reason`, for example
    'state:TOO_LONG; dob:INVALID_DATE'. A valid row gets the empty string.
    `bronze_row_json` is filled for rejected rows only.

    Rules: mandatory value, maximum length, valid date / integer, known country,
    format, allowed tier code.
    A NULL value never fails the length, format or tier rule, because a comparison
    with NULL is not true, so `when` skips it."""
    rules = []

    # 1. mandatory columns must have a value
    for label, column_name in MANDATORY.items():
        rules.append(when(col(column_name).isNull(), lit(label + ":MISSING")))

    # 2. a value longer than its maximum length rejects the row
    for column_name, max_length in MAX_LEN.items():
        rules.append(when(length(col(column_name)) > max_length, lit(column_name + ":TOO_LONG")))

    # 3. text was filled in but did not convert to a DATE / INT
    for converted_column, text_column, error_code in CONVERSIONS:
        rules.append(when(
            col(text_column).isNotNull() & col(converted_column).isNull(),
            lit(converted_column + ":" + error_code),
        ))

    # 4. country code must exist in the reference table
    rules.append(when(
        col("country_code").isNotNull() & col("is_known_country").isNull(),
        lit("country_code:NOT_IN_REFERENCE"),
    ))

    # 5. format check (letters only, digits only)
    for column_name, pattern in FORMAT_RULES.items():
        rules.append(when(~col(column_name).rlike(pattern), lit(column_name + ":INVALID_FORMAT")))

    # 6. tier code: allowed values only
    rules.append(when(~col("tier_code").isin(VALID_TIER_CODES), lit("tier_code:INVALID_VALUE")))

    # join the failed codes; concat_ws skips the NULLs, so a valid row gives ""
    reject_reason = concat_ws("; ", array(rules))

    # JSON of the raw bronze row, only for rejected rows
    row_json = to_json(col("bronze_row"), {"ignoreNullFields": "false"})

    return (
        semicleaned_df
        .withColumn("reject_reason", reject_reason)
        .withColumn("bronze_row_json", when(col("reject_reason") != "", row_json))
        .drop("is_known_country", "bronze_row")
    )


# ---- derived columns and the latest record per member --------------------------

def add_age_and_stale_member(valid_df: DataFrame) -> DataFrame:
    """Derived columns. They use file_date (the feed date), never current_date(),
    so a rerun gives the same result."""
    return (
        valid_df
        .withColumn("age", floor(months_between(col("file_date"), col("dob")) / 12).cast("int"))
        .withColumn("stale_member", datediff(col("file_date"), col("last_flight_date")) > STALE_DAYS)
    )


def keep_latest_record_per_member(valid_df: DataFrame) -> DataFrame:
    """One row per MEMBER_KEY: the newest by file_date, then ingestion_timestamp, then the
    later last_flight_date (NULL loses). If all three tie, the other columns decide,
    so the result is the same on every run.
    Helper columns are dropped here, so the output is the silver shape."""

    # columns that go to silver
    keep_columns = []
    for column_name in valid_df.columns:
        if column_name not in HELPER_COLUMNS:
            keep_columns.append(column_name)

    # sort order, most important first
    priority_columns = ["file_date", "ingestion_timestamp", "last_flight_date"]
    other_columns = []
    for column_name in keep_columns:
        if column_name not in priority_columns:
            other_columns.append(column_name)

    # Spark compares a struct field by field, left to right:
    # it looks at the next field only when the previous one is equal.
    sort_order = struct(priority_columns + other_columns)

    # for each member, take the row with the biggest sort_order
    latest_df = (
        valid_df
        .groupBy(MEMBER_KEY)
        .agg(max_by(struct(keep_columns), sort_order).alias("latest"))
    )

    # open the "latest" struct back into normal columns
    final_columns = []
    for column_name in keep_columns:
        final_columns.append(col("latest." + column_name).alias(column_name))

    return latest_df.select(final_columns)
