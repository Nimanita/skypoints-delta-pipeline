"""Clean and validate redemptions: Bronze text -> typed Silver records.
Explode the redemptions array, validate each transaction, keep the latest record per
(member_id, txn_id), mark orphan members (member_id not in Silver members yet).
Pure DataFrame functions: no tables are touched here."""
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array, col, concat_ws, current_date, explode, expr, length, lit, max_by,
    nullif, struct, to_json, trim, try_to_timestamp, when, year,
)

# ---- rules ---------------------------------------------------------------------
TXN_KEY = ["member_id", "txn_id"]   # one silver row per this pair (same as the MERGE ON clause)

MIN_YEAR = 1900

# maximum length of each column
MAX_LEN = {
    "member_id": 18,
    "txn_id": 100,
    "partner": 255,
    "status": 50,
}

# partner has no fixed list yet. Add a VALID_PARTNERS list the same way when it is confirmed.
VALID_STATUS = ["PENDING", "COMPLETED", "REJECTED"]

# column -> pattern the value must match
DIGITS_ONLY = r"^[0-9]+$"
SIGNED_DIGITS = r"^-?[0-9]+$"            # miles: a minus sign is allowed through, so NEGATIVE_VALUE can fire
TXN_ID_PATTERN = r"^[A-Za-z0-9_-]+$"     # change to DIGITS_ONLY if txn_id is numeric only
FORMAT_RULES = {
    "member_id": DIGITS_ONLY,
    "txn_id": TXN_ID_PATTERN,
}

# reject label -> column that must not be NULL
MANDATORY = {
    "member_id": "member_id",
    "txn_id": "txn_id",
    "partner": "partner",
    "status": "status",
    "file_date": "file_date",
    "ingestion_timestamp": "ingestion_timestamp",
    "source_file_name": "source_file_name",
    # judged on the text, because the converted value is also NULL when the text is invalid
    "txn_date": "txn_date_text",
    "feed_date": "feed_date_text",
    "miles_redeemed": "miles_redeemed_text",
}

# (converted column, its text column, error code)
CONVERSIONS = [
    ("txn_date", "txn_date_text", "INVALID_DATE"),
    ("feed_date", "feed_date_text", "INVALID_DATE"),
    ("miles_redeemed", "miles_redeemed_text", "INVALID_INT"),
]

# columns of the flattened bronze record (one row per transaction), kept as JSON in the reject table
BRONZE_COLS = [
    "member_id", "feed_date", "txn_id", "txn_date", "partner", "miles_redeemed", "status",
    "_rescued_data", "source_file_name", "file_date", "ingestion_timestamp",
]

# silver column name -> flattened bronze column name (these columns are only trimmed, blank -> NULL)
TEXT_COLUMNS = {
    "member_id": "member_id",
    "txn_id": "txn_id",
    "txn_date_text": "txn_date",
    "partner": "partner",
    "miles_redeemed_text": "miles_redeemed",
    "status": "status",
    "feed_date_text": "feed_date",
}

# columns copied from bronze without any change
PASS_THROUGH = ["file_date", "ingestion_timestamp", "source_file_name"]

# working columns that must not reach the silver output
HELPER_COLUMNS = [
    "txn_date_text", "feed_date_text", "miles_redeemed_text",
    "bronze_row_json", "reject_reason",
]


# ---- flatten the bronze array, clean the text ----------------------------------

def flatten_redemptions(bronze_df: DataFrame) -> DataFrame:
    """One bronze row per member/feed -> one row per transaction (explode the redemptions array).
    Everything is cast to STRING here, so the rest of the pipeline sees text only.
    A member row with an empty or NULL array gives no transaction rows."""
    return (
        bronze_df
        .withColumn("redemption", explode(col("redemptions")))
        .select(
            col("member_id").cast("string").alias("member_id"),
            col("feed_date").cast("string").alias("feed_date"),
            col("redemption.txn_id").cast("string").alias("txn_id"),
            col("redemption.txn_date").cast("string").alias("txn_date"),
            col("redemption.partner").cast("string").alias("partner"),
            col("redemption.miles_redeemed").cast("string").alias("miles_redeemed"),
            col("redemption.status").cast("string").alias("status"),
            col("_rescued_data"),
            col("source_file_name"),
            col("file_date"),
            col("ingestion_timestamp"),
        )
    )


def trim_and_blank_to_null(column_name: str) -> Column:
    """Trim spaces. A value that is empty after trimming becomes NULL."""
    return nullif(trim(col(column_name)), lit(""))


def trim_and_rename_bronze_columns(flattened_df: DataFrame) -> DataFrame:
    """Flattened row (all STRING) -> trimmed text, blanks as NULL, silver column names.
    Dates and miles stay as text (*_text columns) until convert_text_to_types().
    The untouched flattened row is kept as a struct in `bronze_row`. It is turned into JSON
    only for rejected rows (see validate_redemptions)."""
    columns = []

    # trimmed columns, renamed to the silver names
    for silver_name, bronze_name in TEXT_COLUMNS.items():
        columns.append(trim_and_blank_to_null(bronze_name).alias(silver_name))

    # columns copied as they are
    for column_name in PASS_THROUGH:
        columns.append(col(column_name))

    # the raw flattened row
    columns.append(struct(BRONZE_COLS).alias("bronze_row"))

    return flattened_df.select(columns)


# ---- convert text to proper types ----------------------------------------------

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


def convert_text_to_types(trimmed_df: DataFrame) -> DataFrame:
    """Text -> DATE / INT.
    A value that cannot be converted becomes NULL; validate_redemptions() rejects the row
    when the original text was not blank."""

    # try_cast: a very long number becomes NULL and is rejected as INVALID_INT,
    # instead of failing the whole job.
    miles_redeemed = when(
        col("miles_redeemed_text").rlike(SIGNED_DIGITS),
        expr("try_cast(miles_redeemed_text AS INT)"),
    )

    return (
        trimmed_df
        .withColumn("txn_date", parse_strict_date(col("txn_date_text")))
        .withColumn("feed_date", parse_strict_date(col("feed_date_text")))
        .withColumn("miles_redeemed", miles_redeemed)
    )


# ---- validate: reject rules ----------------------------------------------------

def validate_redemptions(semicleaned_df: DataFrame) -> DataFrame:
    """All row-level validation rules in one place.

    Each rule gives a reject code "column:CHECK" when it fails, otherwise NULL.
    The failed codes are joined into `reject_reason`, for example
    'txn_date:INVALID_DATE; miles_redeemed:NEGATIVE_VALUE'. A valid row gets the empty string.
    `bronze_row_json` is filled for rejected rows only.

    Rules: mandatory value, maximum length, valid date / integer, non-negative miles,
    format, allowed status.
    A NULL value never fails the length, format or status rule, because a comparison
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

    # 4. miles redeemed cannot be negative
    rules.append(when(col("miles_redeemed") < 0, lit("miles_redeemed:NEGATIVE_VALUE")))

    # 5. format check (member_id, txn_id)
    for column_name, pattern in FORMAT_RULES.items():
        rules.append(when(~col(column_name).rlike(pattern), lit(column_name + ":INVALID_FORMAT")))

    # 6. status: allowed values only
    rules.append(when(~col("status").isin(VALID_STATUS), lit("status:INVALID_VALUE")))

    # join the failed codes; concat_ws skips the NULLs, so a valid row gives ""
    reject_reason = concat_ws("; ", array(rules))

    # JSON of the raw flattened row, only for rejected rows
    row_json = to_json(col("bronze_row"), {"ignoreNullFields": "false"})

    return (
        semicleaned_df
        .withColumn("reject_reason", reject_reason)
        .withColumn("bronze_row_json", when(col("reject_reason") != "", row_json))
        .drop("bronze_row")
    )


# ---- latest record per transaction ---------------------------------------------

def keep_latest_record_per_transaction(valid_df: DataFrame) -> DataFrame:
    """One row per TXN_KEY (member_id + txn_id): the newest by file_date, then ingestion_timestamp.
    If both tie, the other columns decide, so the result is the same on every run.
    Helper columns are dropped here. Without this the MERGE would fail on a source with
    two rows for the same key."""

    # columns that go to silver
    keep_columns = []
    for column_name in valid_df.columns:
        if column_name not in HELPER_COLUMNS:
            keep_columns.append(column_name)

    # sort order, most important first
    priority_columns = ["file_date", "ingestion_timestamp"]
    other_columns = []
    for column_name in keep_columns:
        if column_name not in priority_columns:
            other_columns.append(column_name)

    # Spark compares a struct field by field, left to right:
    # it looks at the next field only when the previous one is equal.
    sort_order = struct(priority_columns + other_columns)

    # for each key, take the row with the biggest sort_order
    latest_df = (
        valid_df
        .groupBy(TXN_KEY)
        .agg(max_by(struct(keep_columns), sort_order).alias("latest"))
    )

    # open the "latest" struct back into normal columns
    final_columns = []
    for column_name in keep_columns:
        final_columns.append(col("latest." + column_name).alias(column_name))

    return latest_df.select(final_columns)


# ---- orphan members ------------------------------------------------------------

def mark_orphan_members(latest_df: DataFrame, known_member_ids_df: DataFrame) -> DataFrame:
    """Adds `is_member_id_orphan`: True when the member_id is not in Silver members (yet).
    The transaction is kept either way. `known_member_ids_df` needs a `member_id` column.
    Runs after keep_latest_record_per_transaction, so it joins fewer rows."""
    known_members = (
        known_member_ids_df
        .select("member_id")
        .where(col("member_id").isNotNull())
        .distinct()
        .withColumn("_member_exists", lit(True))
    )

    return (
        latest_df
        .join(known_members, "member_id", "left")
        .withColumn("is_member_id_orphan", col("_member_exists").isNull())
        .drop("_member_exists")
    )

