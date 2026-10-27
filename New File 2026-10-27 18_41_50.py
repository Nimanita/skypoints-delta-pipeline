from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col, lit, coalesce, try_to_timestamp, when


def parse_strict_date(text_column: Column) -> Column:
    """
    Parse an 8-digit date using these formats, in priority order:
    1. ddMMyyyy
    2. yyyyMMdd
    3. MMddyyyy

    Returns NULL if the input is not 8 digits or none of the formats works.
    """
    is_8_digits = text_column.rlike(r"^[0-9]{8}$")

    parsed_date = coalesce(
        try_to_timestamp(text_column, lit("ddMMyyyy")),
        try_to_timestamp(text_column, lit("yyyyMMdd")),
        try_to_timestamp(text_column, lit("MMddyyyy")),
    ).cast("date")

    return when(is_8_digits, parsed_date)


if __name__ == "__main__":
    # In Databricks, run this file from a notebook with:
    # %run /path/to/date_parser_test
    spark = SparkSession.builder.getOrCreate()

    test_df = spark.createDataFrame(
        [
            ("27102026",),  # ddMMyyyy -> 2026-10-27
            ("20261027",),  # yyyyMMdd -> 2026-10-27
            ("10272026",),  # MMddyyyy -> 2026-10-27
            ("31132026",),  # invalid date -> NULL
            ("1234567",),   # not 8 digits -> NULL
            ("abcdefgh",), # not numeric -> NULL
            (None,),        # NULL input -> NULL
        ],
        ["date_text"],
    )

    result_df = test_df.select(
        "date_text",
        parse_strict_date(col("date_text")).alias("parsed_date"),
    )
    result_df.show(truncate=False)