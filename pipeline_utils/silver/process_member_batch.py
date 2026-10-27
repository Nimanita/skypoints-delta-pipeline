"""What runs for every micro-batch: clean, validate, reject, split. Also the reject table writer."""
from pyspark.sql.functions import col, current_timestamp

from pipeline_utils.silver.clean_and_validate import (
    add_age_and_stale_member, convert_text_to_types, keep_latest_record_per_member,
    trim_and_rename_bronze_columns, validate_members,
)
from pipeline_utils.silver.country_tables import read_known_country_codes

# (the table is created by a %sql cell in the notebook: rename there too)
REJECTS_TABLE = "skypoints.silver.member_rejects"
REJECTS_APP_ID = "silver_member_rejects" 

def build_reject_table_rows(rejected_df):
    """Shape the failed rows like silver.member_rejects (design 5.5).
    `_rescued_data` holds the full raw bronze row as JSON."""
    return rejected_df.select(
        "member_name", "member_id", "reject_reason",
        col("bronze_row_json").alias("_rescued_data"),
        "source_file_name", "file_date", "ingestion_timestamp",
        current_timestamp().alias("rejected_at"),
    )


def append_to_rejects_table(reject_rows_df, batch_id):
    """Append the rejects. txnAppId + txnVersion make Delta skip the write if this batch
    was already written, so a rerun does not create duplicate rejects."""
    (reject_rows_df.write.format("delta").mode("append")
        .option("txnAppId", REJECTS_APP_ID)
        .option("txnVersion", batch_id)
        .saveAsTable(REJECTS_TABLE))


def load_member_batch_to_silver(bronze_batch_df, batch_id):
    """Called once per micro-batch by foreachBatch.
      1. clean the text, convert to types, validate  -> failed rows go to member_rejects
      2. add age / stale_member, keep the latest record per member
      3. write the rejects, then split the good rows into the country tables (design 6):
         upsert into the member's country table, delete movers from the old one,
         update member_country_lookup last
    Every write is safe to rerun (design 6.4)."""
    session = bronze_batch_df.sparkSession

    # 1. clean, convert, validate
    validated_df = (
        bronze_batch_df
        .transform(trim_and_rename_bronze_columns)
        .transform(convert_text_to_types, read_known_country_codes(session))
        .transform(validate_members)
        .persist()
    )
    try:
        rows_in = validated_df.count()                   # also fills the cache
        failed_df = validated_df.where(col("reject_reason") != "")
        passed_df = validated_df.where(col("reject_reason") == "")

        # 2. derived columns, latest record per member
        latest_df = (
            passed_df
            .transform(add_age_and_stale_member)
            .transform(keep_latest_record_per_member)
        )

        # 3. rejects first, then the country split
        append_to_rejects_table(build_reject_table_rows(failed_df), batch_id)
        latest_df.write.format("delta").mode("overwrite").saveAsTable("skypoints.silver.member_test")

        #split_stats = split_into_country_tables(session, latest_df)

        rows_rejected = failed_df.count()
        rows_latest   = latest_df.count()
        print(
            f"batch {batch_id}: in={rows_in} | rejected={rows_rejected} | "
            f"superseded duplicates={rows_in - rows_rejected - rows_latest} | "
            #f"older than lookup, skipped={rows_latest - split_stats['rows_applied']} | "
            #f"applied={split_stats['rows_applied']} | moved to another country={split_stats['movers']} | "
            #f"country tables written={split_stats['country_tables']}"
        )

    finally:
        validated_df.unpersist()
