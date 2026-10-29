"""What runs for every redemption micro-batch: flatten, clean, validate, reject, upsert.
Also the reject table writer."""
import uuid
from functools import reduce

from pyspark.sql.functions import col, current_timestamp

from pipeline_utils.silver.clean_and_validate_redemptions import (
    convert_text_to_types, flatten_redemptions, keep_latest_record_per_transaction,
    mark_orphan_members, trim_and_rename_bronze_columns, validate_redemptions,
)
from pipeline_utils.silver.country_tables import get_country_target_tables

# (the tables are created by %sql cells in the notebook: rename there too)
REDEMPTIONS_TABLE = "skypoints.silver.silver_redemptions"
REJECTS_TABLE = "skypoints.silver.redemption_rejects"
REJECTS_APP_ID = "silver_redemption_rejects"

# column order of the silver_redemptions table
REDEMPTION_COLS = [
    "member_id", "txn_id", "txn_date", "partner", "miles_redeemed", "status", "feed_date",
    "is_member_id_orphan", "file_date", "ingestion_timestamp", "source_file_name",
]

# file_date >= t.file_date stops an old or late file from overwriting newer data.
UPSERT_REDEMPTIONS_SQL = """
MERGE INTO {target} AS t
USING {source} AS s
ON t.member_id = s.member_id AND t.txn_id = s.txn_id
WHEN MATCHED AND s.file_date >= t.file_date THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def read_known_member_ids(session):
    """member_id of every member already in Silver. Members live in one table per country,
    so the country tables are read and combined (only the member_id column)."""
    tables = sorted(set(get_country_target_tables(session).values()))
    frames = [
        session.table(table_name).select("member_id")
        for table_name in tables
        if session.catalog.tableExists(table_name)
    ]
    if not frames:
        return session.createDataFrame([], "member_id STRING")
    return reduce(lambda left, right: left.unionByName(right), frames)


def build_reject_table_rows(rejected_df):
    """Shape the failed rows like silver.redemption_rejects.
    `_rescued_data` holds the full raw flattened row as JSON."""
    return rejected_df.select(
        "member_id", "reject_reason",
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


def upsert_redemptions(session, latest_df):
    """MERGE the batch into silver_redemptions on member_id + txn_id.
    The source has one row per key (keep_latest_record_per_transaction), so the MERGE cannot
    match two source rows to one target row. Rerunning gives the same table."""
    view_name = "_redemptions_src_" + uuid.uuid4().hex[:10]
    latest_df.select(*REDEMPTION_COLS).createOrReplaceTempView(view_name)
    try:
        session.sql(UPSERT_REDEMPTIONS_SQL.format(target=REDEMPTIONS_TABLE, source=view_name))
    finally:
        session.catalog.dropTempView(view_name)


def load_redemption_batch_to_silver(bronze_batch_df, batch_id):
    """Called once per micro-batch by foreachBatch.
      1. flatten the redemptions array, clean the text, convert to types, validate
         -> failed rows go to redemption_rejects
      2. keep the latest record per member_id + txn_id, mark orphan members
      3. write the rejects, then upsert the good rows into silver_redemptions
    Every write is safe to rerun."""
    session = bronze_batch_df.sparkSession

    # 1. flatten, clean, convert, validate
    validated_df = (
        bronze_batch_df
        .transform(flatten_redemptions)
        .transform(trim_and_rename_bronze_columns)
        .transform(convert_text_to_types)
        .transform(validate_redemptions)
        .persist()
    )
    latest_df = None
    try:
        rows_in = validated_df.count()                   # transactions, also fills the cache
        failed_df = validated_df.where(col("reject_reason") != "")
        passed_df = validated_df.where(col("reject_reason") == "")

        # 2. latest record per transaction, orphan flag (persisted: used by the merge and the counts)
        latest_df = (
            passed_df
            .transform(keep_latest_record_per_transaction)
            .transform(mark_orphan_members, read_known_member_ids(session))
            .persist()
        )

        # 3. rejects first, then the upsert
        append_to_rejects_table(build_reject_table_rows(failed_df), batch_id)
        upsert_redemptions(session, latest_df)

        rows_rejected = failed_df.count()
        rows_latest   = latest_df.count()
        rows_orphan   = latest_df.where(col("is_member_id_orphan")).count()
        print(
            f"batch {batch_id}: transactions in={rows_in} | rejected={rows_rejected} | "
            f"superseded duplicates={rows_in - rows_rejected - rows_latest} | "
            f"upserted={rows_latest} | orphan member_id={rows_orphan}"
        )

    finally:
        if latest_df is not None:
            latest_df.unpersist()
        validated_df.unpersist()
