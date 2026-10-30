"""Country list, the split into one table per country, one-time backfill, reconciliation .
The MERGE statements are plain SQL strings next to the code that runs them."""
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from pyspark import StorageLevel
from pyspark.sql.functions import col, lit

# ---- tables --------------------------------------------------------------------
# (the two fixed tables are created by the %sql cells in the notebook: rename there too)
COUNTRY_TABLE = "skypoints.silver.country"      # fixed list, loaded by setup_country_lookup_table
COUNTRY_CODE  = "country_code"                         # column in the reference table that holds the code
COUNTRY_TARGET_TABLE = "target_table"                        # optional column in the reference table: table name per country
MEMBER_LOOKUP_TABLE  = "skypoints.silver.member_country_lookup"   # where does each member live right now
COUNTRY_TABLE_SCHEMA = "skypoints.silver"                  # country tables are created in this schema
LEGACY_MASTER_TABLE  = "skypoints.silver.member"           # old single master table, only read by the one-time backfill

# column order of every country table
CLEAN_COLS = [
    "member_name", "member_id", "enrollment_date", "last_flight_date", "tier_code", "agent_name",
    "state", "country_code", "post_code", "dob", "is_active", "age", "stale_member",
    "file_date", "ingestion_timestamp", "source_file_name",
]

# ---- how many country merges run at the same time ---------------------------------
# One worker per country that has work in this batch, up to MAX_PARALLEL_MERGES.
# Each MERGE is a Spark job with a fixed cost (plan, scan, commit), so many small countries
# gain the most from running together. Spark queues jobs the cluster cannot start yet, so the
# limit only protects the driver and avoids merges fighting for the same cores.
# 8 is a starting point, not a measured value: tune it from the merge_seconds in the batch log.
MAX_PARALLEL_MERGES = 8

# same schema in every country table
COUNTRY_TABLE_COLUMNS_DDL = """
    member_name         STRING,
    member_id           STRING,
    enrollment_date     DATE,
    last_flight_date    DATE,
    tier_code           STRING,
    agent_name          STRING,
    state               STRING,
    country_code        STRING,
    post_code           INT,
    dob                 DATE,
    is_active           STRING,
    age                 INT,
    stale_member        BOOLEAN,
    file_date           DATE,
    ingestion_timestamp TIMESTAMP,
    source_file_name    STRING
"""

# ---- the MERGE statements -------------------------------------------------------
# The country comparison is NOT in the ON clause (that would insert unchanged members twice).
# file_date >= t.file_date stops an old or late file from overwriting newer data.

# new country table: update if the member is already there, insert if not
UPSERT_COUNTRY_SQL = """
MERGE INTO {target} AS t
USING {source} AS s
ON t.member_name = s.member_name
WHEN MATCHED AND s.file_date >= t.file_date THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

# old country table: remove the member who moved away (never delete a newer row)
DELETE_MOVER_SQL = """
MERGE INTO {target} AS t
USING {source} AS s
ON t.member_name = s.member_name
WHEN MATCHED AND t.file_date <= s.file_date THEN DELETE
"""

# lookup: current country per member
UPDATE_LOOKUP_SQL = """
MERGE INTO {target} AS t
USING {source} AS s
ON t.member_name = s.member_name
WHEN MATCHED AND s.file_date >= t.file_date THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

# ---- the fixed country list ----------------------------------------------------

def read_known_country_codes(session):
    return session.table(COUNTRY_TABLE).select(
        col(COUNTRY_CODE),
        lit(True).alias("is_known_country")
    )


# ---- country code -> country table ---------------------------------------------

def _safe_table_name(name: str) -> str:
    """Table names are put into SQL text, so allow only catalog.schema.table characters."""
    if not re.fullmatch(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){0,2}", name):
        raise ValueError(f"unsafe table name in the country reference table: {name!r}")
    # a 2-part name like 'silver.member_india' gets the catalog added
    if name.count(".") == 1:
        name = COUNTRY_TABLE.split(".")[0] + "." + name
    return name


def get_country_target_tables(session) -> dict:
    """{country_code: table name}. Uses the reference table's `target_table` column if it has one,
    otherwise builds '<COUNTRY_TABLE_SCHEMA>.member_<code>'. The list is small, so it is collected."""
    reference_df = session.table(COUNTRY_TABLE)
    has_target_column = COUNTRY_TARGET_TABLE in reference_df.columns
    columns = [COUNTRY_CODE] + ([COUNTRY_TARGET_TABLE] if has_target_column else [])

    mapping = {}
    for row in reference_df.select(*columns).distinct().collect():
        code = row[COUNTRY_CODE]
        if code is None:
            continue
        table_name = row[COUNTRY_TARGET_TABLE] if has_target_column else None
        if not table_name:
            table_name = f"{COUNTRY_TABLE_SCHEMA}.member_{re.sub(r'[^a-z0-9]+', '_', code.lower())}"
        mapping[code] = _safe_table_name(table_name)
    return mapping


def create_country_table_if_missing(session, table_name: str):
    session.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({COUNTRY_TABLE_COLUMNS_DDL}) "
        f"USING DELTA CLUSTER BY (member_name)"
    )


# ---- the split -----------------------------------------------------------------

def _merge_with_temp_view(session, source_df, sql_template: str, target_table: str):
    """Expose the source DataFrame as a uniquely named temp view, run the MERGE, drop the view.
    Unique name, so several countries can run in parallel threads."""
    view_name = "_split_src_" + uuid.uuid4().hex[:10]
    source_df.createOrReplaceTempView(view_name)
    try:
        session.sql(sql_template.format(target=target_table, source=view_name))
    finally:
        session.catalog.dropTempView(view_name)


def choose_worker_count(rows_per_country: dict, max_workers=None) -> int:
    """One worker per country with work, never more than the limit (`max_workers` if the
    caller sets it, otherwise MAX_PARALLEL_MERGES). One country: no thread pool needed."""
    countries = len(rows_per_country)
    if countries <= 1:
        return 1
    limit = max_workers if max_workers else MAX_PARALLEL_MERGES
    return max(1, min(countries, limit))


def _run_parallel(tasks_by_country: dict, rows_per_country: dict, max_workers=None):
    """tasks_by_country = {country_code: callable}. Runs them in a thread pool sized by the load.
    The biggest countries are submitted first, so one big country starts at once and does not
    wait behind many small ones (country sizes are very uneven).
    The first error is raised, so the batch fails and is retried."""
    order = sorted(tasks_by_country, key=lambda code: rows_per_country.get(code, 0), reverse=True)
    workers = choose_worker_count(rows_per_country, max_workers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(tasks_by_country[code]) for code in order]
        for future in futures:
            future.result()


def update_member_country_lookup(session, latest_df):
    """Step 4 of the split: record the current country of every member in this batch.
    Runs LAST. The source is the whole batch, not only the applicable rows:
    `s.file_date >= t.file_date` in the MERGE already ignores a row older than the stored one.
    A new member is inserted, a mover or a same-country member is updated."""
    _merge_with_temp_view(
        session,
        latest_df.select("member_name", "member_id","country_code", "file_date"),
        UPDATE_LOOKUP_SQL,
        MEMBER_LOOKUP_TABLE,
    )


def split_into_country_tables(session, latest_df, max_workers=None) -> dict:
    """latest_df = one row per member for this batch (output of keep_latest_record_per_member).

    1. compare with member_country_lookup (before it is touched): find rows that are not older
       than what the lookup already holds, and find the movers
    2. upsert every applicable row into its (new) country table
    3. delete the movers from their old country table
    4. update member_country_lookup LAST (update_member_country_lookup)
    Every step is safe to rerun: until step 4 the lookup is unchanged, so a retry finds the same
    movers and repeats the same merges.
    `max_workers` overrides MAX_PARALLEL_MERGES (see choose_worker_count).
    Returns {'rows_applied', 'movers', 'country_tables', 'workers', 'merge_seconds'}."""
    country_tables = get_country_target_tables(session)

    lookup_df = session.table(MEMBER_LOOKUP_TABLE).select(
        "member_name",
        "member_id",
        col("country_code").alias("old_country_code"),
        col("file_date").alias("old_file_date"),
    )

    # 1. rows older than the lookup are skipped. A member not in the lookup is new.
    #    Persisted and counted BEFORE the lookup changes (the lookup is part of this plan).
    applicable_df = (
        latest_df
        .join(lookup_df, ["member_name" , "member_id"], "left")
        .where(col("old_file_date").isNull() | (col("file_date") >= col("old_file_date")))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        # rows per new country: gives rows_applied, the country list and the load, in one pass
        new_rows = {r["country_code"]: r["count"]
                    for r in applicable_df.groupBy("country_code").count().collect()}
        rows_applied = sum(new_rows.values())
        if rows_applied == 0:
            return {"rows_applied": 0, "movers": 0, "country_tables": 0, "workers": 0, "merge_seconds": 0.0}

        movers_df = applicable_df.where(
            col("old_country_code").isNotNull() & (col("old_country_code") != col("country_code"))
        )
        old_rows = {r["old_country_code"]: r["count"]
                    for r in movers_df.groupBy("old_country_code").count().collect()}
        movers = sum(old_rows.values())

        unknown = sorted((set(new_rows) | set(old_rows)) - set(country_tables))
        if unknown:
            raise ValueError(f"country codes without a country table mapping: {unknown}")

        for code in set(new_rows) | set(old_rows):
            create_country_table_if_missing(session, country_tables[code])

        merge_started = time.perf_counter()

        # 2. upsert into the new country tables
        def upsert(code):
            rows = applicable_df.where(col("country_code") == code).select(*CLEAN_COLS)
            _merge_with_temp_view(session, rows, UPSERT_COUNTRY_SQL, country_tables[code])

        _run_parallel({c: (lambda c=c: upsert(c)) for c in new_rows}, new_rows, max_workers)

        # 3. movers leave their old country table
        def delete_movers(code):
            rows = (movers_df.where(col("old_country_code") == code)
                    .select("member_name", "file_date"))
            _merge_with_temp_view(session, rows, DELETE_MOVER_SQL, country_tables[code])

        if old_rows:
            _run_parallel({c: (lambda c=c: delete_movers(c)) for c in old_rows}, old_rows, max_workers)

        # 4. lookup last
        update_member_country_lookup(session, latest_df)
        merge_seconds = round(time.perf_counter() - merge_started, 1)
    finally:
        applicable_df.unpersist()

    return {
        "rows_applied": rows_applied,
        "movers": movers,
        "country_tables": len(new_rows),
        "workers": choose_worker_count(new_rows, max_workers),
        "merge_seconds": merge_seconds,
    }


# ---- check: does the lookup agree with the country tables? -------------------------

def reconcile_lookup_with_country_tables(session):
    """One row per country: members the lookup says live there vs rows in the country table.
    `difference` must be 0 for every country. Not 0 means a failed run left a mover in two tables
    or a member in none; rerunning the batch repairs it.
    Run it after the job. It reads the whole lookup once and counts each country table."""
    country_tables = get_country_target_tables(session)
    lookup_counts = {r["country_code"]: r["count"]
                     for r in session.table(MEMBER_LOOKUP_TABLE).groupBy("country_code").count().collect()}

    result = []
    for code in sorted(set(country_tables) | set(lookup_counts)):
        table_name = country_tables.get(code)
        in_lookup = lookup_counts.get(code, 0)
        if table_name is None:                            # lookup holds a code that has no table
            result.append((code, None, in_lookup, None, None))
            continue
        in_table = session.table(table_name).count() if session.catalog.tableExists(table_name) else 0
        if in_lookup == 0 and in_table == 0:
            continue                                      # country never used
        result.append((code, table_name, in_lookup, in_table, in_table - in_lookup))

    return session.createDataFrame(
        result,
        "country_code STRING, country_table STRING, lookup_rows LONG, table_rows LONG, difference LONG",
    )

