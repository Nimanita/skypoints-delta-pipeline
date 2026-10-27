"""Country list, the split into one table per country, one-time backfill, reconciliation (design 5.4 / 6).
The MERGE statements are plain SQL strings next to the code that runs them."""
import re
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, trim

# ---- tables --------------------------------------------------------------------
# (the two fixed tables are created by the %sql cells in the notebook: rename there too)
COUNTRY_TABLE = "skypoints.silver.country"      # fixed list, loaded by setup_country_lookup_table
COUNTRY_CODE  = "country_code"                         # column in the reference table that holds the code
COUNTRY_TARGET_TABLE = "target_table"                        # optional column in the reference table: table name per country (design 5.4)
MEMBER_LOOKUP_TABLE  = "skypoints.silver.member_country_lookup"   # where does each member live right now (design 6.1)
COUNTRY_TABLE_SCHEMA = "skypoints.silver"                  # country tables are created in this schema
LEGACY_MASTER_TABLE  = "skypoints.silver.member"           # old single master table, only read by the one-time backfill

# column order of every country table
CLEAN_COLS = [
    "member_name", "member_id", "enrollment_date", "last_flight_date", "tier_code", "agent_name",
    "state", "country_code", "post_code", "dob", "is_active", "age", "stale_member",
    "file_date", "ingestion_timestamp", "source_file_name",
]

# ---- the fixed country list ----------------------------------------------------

def read_known_country_codes(session):
    return session.table(COUNTRY_TABLE).select(
        col(COUNTRY_CODE),
        lit(True).alias("is_known_country")
    )


