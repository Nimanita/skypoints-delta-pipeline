# SkyPoints Member Pipeline: Design and Implementation Plan

## 1. Overview

This document describes the design of the SkyPoints member data pipeline on Databricks and Delta Lake, using the medallion architecture. It covers the full path from the landing zone, where the source team delivers files, through bronze and silver, to the per-country member tables.

The pipeline processes two daily feeds at billion-record scale:

| Feed | Format | Description |
|---|---|---|
| **Member profile** | Pipe-delimited flat file | Contains header (`\|H\|`) and detail (`\|D\|`) records |
| **Partner redemption** | JSON | One member per record, with a nested `redemptions` array |

The member data is then cleaned, enriched with derived columns, and split into one table per country. When a member moves country, the latest record wins: the member is removed from the old country table and appears in the new one.

Snowflake DDL is out of scope for this document.

## 2. Architecture

Data moves through landing, bronze and silver, and ends in the per-country tables. Each stage reads only from the stage before it.

```text
  Source team
      |
      |  daily member flat file + partner redemption JSON
      v
+-------------------------------------------------------------------+
| LANDING                                                           |
| Cloud object storage | one folder per feed | files never modified |
+-------------------------------------------------------------------+
      |
      |  Auto Loader (cloudFiles), checkpoint, append only
      v
+-------------------------------------------------------------------+
| BRONZE (Delta, all columns STRING)                                |
| bronze.member_raw                bronze.redemption_raw            |
+-------------------------------------------------------------------+
      |
      |  stream with trigger(availableNow=True), member data
      v
+-------------------------------------------------------------------+
| SILVER                                                            |
| Type, clean and validate | latest record wins | derive columns    |
+-------------------------------------------------------------------+
      |
      +-- invalid rows --> silver.member_rejects (quarantine)
      |
      |  valid rows
      v
+-------------------------------------------------------------------+
| COUNTRY TABLES                                                    |
| Upsert into the new country, delete movers from the old one       |
| One table per country: silver.member_india, silver.member_usa, ...|
+-------------------------------------------------------------------+
      |
      |  last step
      v
+-------------------------------------------------------------------+
| silver.member_country_lookup (current country per member)         |
+-------------------------------------------------------------------+
```

The redemption feed is ingested into bronze. The silver and country-table stages above apply to the member feed.

| Stage | Purpose | Location / Tables |
|---|---|---|
| **Landing** | Source team drops the daily files here. Files are stored exactly as received and never modified. | Cloud object storage, one folder per feed |
| **Bronze** | Raw ingestion into Delta. No business logic, no type casting, no rejection. | `bronze.member_raw`, `bronze.redemption_raw` |
| **Silver** | Typed, cleaned and validated data. Also holds the per-country target tables. | `silver.country_reference`, `silver.member_rejects`, `silver.member_country_lookup`, `silver.member_india`, `silver.member_usa`, `silver.member_canada`, ... |

**Processing order for member data:**

1. The source team delivers the daily files to the **landing** zone.
2. Auto Loader ingests new files into **bronze** (append only).
3. The silver job cleans and validates the new bronze data. Invalid rows go to `silver.member_rejects`.
4. Valid records are upserted into the **country tables**, and movers are deleted from their old country table.
5. `silver.member_country_lookup` is updated **last**, so a failed run can be retried safely.

## 3. Landing Layer

The landing layer is the hand-off point between the source team and the data platform.

1. **Delivery.** The source team delivers one member file and one redemption file per day to cloud object storage.
2. **Separate folders per feed.** The member and redemption feeds land in their own folders, so each can be ingested, monitored and replayed independently.
3. **Files are immutable.** The pipeline never edits, renames or overwrites a landed file. Landing is the permanent record of exactly what the source sent, and it is the replay source if bronze ever needs to be rebuilt.
4. **File name carries the feed date.** The file date is parsed from the file name and carried into bronze as `file_date`. It is later used to decide which record is the latest.
5. **No processing.** Nothing is parsed, cast or validated at this stage.

## 4. Bronze Layer

Bronze is the raw copy of what landed, stored as Delta tables. Its job is to capture every record reliably, so that a bad value can never block ingestion.

1. **File-arrival ingestion.** Use Auto Loader (`cloudFiles`) to read from the landing zone. Databricks tracks which files have already been picked up through a checkpoint, so each file is ingested exactly once.
2. **Storage format.** Delta tables.
3. **All columns stored as `STRING`.** Nothing is cast or rejected at this stage. The JSON feed keeps its nested shape as `ARRAY<STRUCT<txn_id, txn_date, partner, miles_redeemed, status>>`.
4. **Two tables.** One for member data and one for redemption transactions.
5. **Extra columns on every row.**
   - `ingestion_timestamp` (required)
   - Recommended: `source_file_name`, `file_date` (parsed from the file name), `row_number_in_file`, and `_rescued_data` (Auto Loader's catch-all for malformed rows)
6. **No clustering.** Bronze is append only, so files are already ordered by arrival.

## 5. Silver Layer

### 5.1 Incremental processing

The silver job reads bronze as a stream using `trigger(availableNow=True)`. It runs as a scheduled batch job: it processes everything that has arrived since the last run, then stops.

- The stream checkpoint replaces a manual watermark. It records how far bronze has been read, and it advances only after the batch function finishes successfully.
- A failed run re-reads the same bronze data on retry, so every silver write must be safe to rerun (see section 6.4).
- Late files are not a problem. A late file gets a later bronze `ingestion_timestamp`, so it is picked up in the next run.
- A manual watermark on `ingestion_timestamp` also works with a single writer, but the checkpoint does the same job with no state to maintain.
- Delta Change Data Feed is not needed, because bronze is append only.

### 5.2 Column-level transformations

Bronze stores every column as `STRING`. The table below defines, for each silver column, the expected data type, whether it is mandatory, what happens when the value is null or blank, the transformation or validation applied, and the data issues seen in the sample with how each is handled. The schema is identical in every country table.

Dates arrive as `YYYYMMDD`, except `dob`, which arrives as `DDMMYYYY`.

**Two kinds of failure apply to every column.** A **blank** optional column stays `NULL`. A **populated but invalid** value (wrong data type, or longer than the maximum length, such as a `state` of more than 5 characters) rejects the whole row to `silver.member_rejects`, whether the column is mandatory or not.

| Column | Expected data type | Mandatory | If null or blank | Transformation and validation | Issue seen in sample and handling |
|---|---|---|---|---|---|
| `member_name` | `STRING` (max 255) | Yes | Reject the row | Trim. Longer than 255 characters goes to rejects. Unique username. | The spec marks it as the key column, so it stays the key. |
| `member_id` | `STRING` (max 18) | Yes | Reject the row | Trim, no padding. Longer than 18 characters goes to rejects. Must be unique: an ID used by two different names goes to rejects. | Lengths vary (`22345`, `2256` versus 6 digits). All are valid up to 18 characters and are kept as they are, with no padding. |
| `enrollment_date` | `DATE` | Yes | Reject the row | Parse from `YYYYMMDD`. A value that cannot be parsed goes to rejects. | None seen. |
| `last_flight_date` | `DATE` | No | Stay `NULL` | Parse from `YYYYMMDD`. A value that cannot be parsed goes to rejects. | None seen. |
| `tier_code` | `STRING` (max 5) | No | Stay `NULL` | Trim. Longer than 5 characters goes to rejects. | None seen. |
| `agent_name` | `STRING` (max 255) | No | Stay `NULL` | Trim. Longer than 255 characters goes to rejects. | Missing on some rows, and the columns look shifted. Check the column count per row and reject shifted rows. |
| `state` | `STRING` (max 5) | No | Stay `NULL` | Trim. Longer than 5 characters goes to rejects. | None seen. |
| `country_code` | `STRING` (max 5) | Yes | Reject the row | Trim. Must match `silver.country_reference` exactly (section 5.4). No match goes to rejects. | Mixed codes (`AU`, `USA`, `PHIL`). Anything not in the reference list is rejected. The staging column is named `County` and is renamed to `Country`. The spec marks it optional, but routing needs it, so it is treated as mandatory. |
| `post_code` | `INT` | No | Stay `NULL` | Cast to `INT`. A value that is not a valid integer goes to rejects. | The spec lists it but the sample header does not. Treated as nullable until confirmed with the source team. |
| `dob` | `DATE` | No | Stay `NULL` | Left-pad to 8 characters, then parse as `DDMMYYYY`. A value that cannot be parsed goes to rejects. | The leading zero is lost (`3051985` versus `03051985`). Left-padding restores it before parsing. |
| `is_active` | `STRING` (1) | No | Stay `NULL` | Trim. Longer than 1 character goes to rejects. | None seen. |
| `age` | `INT` | No | `NULL` when `dob` is empty | **Derived.** Age computed from `dob`, relative to `file_date`. | None seen. |
| `stale_member` | `BOOLEAN` | No | `NULL` when `last_flight_date` is empty | **Derived.** `true` when the days between `last_flight_date` and `file_date` is greater than 90. | None seen. |
| `file_date` | `DATE` | Yes | Reject the row | Carried from bronze. Drives latest record wins. | None seen. |
| `ingestion_timestamp` | `TIMESTAMP` | Yes | Reject the row | Carried from bronze. Tie-breaker for latest record wins. | None seen. |
| `source_file_name` | `STRING` | Yes | Reject the row | Carried from bronze. Used for audit. | None seen. |

`age` and `stale_member` are computed relative to `file_date` (the feed date), not `current_date()`, so reruns give identical results.

### 5.3 Row-level rules

1. **Nothing is dropped silently.** A row that fails any check above goes to `silver.member_rejects` (section 5.5), with a reason code naming the column and the failed check.
2. **Invalid optional values reject the row.** An optional column that is blank stays `NULL`. An optional column that is populated but breaks its data type or maximum length (for example a `state` longer than 5 characters) sends the whole row to the reject table.
3. **Latest record wins.** Keep one row per `member_name`, ordered by `file_date`, then `ingestion_timestamp`, then `row_number_in_file`, newest first. This resolves repeats within a file, across files, and a member who reappears with a new country.

### 5.4 Country reference table

`silver.country_reference` is a fixed list with one row per country. It is set by us and is not built from the incoming data. The incoming country code must match `country_code` exactly.

| Column | Data type | Mandatory | Notes |
|---|---|---|---|
| `country_code` | `STRING` (max 5) | Yes | **Key.** For example `IND`, `USA`, `CAN`. |
| `country_name` | `STRING` | Yes | |
| `target_table` | `STRING` | Yes | For example `silver.member_india`. |

A record whose country code does not match goes to `silver.member_rejects`, with the raw record kept in its `_rescued_data` column. No country table is created for it.

### 5.5 Reject (quarantine) table

Every failed record is written to `silver.member_rejects` with the raw row in a `_rescued_data` column, a reason code, the source file name and the ingestion time. Validation results (mandatory checks, key uniqueness, format checks) surface here.

## 6. Country Split (Member Table)

This is the hardest part of the design, because a member who moves country must leave the old country table and appear in the new one.

### 6.1 Supporting table

Create `silver.member_country_lookup` if it does not exist.

| Column | Meaning |
|---|---|
| `member_name` | One row per member (the key) |
| `country_code` | The country the member currently lives in |
| `file_date` | Feed date of the record that set this value |

The table is small (one row per member) and answers one question: **where does this member live right now?** The `file_date` column stops old or late files from overwriting newer data.

### 6.2 Steps

1. **De-duplicate the incoming batch** on `member_name`, keeping the latest record (by `file_date`, then `ingestion_timestamp`, then row number). This is the **target DF**.
2. **Find movers** *before* touching `member_country_lookup`. Join the target DF to `member_country_lookup` on `member_name`, where the incoming country differs from the stored country.
3. **Update the country tables.**
   - Upsert every record into its (new) country table. `WHEN MATCHED` updates and `WHEN NOT MATCHED` inserts. This also covers brand-new members.
   - For movers only, delete the member from the **old** country table.
4. **Update `member_country_lookup` last.**

### 6.3 Why `member_country_lookup` goes last

It is the bookmark used to detect movers. If it were updated first and the job then crashed, the rerun would see no country change, and the old country table would keep a stale row forever. Updating it last means a crash leaves it at the old value, so the rerun finds the same movers and finishes the job.

### 6.4 No multi-table transactions in Delta

One `MERGE` cannot touch two tables, so each step is made **safe to rerun**:

- Upsert is idempotent: the same input gives the same rows.
- Delete is idempotent: deleting an already-deleted row does nothing.
- `member_country_lookup` is unchanged until the end, so movers are recomputed identically on retry.

### 6.5 How each case is handled

| Case | In `member_country_lookup`? | Country table | `member_country_lookup` |
|---|---|---|---|
| New member | No | Insert into their country table | Insert |
| Existing, same country | Yes, same country | Update in place | Update |
| Existing, moved | Yes, different country | Upsert into new, delete from old | Update |

### 6.6 Correct MERGE shape

In a `MERGE`, the table being changed is the **target** and the incoming batch is the **source**. Do not put the country comparison in the `ON` clause, or unchanged members count as "not matched" and are inserted twice. Put the change check in the `WHEN MATCHED AND ...` clause instead.

```sql
MERGE INTO member_country_lookup t
USING batch s
ON t.member_name = s.member_name
WHEN MATCHED AND s.file_date >= t.file_date THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

If a history of country moves is needed, append the movers to a separate `member_country_history` table (SCD2 style). Overwriting `member_country_lookup` alone is SCD1, which keeps only the current country.

## 7. Performance and Scale

| Table | Cluster by |
|---|---|
| Bronze (both tables) | Nothing |
| `member_country_lookup` | `member_name` |
| Country tables | `member_name` (never `country_code`, since each table holds one value) |

- Use **liquid clustering** (`CLUSTER BY (member_name)`) rather than partitioning. `member_name` is high cardinality, and partitioning on it would create millions of tiny folders.
- Delta has no traditional indexes. Speed comes from **data skipping** on min/max statistics, an optional **bloom filter** on `member_name`, and **deletion vectors** for cheap deletes.
- Enable **optimized writes and auto compaction** to avoid small files.
- De-duplicate only the incremental batch, then MERGE. Never window over the full history.
- The per-country loop runs once per country (a few hundred at most), not once per record. It can be parallelised with a thread pool.
- `member_country_lookup` is millions of rows, not billions, so the lookup join is cheap.

## 8. Build Order

1. Set up the landing zone folders for both feeds and agree the file naming convention with the source team.
2. Create the bronze tables and Auto Loader ingestion for both feeds.
3. Create `silver.country_reference` with the fixed country code list.
4. Build the silver member cleaning step: types, null handling, rejects, latest record wins, derived columns.
5. Create `member_country_lookup`.
6. Build the country split: dedup, movers, upsert, delete, then update `member_country_lookup`.
7. Add validation checks and the daily reconciliation check.
8. Make small, incremental commits at each step so the evolution is visible in the history.

## 9. Architect's Note: Are Separate Tables the Best Design?

For billions of records, physically separate country tables are not an ideal default:

- A country move touches two tables, so there is no atomicity.
- Skew: a few countries hold most rows while many hold very little, giving tiny tables and tiny files.
- Every schema change or maintenance policy is multiplied across hundreds of tables.
- Dirty country codes risk creating junk tables.

**Recommended production alternative:** one silver `members` table with liquid clustering on `(country_code, member_name)`, and a **view per country**. A move becomes one atomic MERGE that updates the `country_code` column. Per-country access can use Unity Catalog row filters.

**Separate tables are justified when** there are data residency rules, different retention per country, or isolated consumers per country.

This design builds separate country tables as required, and records the alternative above for future consideration.
