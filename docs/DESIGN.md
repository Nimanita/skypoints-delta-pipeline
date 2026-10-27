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

Bronze is the raw copy of what landed, stored as Delta tables. Its job is to capture every record reliably, so that a bad value can never block ingestion. This section describes the bronze layer **as implemented** (notebooks `bronze_create_member` and `bronze_create_redemptions`).

### 4.1 Principles

1. **Raw and complete.** No business logic, no type casting, no rejection. Every value is stored as it arrived.
2. **All data columns are `STRING`.** This protects values that look numeric but are not, such as `000224160` and `03051985`, which would lose their leading zeros if cast. The JSON feed keeps its nested shape as `ARRAY<STRUCT<...>>` with `STRING` fields inside.
3. **Nothing is lost.** Fields that do not fit the declared schema are kept in `_rescued_data` instead of being dropped.
4. **Append only, exactly once per file.** Auto Loader tracks ingested files in a checkpoint, so a file is never loaded twice and a rerun does nothing new.
5. **Replayable.** Landing files are immutable, so bronze can be rebuilt at any time by clearing the checkpoint and the table and reloading from landing.
6. **Traceable.** Every row carries the file it came from, the feed date and the load time (section 4.4).

### 4.2 Source to target

| | Member feed | Redemption feed |
|---|---|---|
| **Landing path** | `/Volumes/skypoints/landing/members` | `/Volumes/skypoints/landing/redemptions` |
| **File name pattern** | `SKYPOINTS_MEMBERS_<YYYYMMDD>_<nnnnnnnn>.txt` | `SKYPOINTS_REDEMPTIONS_<YYYYMMDD>_<nnnnnnnn>.json` |
| **File format** | Pipe-delimited, no header row for the reader. Records are tagged `H` (header) or `D` (detail). | Newline-delimited JSON, one member per line, with a nested `redemptions` array |
| **Bronze table** | `skypoints.bronze.raw_member` | `skypoints.bronze.raw_redemptions` |
| **Grain** | One row per detail line in the file | One row per member per feed (nested array kept as is) |
| **Checkpoint** | `/Volumes/skypoints/bronze/members/member_checkpoints` | `/Volumes/skypoints/bronze/redemptions/redemption_checkpoints` |
| **Storage format** | Delta | Delta |
| **Write mode** | Append only | Append only |

### 4.3 Ingestion configuration

Both feeds use Auto Loader (`cloudFiles`) as a stream with `trigger(availableNow=True)`: it ingests every new file, then stops. This makes it a scheduled batch job with file-level exactly-once tracking.

| Setting | Member feed | Redemption feed | Why |
|---|---|---|---|
| `cloudFiles.format` | `csv` | `json` | Source file type |
| `delimiter` | `\|` | n/a | Pipe-delimited source |
| `header` | `false` | n/a | The header line is a normal `\|H\|` record, handled by the filter below |
| Schema | Explicit, 13 `STRING` positions (`filler`, `record_type` and 11 data fields) | Explicit, `STRING` fields with a nested `ARRAY<STRUCT>` | No schema inference, so types can never drift between files |
| `rescuedDataColumn` | `_rescued_data` | `_rescued_data` | Keeps anything that does not fit the schema |
| Row filter | `record_type = 'D'` | None | Removes the header record. `filler` and `record_type` are then dropped. |
| Trigger | `availableNow=True` | `availableNow=True` | Process what is there, then stop |

The `filler` column exists only because every line starts with a pipe, so the text before it is an empty first field.

### 4.4 Table schemas

#### 4.4.1 `skypoints.bronze.raw_member`

| Column | Type | Source field | Notes |
|---|---|---|---|
| `member_id` | `STRING` | `Member_Id` | Kept as text. Leading zeros are preserved. |
| `member_name` | `STRING` | `Member_Name` | |
| `country` | `STRING` | `Country` | Silver renames to `country_code`. |
| `dob` | `STRING` | `DOB` | `DDMMYYYY`. Text on purpose: `03051985` must keep its leading zero. |
| `enrollment_date` | `STRING` | `Enrollment_Date` | `YYYYMMDD` |
| `last_flight_date` | `STRING` | `Last_Flight_Date` | `YYYYMMDD` |
| `tier` | `STRING` | `Tier_Code` | Silver renames to `tier_code`. |
| `is_active` | `STRING` | `Is_Active` | |
| `agent_name` | `STRING` | `Agent_Name` | |
| `state` | `STRING` | `State` | |
| `post_code` | `STRING` | `Post_Code` | |
| `_rescued_data` | `STRING` | Auto Loader | Surplus or unparseable fields, as JSON |
| `source_file_name` | `STRING` | `_metadata.file_name` | Audit |
| `file_date` | `DATE` | File name | Audit |
| `ingestion_timestamp` | `TIMESTAMP` | `current_timestamp()` | Audit |

An empty field between two pipes is stored as `NULL`, not as an empty string.

#### 4.4.2 `skypoints.bronze.raw_redemptions`

| Column | Type | Source field | Notes |
|---|---|---|---|
| `member_id` | `STRING` | `member_id` | `NULL` when the source uses another key name (see section 4.6) |
| `feed_date` | `STRING` | `feed_date` | The date the source states inside the record. Not the same as `file_date`. |
| `redemptions` | `ARRAY<STRUCT<txn_id, txn_date, partner, miles_redeemed, status>>` | `redemptions` | Every inner field is `STRING`. Numbers such as `miles_redeemed` are stored as text. Exploded in silver. |
| `_rescued_data` | `STRING` | Auto Loader | Fields not in the schema: `memberId`, `source_system`, `fee_usd`, `channel`, `route`, and so on |
| `source_file_name` | `STRING` | `_metadata.file_name` | Audit |
| `file_date` | `DATE` | File name | Audit |
| `ingestion_timestamp` | `TIMESTAMP` | `current_timestamp()` | Audit |

### 4.5 Audit and metadata columns

Every bronze row carries the same four audit columns, so any row can be traced back to its file and load.

| Column | Type | Populated from | Purpose | Used downstream by |
|---|---|---|---|---|
| `source_file_name` | `STRING` | `_metadata.file_name` | Lineage: which landed file the row came from | Reject table, audit, replay of a single file |
| `file_date` | `DATE` | Regex `_(\d{8})_` on the file name, parsed as `yyyyMMdd` | The feed date the source assigned to the file | Latest record wins, `age`, `stale_member` |
| `ingestion_timestamp` | `TIMESTAMP` | `current_timestamp()` at load | When the row entered bronze. One value per micro-batch, so all rows of one run share it. | Incremental reads, tie-breaker between files |
| `_rescued_data` | `STRING` (JSON) | Auto Loader | Anything that did not match the schema, plus the source file path | Schema drift monitoring, recovery of renamed or new fields |

**Recommended additions, not yet implemented:**

| Column | Populated from | Why it would help |
|---|---|---|
| `row_number_in_file` | Position of the line in the file | Silver rule 5.3 uses it as the last tie-breaker (see section 4.9) |
| `source_file_path` | `_metadata.file_path` | Full path, useful when the same file name exists in two folders |
| `file_modification_time` | `_metadata.file_modification_time` | Detects a file that was replaced after delivery |
| `pipeline_run_id` | Job run ID | Ties a row to the exact job run that loaded it |

### 4.6 Sample records

The samples below come from the delivered files `SKYPOINTS_MEMBERS_20240115_03553580.txt` and `SKYPOINTS_REDEMPTIONS_20240115_04280208.json`. The bronze values are what the schema and options in section 4.3 produce for those lines. `<load time>` stands for the run time.

#### 4.6.1 Member feed

**Raw lines in the landed file:**

```text
|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|Tier_Code|Agent_Name|State|Country|Post_Code|DOB|Is_Active
|D|Sanjay Smith|225138|20191118|20240115|SLV|Sam|MY3|MYT|73072|25031957|A
|D|Leila Tanaka|226168|20150704|20231127|PLT|Diego|SP1|SPM||12121984|A
|D|Ivan Kim 3494|000224160|20161012|20240112|SLV|Omar|AG2|AGO|52056|08051993|A
|D|Aisha Garcia|226045|20230210|20231215|GLD|GI1|GIN|35243|10081986|A
```

The `H` line is removed by the `record_type = 'D'` filter. The four `D` lines become these `raw_member` rows:

| Column | A: clean row | B: blank post code | C: leading zeros | D: missing agent, fields shifted |
|---|---|---|---|---|
| `member_id` | `225138` | `226168` | `000224160` | `226045` |
| `member_name` | `Sanjay Smith` | `Leila Tanaka` | `Ivan Kim 3494` | `Aisha Garcia` |
| `country` | `MYT` | `SPM` | `AGO` | `35243` |
| `dob` | `25031957` | `12121984` | `08051993` | `A` |
| `enrollment_date` | `20191118` | `20150704` | `20161012` | `20230210` |
| `last_flight_date` | `20240115` | `20231127` | `20240112` | `20231215` |
| `tier` | `SLV` | `PLT` | `SLV` | `GLD` |
| `is_active` | `A` | `A` | `A` | `NULL` |
| `agent_name` | `Sam` | `Diego` | `Omar` | `GI1` |
| `state` | `MY3` | `SP1` | `AG2` | `GIN` |
| `post_code` | `73072` | `NULL` | `52056` | `10081986` |
| `_rescued_data` | `NULL` | `NULL` | `NULL` | `NULL` |
| `source_file_name` | `SKYPOINTS_MEMBERS_20240115_03553580.txt` | same | same | same |
| `file_date` | `2024-01-15` | same | same | same |
| `ingestion_timestamp` | `<load time>` | same | same | same |

- **B** shows that a blank field becomes `NULL`.
- **C** shows why every column is `STRING`: the leading zeros of `000224160` and `08051993` survive.
- **D** shows that bronze does not repair a shifted row. The source left out `agent_name`, so every later field moved one place left. The row is stored exactly as it arrived, and silver rejects it because `country` holds `35243`, which is not in `silver.country_reference`.

#### 4.6.2 Redemption feed

**Raw lines in the landed file:**

```json
{"member_id": "223457", "feed_date": "20240115", "redemptions": [{"txn_id": "RX10091", "txn_date": "20240110", "partner": "AeroLink", "miles_redeemed": 12000, "status": "COMPLETED"}, {"txn_id": "RX10092", "txn_date": "20240113", "partner": "SkyPoints", "miles_redeemed": 5000, "status": "PENDING"}]}
{"member_id": "224144", "feed_date": "20240115", "redemptions": [{"txn_id": "RX13438", "txn_date": "20240111", "partner": "NorthStar", "miles_redeemed": 22000, "status": "COMPLETED", "fee_usd": 12.28, "channel": "KIOSK"}], "source_system": "PARTNER_API_V2"}
{"feed_date": "20240115", "redemptions": [{"txn_id": "RX13445", "txn_date": "20240108", "partner": "NorthStar", "miles_redeemed": 55000, "status": "CANCELLED"}], "memberId": "226120"}
```

**Resulting `raw_redemptions` rows** (audit columns are the same on every row: `source_file_name` = `SKYPOINTS_REDEMPTIONS_20240115_04280208.json`, `file_date` = `2024-01-15`, `ingestion_timestamp` = `<load time>`):

| `member_id` | `feed_date` | `redemptions` (`txn_id`, `txn_date`, `partner`, `miles_redeemed`, `status`) | `_rescued_data` |
|---|---|---|---|
| `223457` | `20240115` | `[(RX10091, 20240110, AeroLink, 12000, COMPLETED), (RX10092, 20240113, SkyPoints, 5000, PENDING)]` | `NULL` |
| `224144` | `20240115` | `[(RX13438, 20240111, NorthStar, 22000, COMPLETED)]` | JSON holding `source_system` and the inner `fee_usd` and `channel` |
| `NULL` | `20240115` | `[(RX13445, 20240108, NorthStar, 55000, CANCELLED)]` | JSON holding `memberId` = `226120` |

- Row 1 is the normal case. The number `12000` is stored as the text `12000`.
- Row 2 has fields the schema does not list. They are not dropped: they sit in `_rescued_data`, and the known fields load normally.
- Row 3 uses `memberId` instead of `member_id`. The schema does not know that name, so `member_id` is `NULL` and the value is recoverable from `_rescued_data`. Silver decides how to map it.

### 4.7 Raw data issues seen in the sample files and how bronze treats them

Bronze stores these as they arrive. The last column says where each one is dealt with.

#### Member feed

| Issue seen | Example | Bronze behaviour | Dealt with in |
|---|---|---|---|
| Header record | `\|H\|Member_Name\|...` | Filtered out (`record_type = 'D'`). Not stored. | Bronze |
| Blank lines | 2 empty lines in the file | Ignored by the reader | Bronze |
| Blank field | `\|SPM\|\|12121984\|` | Stored as `NULL` | Silver (mandatory and optional checks) |
| Missing field, later fields shift left | `Aisha Garcia`, `Liam Nguyen 7458` | Stored as arrived, last column `NULL` | Silver: the shifted `country` fails the reference check |
| Truncated line | `Maya Smith 6671` (line ends after `NER`) | Missing trailing fields stored as `NULL` | Silver. A short line is indistinguishable from blank optional fields. |
| Extra delimiter inside a value | `Ravi\|Kumar` | Fields shift right and the surplus last token goes to `_rescued_data` | Silver: unparseable dates and IDs reject the row |
| Leading zeros | `000224160`, `08051993` | Preserved | Silver |
| Lost leading zero | `7101966` (should be `07101966`) | Stored as arrived | Silver: left-pad to 8, then parse |
| Invalid or unusual dates | `20211301` and `Sep 11 2019` in `enrollment_date`, `07102023` in `dob` | Stored as text | Silver: date parsing |
| Lower-case or unknown codes | `gld`, `slv`, `XYZ`, `hmd` | Stored as arrived | Silver |
| Very long values | `Longname xxxx...`, `Agent zzzz...` (over 255 characters) | Stored in full | Silver: length checks |
| Non-ASCII and quoted names | `Иван Петров`, `Zoë Müller 5606`, `Anne-Marie "Annie" Clarke` | Stored as UTF-8 text | Silver |
| Repeated members | `Sofia Walker 5035` twice (identical), `Oscar Tanaka 6447` twice (different state) | All rows kept | Silver: latest record wins |
| Same ID, different member | `226129` for `Kenji Rossi 6644` and `Noah Tanaka` | Both rows kept | Silver: uniqueness check |

#### Redemption feed

| Issue seen | Example | Bronze behaviour | Dealt with in |
|---|---|---|---|
| Different key name | `memberId` instead of `member_id` (2 rows) | `member_id` is `NULL`, value in `_rescued_data` | Silver |
| No member key at all | 1 row | `member_id` is `NULL` | Silver |
| Number instead of string | `"member_id": 224279` | Stored as the text `224279` | Silver |
| Unexpected fields | `source_system`, `fee_usd`, `channel`, nested `route` | Kept in `_rescued_data` | Silver, or a later schema update |
| Mixed types in `miles_redeemed` | `12000`, `"5000"`, `null`, `0`, `-11500` | All stored as text, `null` stays `NULL` | Silver: type and range checks |
| Status spelling | `completed`, `REVERSED`, `ON_HOLD` | Stored as arrived | Silver: standardise |
| Date format variants | `feed_date` of `2024-01-15`, and `20240109` inside a `20240115` file | Stored as arrived | Silver |
| Empty array | `"redemptions": []` | Stored as an empty array | Silver: the explode produces no rows |
| Repeated keys | `txn_id` `RX13413` and `RX13411` in two different rows, `RX13407` twice in one row; `member_id` `224365` in two rows | All kept | Silver: de-duplication |

### 4.8 Bronze acceptance checks

Run after each load. Counts for the two sample files are shown for reference.

| Check | Query | Expected for the sample files |
|---|---|---|
| Row count matches the file | `SELECT source_file_name, COUNT(*) FROM skypoints.bronze.raw_member GROUP BY 1` | 148 (file has 151 lines: 1 header, 2 blank, 148 detail) |
| Row count matches the file | `SELECT source_file_name, COUNT(*) FROM skypoints.bronze.raw_redemptions GROUP BY 1` | 200 rows, holding 377 nested transactions |
| Schema drift | `SELECT COUNT(*) FROM skypoints.bronze.raw_redemptions WHERE _rescued_data IS NOT NULL` | A small, stable number. A sudden rise means the source changed its layout. |
| Every row has audit values | `SELECT COUNT(*) FROM <table> WHERE source_file_name IS NULL OR file_date IS NULL OR ingestion_timestamp IS NULL` | 0 |
| No file loaded twice | `SELECT source_file_name, COUNT(DISTINCT ingestion_timestamp) FROM <table> GROUP BY 1 HAVING COUNT(DISTINCT ingestion_timestamp) > 1` | No rows |

### 4.9 Design notes and open items

| Topic | Original design | As implemented | Impact and next step |
|---|---|---|---|
| Table names | `bronze.member_raw`, `bronze.redemption_raw` | `skypoints.bronze.raw_member`, `skypoints.bronze.raw_redemptions` | Naming only. Use the implemented names in silver. |
| `row_number_in_file` | Recommended audit column | Not implemented | Silver rule 5.3 lists it as the last tie-breaker. Within one file, `file_date` and `ingestion_timestamp` are identical for every row, so two rows for the same member (for example `Oscar Tanaka 6447`) cannot be ordered. **Decision needed:** add the column at bronze, or choose another deterministic tie-breaker in silver, such as `last_flight_date`. |
| Header record | No rejection at bronze | `H` records are filtered out | The header carries no member data, so nothing is lost. Note that the filter also removes any line whose record type is not exactly `D`, such as a lower-case `d`. Check the row count against the file (section 4.8). |
| `tier` column name | Source header is `Tier_Code` | Bronze column is `tier` | Silver renames it to `tier_code`. |
| Clustering | None | None | Bronze is append only, so files are already ordered by arrival. |
| Table hardening | Not specified | Not applied | Optional: set `delta.appendOnly = true` so no job can update or delete bronze rows, and enable optimized writes and auto compaction. |

**Operational notes**

- **Replay.** To rebuild a bronze table, stop the job, drop or truncate the table, delete its checkpoint folder, and run the notebook again. Landing is never touched.
- **Late or corrected files.** A file with a new name is picked up on the next run. A file re-delivered under the same name is **not** loaded again, so the source must send corrections under a new file name.
- **Schema changes.** The schema is explicit, so a new source field never breaks the load. It appears in `_rescued_data` until the schema and table are updated and the field is back-filled from the rescued JSON.

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
