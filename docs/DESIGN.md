# SkyPoints Pipeline: Design and Implementation Plan

## 1. Overview

This document describes the design of the SkyPoints data pipeline on Databricks and Delta Lake, using the medallion architecture. It covers the full path from the landing zone, where the source team delivers files, through bronze and silver, to the per-country member tables and the flattened redemption table.

The pipeline processes two daily feeds at billion-record scale:

| Feed | Format | Description | Silver result |
|---|---|---|---|
| **Member profile** | Pipe-delimited flat file | Contains header (`\|H\|`) and detail (`\|D\|`) records | One table per country, plus a lookup of each member's current country |
| **Partner redemption** | Newline-delimited JSON | One member per record, with a nested `redemptions` array | One flat table, one row per redemption transaction |

When a member moves country, the latest record wins: the member is removed from the old country table and appears in the new one. When a redemption transaction changes status, the latest feed wins and the row is overwritten (SCD1).

Snowflake DDL is out of scope for this document.

### 1.1 Status

| Component | Status |
|---|---|
| Landing zone, both feeds | Done |
| Bronze member (`raw_member`) | Done |
| Bronze redemptions (`raw_redemptions`) | Done |
| Country reference table | Done |
| Silver member: clean, validate, rejects, latest wins, derived columns | Done |
| Country split and `member_country_lookup` | Done |
| Silver redemption: `redemption_transaction` and `redemption_rejects` | **Designed in section 7, to be built** |
| Join of redemptions to members (adds `member_id` to the lookup) | **Designed in section 8, to be built** |
| Gold layer | Out of scope |

### 1.2 Conventions used everywhere

| Convention | Rule |
|---|---|
| **Date storage** | Every date column in silver, and `file_date` in bronze, is a Delta `DATE`, written and displayed as `YYYY-MM-DD`. Raw date text in bronze (`20240115`, `03051985`, `2024-01-15`) stays exactly as it arrived, because bronze does no casting. |
| **Nothing dropped silently** | A row that fails a rule goes to a reject table with a reason code and the raw record. |
| **Reject reason format** | `column:CHECK`, several codes joined with `"; "`. Example: `state:INVALID_FORMAT; dob:INVALID_DATE`. |
| **Deterministic reruns** | Derived values use `file_date`, never `current_date()`. Ties in "latest wins" are broken by explicit columns, so two runs give the same result. |
| **Idempotent writes** | Every silver write is safe to repeat (see 5.1, 6.4 and 7.5). |
| **Catalog layout** | Catalog `skypoints`, schemas `landing`, `bronze`, `silver`. |

### 1.3 Repository layout

```text
├── data
│   ├── country_reference.csv              fixed country list, loaded by setup_country_table
│   ├── flat_files/SKYPOINTS_MEMBERS_<YYYYMMDD>_<nnnnnnnn>.txt            10 sample files, 2024-01-06 to 2024-01-15
│   └── json_feeds/SKYPOINTS_REDEMPTIONS_<YYYYMMDD>_<nnnnnnnn>.json       10 sample files, 2024-01-06 to 2024-01-15
├── docs
│   └── DESIGN.md                          this document
├── pipeline_utils
│   └── silver
│       ├── clean_and_validate.py          member: trim, convert, validate, derive, latest record
│       ├── country_tables.py              member: country split, lookup, reconciliation
│       ├── process_member_batch.py        member: what runs per micro-batch, reject writer
│       ├── clean_and_validate_redemption.py   (planned) redemption: resolve key, explode, validate, dedupe
│       └── process_redemption_batch.py        (planned) redemption: per micro-batch, reject writer, merge
└── src
    ├── init
    │   ├── initial_setup.ipynb            catalog, schemas, volumes
    │   └── setup_country_table.ipynb      loads data/country_reference.csv
    ├── bronze
    │   ├── bronze_create_member.ipynb
    │   └── bronze_create_redemptions.ipynb
    └── silver
        ├── silver_create_member.ipynb
        └── silver_create_redemption_transaction.ipynb   (planned)
```

Notebooks hold only table DDL, configuration and the stream start. All logic lives in `pipeline_utils`, so it can be unit tested without a cluster.

## 2. Architecture

Data moves through landing, bronze and silver. Each stage reads only from the stage before it.

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
| BRONZE (Delta, all data columns STRING)                           |
| bronze.raw_member                bronze.raw_redemptions           |
+-------------------------------------------------------------------+
      |                                      |
      | stream, availableNow                 | stream, availableNow
      v                                      v
+-----------------------------------+  +-----------------------------------+
| SILVER: MEMBER                    |  | SILVER: REDEMPTION                |
| type, clean, validate             |  | resolve member_id, explode,       |
| latest record per member          |  | type, clean, validate             |
| derive age, stale_member          |  | latest record per transaction     |
+-----------------------------------+  +-----------------------------------+
      |                  |                   |                  |
      | invalid          | valid             | invalid          | valid
      v                  v                   v                  v
 member_rejects    country tables      redemption_rejects  redemption_transaction
                   member_<code>                           (one flat table, SCD1)
                         |
                         | last step
                         v
              member_country_lookup  <---- join on member_id (section 8)
```

| Stage | Purpose | Location / Tables |
|---|---|---|
| **Landing** | Source team drops the daily files here. Files are stored exactly as received and never modified. | `/Volumes/skypoints/landing/members`, `/Volumes/skypoints/landing/redemptions` |
| **Bronze** | Raw ingestion into Delta. No business logic, no type casting, no rejection. | `bronze.raw_member`, `bronze.raw_redemptions` |
| **Silver, member** | Typed, cleaned and validated members, split into one table per country. | `silver.country`, `silver.member_rejects`, `silver.member_country_lookup`, `silver.member_<code>` |
| **Silver, redemption** | Typed, cleaned and validated redemption transactions in one flat table. | `silver.redemption_transaction`, `silver.redemption_rejects` |

**Processing order:**

1. The source team delivers the daily files to **landing**.
2. Auto Loader ingests new files into **bronze** (append only).
3. Each silver job reads bronze as a stream, processes everything new, and stops.
4. Invalid rows go to the feed's reject table. Valid rows are written to the silver target.
5. For members, `member_country_lookup` is updated **last**, so a failed run can be retried safely.

The two feeds are processed independently. They can arrive in any order, which is why a redemption is never rejected because its member is not found yet (section 8).

## 3. Landing Layer

The landing layer is the hand-off point between the source team and the data platform.

1. **Delivery.** The source team delivers one member file and one redemption file per day to cloud object storage. The sample files in `data/flat_files` and `data/json_feeds` are uploaded to the two landing volumes.
2. **Separate folders per feed.** Each feed can be ingested, monitored and replayed independently.
3. **Files are immutable.** The pipeline never edits, renames or overwrites a landed file. Landing is the permanent record of what the source sent, and the replay source if bronze needs to be rebuilt.
4. **File name carries the feed date.** `SKYPOINTS_<FEED>_<YYYYMMDD>_<nnnnnnnn>.<ext>`. The date is parsed into bronze as `file_date` (a `DATE`, `YYYY-MM-DD`) and later decides which record is the latest.
5. **No processing.** Nothing is parsed, cast or validated here.

## 4. Bronze Layer

Bronze is the raw copy of what landed, stored as Delta tables. Its job is to capture every record reliably, so that a bad value can never block ingestion. This section describes bronze **as implemented** (notebooks `bronze_create_member` and `bronze_create_redemptions`).

### 4.1 Principles

1. **Raw and complete.** No business logic, no type casting, no rejection. Every value is stored as it arrived.
2. **All data columns are `STRING`.** This protects values that look numeric but are not, such as `000224160` and `03051985`, which would lose their leading zeros if cast. The JSON feed keeps its nested shape as `ARRAY<STRUCT<...>>` with `STRING` fields inside.
3. **Nothing is lost.** Fields that do not fit the declared schema are kept in `_rescued_data` instead of being dropped.
4. **Append only, exactly once per file.** Auto Loader tracks ingested files in a checkpoint, so a file is never loaded twice.
5. **Replayable.** Landing files are immutable, so bronze can be rebuilt by clearing the checkpoint and the table and reloading from landing.
6. **Traceable.** Every row carries its file, the feed date and the load time (section 4.5).

The only typed bronze column is the audit column `file_date`, which is a `DATE` shown as `YYYY-MM-DD`. It comes from the file name, not from the data.

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
| Schema | Explicit, 13 `STRING` positions (`filler`, `record_type` and 11 data fields) | Explicit, `STRING` fields with a nested `ARRAY<STRUCT>` | No schema inference, so types never drift between files |
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
| `dob` | `STRING` | `DOB` | Source format `DDMMYYYY`. Text on purpose: `03051985` must keep its leading zero. |
| `enrollment_date` | `STRING` | `Enrollment_Date` | Source format `YYYYMMDD`, stored as text |
| `last_flight_date` | `STRING` | `Last_Flight_Date` | Source format `YYYYMMDD`, stored as text |
| `tier` | `STRING` | `Tier_Code` | Silver renames to `tier_code`. |
| `is_active` | `STRING` | `Is_Active` | |
| `agent_name` | `STRING` | `Agent_Name` | |
| `state` | `STRING` | `State` | |
| `post_code` | `STRING` | `Post_Code` | |
| `_rescued_data` | `STRING` | Auto Loader | Surplus or unparseable fields, as JSON |
| `source_file_name` | `STRING` | `_metadata.file_name` | Audit |
| `file_date` | `DATE` | File name | Audit. Stored as `YYYY-MM-DD`. |
| `ingestion_timestamp` | `TIMESTAMP` | `current_timestamp()` | Audit |

An empty field between two pipes is stored as `NULL`, not as an empty string.

#### 4.4.2 `skypoints.bronze.raw_redemptions`

| Column | Type | Source field | Notes |
|---|---|---|---|
| `member_id` | `STRING` | `member_id` | `NULL` when the source uses another key name (see 4.6) |
| `feed_date` | `STRING` | `feed_date` | The date the source states inside the record, as text. Not the same as `file_date`. |
| `redemptions` | `ARRAY<STRUCT<txn_id, txn_date, partner, miles_redeemed, status>>` | `redemptions` | Every inner field is `STRING`. Numbers such as `miles_redeemed` are stored as text. Exploded in silver. |
| `_rescued_data` | `STRING` | Auto Loader | Fields not in the schema: `memberId`, `source_system`, `fee_usd`, `channel`, `route`, and so on |
| `source_file_name` | `STRING` | `_metadata.file_name` | Audit |
| `file_date` | `DATE` | File name | Audit. Stored as `YYYY-MM-DD`. |
| `ingestion_timestamp` | `TIMESTAMP` | `current_timestamp()` | Audit |

### 4.5 Audit and metadata columns

Every bronze row carries the same four audit columns, so any row can be traced back to its file and load.

| Column | Type | Populated from | Purpose | Used downstream by |
|---|---|---|---|---|
| `source_file_name` | `STRING` | `_metadata.file_name` | Lineage: which landed file the row came from | Reject tables, audit, replay of one file |
| `file_date` | `DATE` | Regex `_(\d{8})_` on the file name, parsed as `yyyyMMdd` | The feed date the source assigned to the file | Latest record wins, `age`, `stale_member` |
| `ingestion_timestamp` | `TIMESTAMP` | `current_timestamp()` at load | When the row entered bronze. One value per micro-batch. | Tie-breaker between files |
| `_rescued_data` | `STRING` (JSON) | Auto Loader | Anything that did not match the schema, plus the source file path | Schema drift monitoring, recovery of renamed or new fields (for example `memberId`) |

**Not implemented, and no longer needed:** `row_number_in_file`. The silver member job breaks ties with `last_flight_date` and then all remaining columns (5.5). The silver redemption job breaks ties with the position of the transaction inside the array (7.5).

**Optional additions:** `source_file_path` (`_metadata.file_path`, for the same file name in two folders), `file_modification_time` (detects a file replaced after delivery), `pipeline_run_id`.

### 4.6 Sample records

The samples come from `SKYPOINTS_MEMBERS_20240115_03553580.txt` and `SKYPOINTS_REDEMPTIONS_20240115_04280208.json`. `<load time>` stands for the run time.

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
- **D** shows that bronze does not repair a shifted row. The source left out `agent_name`, so every later field moved one place left. Silver rejects it because `country` holds `35243`, which is not in `silver.country`.

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
- Row 3 uses `memberId` instead of `member_id`, so `member_id` is `NULL` and the value is recoverable from `_rescued_data`. Silver recovers it (7.2).

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
| Invalid or unusual dates | `20211301` and `Sep 11 2019` in `enrollment_date`, `07102023` in `dob` | Stored as text | Silver: date parsing (5.3) |
| Lower-case or unknown codes | `gld`, `slv`, `XYZ`, `hmd` | Stored as arrived | Silver |
| Very long values | `Longname xxxx...`, `Agent zzzz...` (over 255 characters) | Stored in full | Silver: length checks |
| Non-ASCII and quoted names | `Иван Петров`, `Zoë Müller 5606`, `Anne-Marie "Annie" Clarke` | Stored as UTF-8 text | Silver: format check (see open item 12.2) |
| Repeated members | `Sofia Walker 5035` twice (identical), `Oscar Tanaka 6447` twice (different state) | All rows kept | Silver: latest record wins |
| Same ID, different member | `226129` for `Kenji Rossi 6644` and `Noah Tanaka` | Both rows kept | Silver: `member_id` uniqueness (open item 12.1) |

#### Redemption feed

| Issue seen | Example | Bronze behaviour | Dealt with in |
|---|---|---|---|
| Different key name | `memberId` instead of `member_id` (2 rows) | `member_id` is `NULL`, value in `_rescued_data` | Silver: recovered (7.2) |
| No member key at all | 1 row | `member_id` is `NULL` | Silver: each transaction rejected with `member_id:MISSING` |
| Number instead of string | `"member_id": 224279` | Stored as the text `224279` | Silver |
| Unexpected fields | `source_system`, `fee_usd`, `channel`, nested `route` | Kept in `_rescued_data` | Silver ignores them. A later schema update can promote them. |
| Mixed types in `miles_redeemed` | `12000`, `"5000"`, `null`, `0`, `-11500` | All stored as text, `null` stays `NULL` | Silver: type and range checks (7.4) |
| Status spelling | `completed`, `REVERSED`, `ON_HOLD` | Stored as arrived | Silver: upper-case, then allowed-list check |
| Date format variants | `feed_date` of `2024-01-15`, and `20240109` inside a `20240115` file | Stored as arrived | Silver: both `yyyyMMdd` and `yyyy-MM-dd` accepted |
| Empty array | `"redemptions": []` | Stored as an empty array | Silver: produces no rows, counted in the batch log, not a reject |
| Repeated keys | `txn_id` `RX13413` and `RX13411` in two different rows, `RX13407` twice in one row; `member_id` `224365` in two rows | All kept | Silver: latest record per key (7.5) |

### 4.8 Bronze acceptance checks

Run after each load. Counts for the two sample files are shown for reference.

| Check | Query | Expected for the sample files |
|---|---|---|
| Row count matches the file | `SELECT source_file_name, COUNT(*) FROM skypoints.bronze.raw_member GROUP BY 1` | 148 (file has 151 lines: 1 header, 2 blank, 148 detail) |
| Row count matches the file | `SELECT source_file_name, COUNT(*) FROM skypoints.bronze.raw_redemptions GROUP BY 1` | 200 rows, holding 377 nested transactions |
| Schema drift | `SELECT COUNT(*) FROM skypoints.bronze.raw_redemptions WHERE _rescued_data IS NOT NULL` | A small, stable number. A sudden rise means the source changed its layout. |
| Every row has audit values | `SELECT COUNT(*) FROM <table> WHERE source_file_name IS NULL OR file_date IS NULL OR ingestion_timestamp IS NULL` | 0 |
| No file loaded twice | `SELECT source_file_name, COUNT(DISTINCT ingestion_timestamp) FROM <table> GROUP BY 1 HAVING COUNT(DISTINCT ingestion_timestamp) > 1` | No rows |

### 4.9 Design notes

| Topic | Decision |
|---|---|
| Table names | `skypoints.bronze.raw_member` and `skypoints.bronze.raw_redemptions` are the final names. |
| Header record | `H` records are filtered out. The filter also removes any line whose record type is not exactly `D`, such as a lower-case `d`, so check the row count against the file (4.8). |
| `tier` column name | Source header is `Tier_Code`. Bronze column is `tier`. Silver renames it to `tier_code`. |
| Clustering | None. Bronze is append only, so files are already ordered by arrival. |
| Table hardening | Optional: set `delta.appendOnly = true` so no job can update or delete bronze rows, and enable optimized writes and auto compaction. |

**Operational notes**

- **Replay.** Stop the job, drop or truncate the table, delete its checkpoint folder, and run the notebook again. Landing is never touched. A silver job reading that bronze table must also have its checkpoint cleared.
- **Late or corrected files.** A file with a new name is picked up on the next run. A file re-delivered under the same name is **not** loaded again, so the source must send corrections under a new file name.
- **Schema changes.** The schema is explicit, so a new source field never breaks the load. It appears in `_rescued_data` until the schema and table are updated and the field is back-filled from the rescued JSON.

## 5. Silver Layer: Member

### 5.1 Incremental processing

The silver job reads `bronze.raw_member` as a stream with `trigger(availableNow=True)` and hands each micro-batch to `load_member_batch_to_silver` through `foreachBatch`. It processes everything that arrived since the last run, then stops. The checkpoint is `/Volumes/skypoints/silver/members/member_checkpoints`.

- The checkpoint replaces a manual watermark. It advances only after the batch function finishes successfully.
- A failed run re-reads the same bronze data on retry, so every write must be safe to repeat (6.4).
- A late file gets a later bronze `ingestion_timestamp`, so it is picked up in the next run.
- Delta Change Data Feed is not needed, because bronze is append only.

**Per micro-batch:**

1. Trim and rename the bronze columns. Blank becomes `NULL`.
2. Convert text to `DATE` / `INT` and mark known country codes.
3. Validate. Each failed rule adds a reject code. The batch is persisted once and counted.
4. Append rejected rows to `member_rejects`. The write carries `txnAppId` and `txnVersion = batch_id`, so Delta skips it if the same batch was already written. A rerun creates no duplicate rejects.
5. For the valid rows: add `age` and `stale_member`, keep the latest record per member.
6. Split into the country tables and update the lookup (section 6).
7. Print the batch log (6.8).

### 5.2 Column-level transformations

Bronze stores every column as `STRING`. The table below defines, for each silver column, the type, whether it is mandatory, what happens when it is blank, and the rules applied. The schema is identical in every country table. All `DATE` columns are stored as `YYYY-MM-DD`.

**Two kinds of failure apply to every column.** A **blank** optional column stays `NULL`. A **populated but invalid** value (wrong type, longer than the maximum length, wrong format) rejects the whole row, whether the column is mandatory or not. A `NULL` never fails a length, format or tier rule.

| Column | Type | Mandatory | If null or blank | Validation | Issue seen and handling |
|---|---|---|---|---|---|
| `member_name` | `STRING` (max 255) | Yes | Reject `MISSING` | Trim. `TOO_LONG` above 255. `INVALID_FORMAT` unless letters and spaces only. | The spec marks it as the key column, so it stays the key. |
| `member_id` | `STRING` (max 18) | Yes | Reject `MISSING` | Trim, no padding. `TOO_LONG` above 18. `INVALID_FORMAT` unless digits only. | Lengths vary (`22345`, `2256` versus 6 digits). All are valid up to 18 digits and kept as they are. |
| `enrollment_date` | `DATE` | Yes | Reject `MISSING` | Tolerant parser (5.3). `INVALID_DATE` if the text does not parse. | `20211301` and `Sep 11 2019` are rejected. |
| `last_flight_date` | `DATE` | No | `NULL` | Tolerant parser. `INVALID_DATE` if populated and unparseable. | |
| `tier_code` | `STRING` (max 5) | No | `NULL` | Trim. `TOO_LONG` above 5. `INVALID_VALUE` unless one of `GLD`, `PLT`, `SLV` (case-sensitive). | `gld`, `XYZ`, `hmd` are rejected. |
| `agent_name` | `STRING` (max 255) | No | `NULL` | Trim. `TOO_LONG` above 255. `INVALID_FORMAT` unless letters and spaces only. | A missing agent shifts later fields. The shift is caught by the country check. |
| `state` | `STRING` (max 5) | No | `NULL` | Trim. `TOO_LONG` above 5. `INVALID_FORMAT` unless letters and spaces only. | See open item 12.2. |
| `country_code` | `STRING` (max 5) | Yes | Reject `MISSING` | Trim. `TOO_LONG` above 5. `NOT_IN_REFERENCE` unless it matches `silver.country` exactly, case-sensitive. | The spec marks it optional, but routing needs it, so it is mandatory. The staging column `County` is `Country`. |
| `post_code` | `INT` | No | `NULL` | Digits only, then `try_cast` to `INT`. `INVALID_INT` if populated and it does not convert (including overflow). | Treated as nullable until confirmed with the source team. |
| `dob` | `DATE` | No | `NULL` | Left-pad 1 to 8 digit values to 8, then tolerant parser. `INVALID_DATE` if populated and unparseable. | The leading zero is lost (`3051985`). Padding restores it. |
| `is_active` | `STRING` (max 1) | No | `NULL` | Trim. `TOO_LONG` above 1. | |
| `age` | `INT` | No | `NULL` when `dob` is `NULL` | **Derived.** `floor(months_between(file_date, dob) / 12)` | |
| `stale_member` | `BOOLEAN` | No | `NULL` when `last_flight_date` is `NULL` | **Derived.** `true` when `datediff(file_date, last_flight_date) > 90` | |
| `file_date` | `DATE` | Yes | Reject `MISSING` | Carried from bronze. Drives latest record wins. | |
| `ingestion_timestamp` | `TIMESTAMP` | Yes | Reject `MISSING` | Carried from bronze. Tie-breaker. | |
| `source_file_name` | `STRING` | Yes | Reject `MISSING` | Carried from bronze. Audit. | |

`age` and `stale_member` use `file_date`, not `current_date()`, so reruns give identical results.

### 5.3 Date parsing

All member dates (`enrollment_date`, `last_flight_date`, `dob`) go through one tolerant parser:

1. The text must be exactly 8 digits. Anything else (`Sep 11 2019`, `2019-11-18`) fails.
2. The formats `ddMMyyyy`, `yyyyMMdd` and `MMddyyyy` are tried in that order. The first that gives a real date wins.
3. A date is kept only if its year is between 1900 and the current year.
4. The result is a `DATE`, stored as `YYYY-MM-DD`.

**Known limits.** A value valid in two formats, such as `03051985` (3 May or 5 March), is read as the first format that parses, `ddMMyyyy`. The upper year bound uses `current_date()`, so it moves with the run date. It only affects future-dated values, but it is the one place where a rerun years later could differ.

### 5.4 Reject codes

| Code | Meaning |
|---|---|
| `column:MISSING` | A mandatory value is `NULL` or blank |
| `column:TOO_LONG` | Longer than the maximum length for the column |
| `column:INVALID_DATE` | Text was filled in but is not a valid date |
| `column:INVALID_INT` | Text was filled in but is not a valid integer |
| `column:INVALID_FORMAT` | Fails the letters-only or digits-only pattern |
| `column:INVALID_VALUE` | Not in the allowed list (`tier_code`) |
| `country_code:NOT_IN_REFERENCE` | Not in the fixed country list |

A row with several problems appears once, with all its codes joined by `"; "`.

### 5.5 Latest record wins

One row per `member_name` is kept. The newest row is chosen by, in order:

1. `file_date`
2. `ingestion_timestamp`
3. `last_flight_date` (a `NULL` loses)
4. every remaining column, left to right, so that a full tie still gives the same answer on every run

This resolves repeats within a file, across files, and a member who reappears with a new country. De-duplication runs only on the micro-batch, never over the full history.

### 5.6 Country reference table

`skypoints.silver.country` is a fixed list with one row per country. It is loaded from `data/country_reference.csv` by `setup_country_table`. It is set by us and is not built from incoming data.

| Column | Data type | Notes |
|---|---|---|
| `country_code` | `STRING` (max 5) | **Key.** For example `IND`, `USA`, `CAN`. The incoming code must match it exactly. |
| `country_name` | `STRING` | |
| `target_table` | `STRING` | Optional. For example `silver.member_india`. When absent, the table is `skypoints.silver.member_<code in lower case>`. Names are checked against a safe pattern, because they are placed into SQL text. |

A record whose country code does not match goes to `member_rejects`. No country table is created for it.

### 5.7 Reject table

`skypoints.silver.member_rejects` holds every failed member row.

| Column | Type | Notes |
|---|---|---|
| `member_name` | `STRING` | Trimmed raw value, can be `NULL` |
| `member_id` | `STRING` | Trimmed raw value, can be `NULL` |
| `reject_reason` | `STRING` | One or more `column:CHECK` codes joined with `"; "` |
| `_rescued_data` | `STRING` | The full raw bronze row as JSON |
| `source_file_name` | `STRING` | |
| `file_date` | `DATE` | `YYYY-MM-DD` |
| `ingestion_timestamp` | `TIMESTAMP` | Bronze load time of the rejected row |
| `rejected_at` | `TIMESTAMP` | When silver rejected the row |

## 6. Country Split (Member Table)

This is the hardest part of the member design, because a member who moves country must leave the old country table and appear in the new one.

### 6.1 Supporting table

`skypoints.silver.member_country_lookup`, clustered by `member_name`.

| Column | Meaning |
|---|---|
| `member_name` | One row per member (the key) |
| `country_code` | The country the member currently lives in |
| `file_date` | Feed date of the record that set this value. Stops old files overwriting newer data. |

It is small (one row per member) and answers one question: **where does this member live right now?** Section 8 adds `member_id` so redemptions can use it.

### 6.2 Steps

Input is the latest record per member for the batch (5.5).

1. **Compare with the lookup**, before touching it. Left-join the batch to the lookup on `member_name`. Keep rows that are new, or whose `file_date` is not older than the lookup's. Rows older than the lookup are skipped. Persist this set once.
2. **Find movers**: rows whose stored country differs from the incoming country.
3. **Upsert** every kept row into its (new) country table, creating the table if it is missing. This also covers new members.
4. **Delete movers** from their old country table.
5. **Update the lookup last.**

### 6.3 Why `member_country_lookup` goes last

It is the bookmark used to detect movers. If it were updated first and the job then crashed, the rerun would see no country change, and the old country table would keep a stale row forever. Updating it last means a crash leaves it at the old value, so the rerun finds the same movers and finishes the job.

### 6.4 No multi-table transactions in Delta

One `MERGE` cannot touch two tables, so each step is safe to rerun:

- Upsert gives the same rows when repeated.
- Deleting an already-deleted row does nothing.
- The lookup is unchanged until the end, so movers are recomputed identically on retry.

### 6.5 How each case is handled

| Case | In lookup? | Country table | Lookup |
|---|---|---|---|
| New member | No | Insert into their country table | Insert |
| Existing, same country | Yes, same | Update in place | Update |
| Existing, moved | Yes, different | Upsert into new, delete from old | Update |
| Row older than the lookup | Yes, newer file_date | Not applied | Not changed |

### 6.6 MERGE shapes

The table being changed is the **target** and the batch is the **source**. The country comparison is never in the `ON` clause, or unchanged members count as not matched and are inserted twice. The change check sits in `WHEN MATCHED AND ...`.

```sql
-- upsert into the new country table, and update the lookup (same shape)
MERGE INTO <country table> t
USING <batch> s
ON t.member_name = s.member_name
WHEN MATCHED AND s.file_date >= t.file_date THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- delete a mover from the old country table (never delete a newer row)
MERGE INTO <old country table> t
USING <movers> s
ON t.member_name = s.member_name
WHEN MATCHED AND t.file_date <= s.file_date THEN DELETE
```

The batch is exposed to SQL as a uniquely named temporary view, dropped after the merge.

### 6.7 Parallel merges

Each country is its own `MERGE`, so countries run in a thread pool. One worker per country with work, up to `MAX_PARALLEL_MERGES` (8, a starting value to tune from the logged merge time). The largest countries are submitted first, so one big country does not wait behind many small ones. The first error fails the batch, and the batch is retried.

### 6.8 Batch log

Each batch prints: rows in, rejected, superseded duplicates, older than the lookup (skipped), applied, movers, country tables written, workers, merge seconds. `in = rejected + superseded + skipped + applied` must hold.

### 6.9 Reconciliation check

`reconcile_lookup_with_country_tables` returns one row per country: members the lookup places there against rows in the country table. `difference` must be `0` everywhere. A non-zero value means a failed run left a mover in two tables or a member in none, and rerunning the batch repairs it. Run it after the job.

If a history of country moves is needed, append movers to a `member_country_history` table (SCD2 style). The lookup alone is SCD1.

## 7. Silver Layer: Redemption Transactions

### 7.1 Goals and design decisions

Bronze holds one row per member per feed, with a nested array of transactions. Silver turns that into **one row per redemption transaction**, typed, validated and queryable, which is deliverable 4 of the assignment.

| Decision | Choice | Reason |
|---|---|---|
| Number of tables | **One** target table, plus one reject table | The relationship is one member to many transactions, so `member_id` is simply a column. A separate key table plus a details table would double the merges and add a join to every query. It would also copy bronze, which already holds the per-feed history. |
| Slowly changing type | **SCD1**: overwrite with the latest status | The business question is the current status. Bronze keeps every version received, so a history can be rebuilt later. |
| Key | **(`member_id`, `txn_id`)** | Safe whether `txn_id` is unique globally or only per member. It can be reduced to `txn_id` once the source team confirms global uniqueness (12.4). |
| "Latest" means | Newest `feed_date`, then `file_date`, then `ingestion_timestamp`, then position in the array | `feed_date` is what the source states about the record, so a stale record re-sent in a newer file cannot overwrite newer data. |
| Granularity of rejects | One **transaction**, not the whole member row | One bad transaction must not discard that member's good ones. |
| Missing member | **Not** a reject | The two feeds arrive independently, so a redemption can land before its member. It is reported at join time (section 8). |

| Object | Name |
|---|---|
| Target | `skypoints.silver.redemption_transaction` |
| Rejects | `skypoints.silver.redemption_rejects` |
| Checkpoint | `/Volumes/skypoints/silver/redemptions/redemption_checkpoints` |
| Code | `pipeline_utils/silver/clean_and_validate_redemption.py`, `process_redemption_batch.py` |
| Notebook | `src/silver/silver_create_redemption_transaction.ipynb` |

### 7.2 Processing flow per micro-batch

The job reads `bronze.raw_redemptions` as a stream with `trigger(availableNow=True)` and uses `foreachBatch`, the same pattern as the member job.

1. **Resolve `member_id`.** Use `member_id`. If it is blank, take `memberId` from the bronze `_rescued_data` JSON. Record the source in `member_id_recovered` (`true` when recovered).
2. **Separate rows with no array.** A bronze row whose `redemptions` is `NULL` (not an empty list) is written to the rejects as `redemptions:MISSING`, so a renamed array key is never lost silently. A row with an empty list `[]` is valid and simply produces no transactions. Both are counted in the batch log.
3. **Explode** the array, keeping the position of each transaction in it. One output row per transaction.
4. **Trim** all text, blank becomes `NULL`. Upper-case `status`.
5. **Convert** `txn_date` and `feed_date` to `DATE` (`YYYY-MM-DD`) and `miles_redeemed` to `INT`.
6. **Validate** (7.4). Failed rows get reason codes. The batch is persisted once and counted.
7. **Append rejects** to `redemption_rejects`, with `txnAppId` and `txnVersion = batch_id`, so a rerun creates no duplicates.
8. **Keep the latest record per key** within the batch (7.5).
9. **MERGE** into `redemption_transaction` (7.5).
10. **Print the batch log** (7.8).

### 7.3 Target table: `silver.redemption_transaction`

Clustered by (`member_id`, `txn_id`).

| Column | Type | Mandatory | Source and transformation |
|---|---|---|---|
| `member_id` | `STRING` (max 18) | Yes | Trimmed, kept as text, no casting, so leading zeros survive. Recovered from `memberId` when needed. **Key part 1.** |
| `txn_id` | `STRING` (max 30) | Yes | Trimmed. **Key part 2.** |
| `txn_date` | `DATE` | Yes | Parsed from `yyyyMMdd` or `yyyy-MM-dd`. Stored as `YYYY-MM-DD`. |
| `partner` | `STRING` (max 100) | Yes | Trimmed |
| `miles_redeemed` | `INT` | Yes | Positive whole number |
| `status` | `STRING` (max 20) | Yes | Trimmed and upper-cased, one of the allowed values |
| `feed_date` | `DATE` | Yes | Date stated by the source inside the record. Stored as `YYYY-MM-DD`. Decides "latest wins". |
| `member_id_recovered` | `BOOLEAN` | Yes | `true` when `member_id` came from `memberId` in the rescued data |
| `file_date` | `DATE` | Yes | From bronze. Tie-breaker. |
| `ingestion_timestamp` | `TIMESTAMP` | Yes | From bronze. Tie-breaker. |
| `source_file_name` | `STRING` | Yes | From bronze. Lineage. |

Fields such as `fee_usd`, `channel` and `route` stay in bronze's `_rescued_data`. They are not part of the assignment, and a later schema update can promote them.

### 7.4 Validation rules

All rules run at transaction level. A `NULL` never fails a length, format, range or list rule, because the mandatory rule already reports it.

| Rule | Column | Check | Code |
|---|---|---|---|
| Mandatory | `member_id`, `txn_id`, `txn_date`, `partner`, `miles_redeemed`, `status`, `feed_date`, `file_date`, `ingestion_timestamp`, `source_file_name` | Not `NULL` after trimming. Dates are judged on the text, because the converted date is also `NULL` when invalid. | `column:MISSING` |
| Length | `member_id` 18, `txn_id` 30, `partner` 100, `status` 20 | Not longer than the maximum | `column:TOO_LONG` |
| Format | `member_id` | Digits only | `member_id:INVALID_FORMAT` |
| Format | `txn_id` | Letters, digits, `-` and `_` only | `txn_id:INVALID_FORMAT` |
| Date | `txn_date`, `feed_date` | Parses as `yyyyMMdd` or `yyyy-MM-dd`, and is not before 2000-01-01 | `column:INVALID_DATE` |
| Cross-field | `txn_date` | Not after `feed_date` (checked only when both dates are valid) | `txn_date:AFTER_FEED_DATE` |
| Cross-field | `feed_date` | Not after `file_date` (checked only when both dates are valid) | `feed_date:AFTER_FILE_DATE` |
| Integer | `miles_redeemed` | Text that is filled in must convert to `INT` | `miles_redeemed:INVALID_INT` |
| Range | `miles_redeemed` | Greater than 0 | `miles_redeemed:NOT_POSITIVE` |
| Allowed list | `status` | One of `COMPLETED`, `PENDING`, `CANCELLED`, `REVERSED`, `ON_HOLD` (configurable) | `status:INVALID_VALUE` |
| Array present | `redemptions` | Not `NULL` for the bronze row | `redemptions:MISSING` |

The date checks use `file_date` and a fixed lower bound, never `current_date()`, so a rerun gives the same result.

**Deliberately not checked in the batch:** that the member exists (section 8), and uniqueness of the key, which is enforced by de-duplication and the merge rather than by rejection.

### 7.5 Latest record wins and the merge

**Within the batch.** One row per (`member_id`, `txn_id`) is kept, the one with the largest of: `feed_date`, `file_date`, `ingestion_timestamp`, position in the array, then all remaining columns. The array position means that when the same transaction appears twice in one row (for example `RX13407`), the later entry is treated as the newer statement. This is the same struct-ordering technique as the member job (5.5).

**Into the table.**

```sql
MERGE INTO redemption_transaction t
USING <deduplicated batch> s
ON  t.member_id = s.member_id AND t.txn_id = s.txn_id
WHEN MATCHED AND struct(s.feed_date, s.file_date, s.ingestion_timestamp)
               > struct(t.feed_date, t.file_date, t.ingestion_timestamp) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

- **A new transaction** is inserted.
- **A newer version** overwrites the row (`PENDING` becomes `COMPLETED`).
- **An older version** arriving late changes nothing. A stale file cannot revert `COMPLETED` to `PENDING`.
- **A rerun** of the same batch changes nothing, because the comparison is strictly greater and the stored row already carries the batch's values. This makes the merge idempotent without extra bookkeeping.

**Why unchanged rows are still updated.** It is tempting to skip rows whose status and miles did not change. That is wrong. Suppose `RX1` is `PENDING` at `feed_date` 10 Jan, a 15 Jan feed repeats it unchanged, and a late 12 Jan file then says `COMPLETED`. If the 15 Jan repeat had been skipped, the stored date would still be 10 Jan and the stale 12 Jan record would overwrite the newer state. Updating the date on every newer record keeps the guard correct. The cost is a rewrite per repeated transaction, which clustering keeps small.

### 7.6 Reject table: `silver.redemption_rejects`

Failed values are kept as trimmed **text**, because an invalid value cannot be held in its real type.

| Column | Type | Notes |
|---|---|---|
| `member_id` | `STRING` | After recovery. Can be `NULL`. |
| `txn_id` | `STRING` | Can be `NULL` |
| `txn_date` | `STRING` | Raw text, so a bad date stays visible |
| `partner` | `STRING` | |
| `miles_redeemed` | `STRING` | Raw text, for example `-500` or `abc` |
| `status` | `STRING` | |
| `feed_date` | `STRING` | Raw text |
| `reject_reason` | `STRING` | `column:CHECK` codes joined with `"; "` |
| `_rescued_data` | `STRING` | JSON of the raw record: the member key, the feed date, the single raw transaction, and the bronze `_rescued_data`. The sibling transactions are not copied, so the record stays small and self-contained. |
| `source_file_name` | `STRING` | |
| `file_date` | `DATE` | `YYYY-MM-DD` |
| `ingestion_timestamp` | `TIMESTAMP` | Bronze load time |
| `rejected_at` | `TIMESTAMP` | When silver rejected it |

### 7.7 Worked example

The three bronze rows from 4.6.2 become:

**`redemption_transaction`**

| member_id | txn_id | txn_date | partner | miles_redeemed | status | feed_date | member_id_recovered |
|---|---|---|---|---|---|---|---|
| 223457 | RX10091 | 2024-01-10 | AeroLink | 12000 | COMPLETED | 2024-01-15 | false |
| 223457 | RX10092 | 2024-01-13 | SkyPoints | 5000 | PENDING | 2024-01-15 | false |
| 224144 | RX13438 | 2024-01-11 | NorthStar | 22000 | COMPLETED | 2024-01-15 | false |
| 226120 | RX13445 | 2024-01-08 | NorthStar | 55000 | CANCELLED | 2024-01-15 | true |

`fee_usd`, `channel` and `source_system` remain in bronze. The fourth row shows the `memberId` recovery.

**Later feeds acting on `RX10092`**

| Event | Stored row after | Why |
|---|---|---|
| Feed dated 2024-01-20 sends `RX10092` as `COMPLETED` | `COMPLETED`, `feed_date` 2024-01-20 | Newer feed wins |
| A late file with `feed_date` 2024-01-12 then sends it as `PENDING` | unchanged | Older than the stored row |
| The 2024-01-20 batch is run again | unchanged | Not strictly greater |

**`redemption_rejects`.** A transaction with miles `-500` and status `DONE` is written with `reject_reason = miles_redeemed:NOT_POSITIVE; status:INVALID_VALUE` and never reaches the target.

### 7.8 Counts, batch log and checks

Each batch prints: bronze rows in, rows with no array (rejected), rows with an empty array, transactions exploded, rejected, superseded duplicates in the batch, inserted, updated, older than stored (skipped). Inserted and updated come from the merge's operation metrics, and `skipped = deduplicated − inserted − updated`.

The identity to hold for every batch: `transactions exploded = rejected + superseded + skipped + inserted + updated`.

Post-load checks (also in section 9):

- `SUM(size(redemptions))` over the batch's bronze rows equals the exploded count.
- No (`member_id`, `txn_id`) appears twice in `redemption_transaction`. Expected: 0.
- Rejects grouped by `reject_reason`, to spot a sudden new failure type.

### 7.9 Not built in the first version

| Item | Reason |
|---|---|
| Status history table (`txn_id`, `status`, `feed_date`) | Only needed if the business asks when a status changed. It would be append-only and sits beside the main table. Bronze already holds every version, so it can be rebuilt. |
| Member-level summary (total miles, count, last redemption date) | A gold table, built from this one. |
| `fee_usd`, `channel`, `route` columns | Not in the assignment. Promote from `_rescued_data` when requested. |

## 8. Joining Redemptions to Members

### 8.1 The join

Redemptions carry `member_id` and no name. The member tables are keyed on `member_name`, and the lookup holds the current country per member. To join, the lookup needs `member_id`.

**Change to the member job (to be built).** Add `member_id` to `silver.member_country_lookup`, written in the same lookup merge (6.6), and back-fill it once, either from the country tables or by replaying member silver from bronze. The table stays small, one row per member.

| Step | Join | Gives |
|---|---|---|
| 1 | `redemption_transaction.member_id` = `member_country_lookup.member_id` | Current country of the member |
| 2 (optional) | country table (`member_<code>`) on `member_name` | Tier, active flag, age, stale flag and the rest of the profile |

A read-only view or a gold table presents the result, for example `redemptions with country`. The join is done at read time, not in silver, because the feeds arrive independently.

### 8.2 Orphans and the uniqueness caveat

- **Orphans.** A redemption whose `member_id` is not in the lookup stays in the table. A data-quality check reports their count and share per day. A rising share means the member feed is late or a key is wrong. A redemption that arrives before its member joins correctly on the next read after the member lands.
- **`member_id` is not yet unique in the lookup.** The member key is `member_name`, and uniqueness of `member_id` is not enforced (open item 12.1). Until it is, one `member_id` can map to two lookup rows and fan the join out. A check counts `member_id` values that map to more than one `member_name`.
- **Late-arriving member.** Redemptions are never rewritten because of a member change. A move of country changes only the lookup, so the next read shows the new country.

## 9. Validation and Data-Quality Matrix

This answers deliverable 5. "Reject" sends the row to the feed's reject table.

| Category | Check | Member feed | Redemption feed |
|---|---|---|---|
| Mandatory fields | Not `NULL` or blank | Reject `MISSING` | Reject `MISSING` |
| Key uniqueness | One row per key | `member_name`: latest record wins (5.5) | (`member_id`, `txn_id`): latest wins and merge (7.5) |
| ID uniqueness | An ID belongs to one key | `member_id` per `member_name`: **not implemented** (12.1) | `txn_id` under more than one member: report (12.4) |
| Data type | Converts to its type | `INVALID_DATE`, `INVALID_INT` | `INVALID_DATE`, `INVALID_INT` |
| Length | Within the maximum | `TOO_LONG` | `TOO_LONG` |
| Format | Letters-only or digits-only | `INVALID_FORMAT` | `INVALID_FORMAT` |
| Allowed values | In a fixed list | `tier_code` | `status` |
| Reference data | Exists in a reference table | `country_code` in `silver.country` | n/a |
| Range | Sensible number or date | Year 1900 to current year | `miles_redeemed` > 0, date not before 2000-01-01 |
| Cross-field | Fields agree | n/a | `txn_date` ≤ `feed_date` ≤ `file_date` |
| Shifted or truncated rows | Caught indirectly | Country not in reference | Missing array reported |
| Referential | Member exists | n/a | Orphan report at join time, not a reject |
| Completeness | Nothing lost between layers | Batch identity (6.8) | Batch identity (7.8) |
| Schema drift | New or renamed source fields | `_rescued_data` count (4.8) | `_rescued_data` count, `memberId` recovery rate |
| Reconciliation | Targets agree | Lookup against country tables (6.9) | Silver transaction count against bronze nested count |

Each reject table is also a monitoring source: group by `reject_reason` after every run.

## 10. Performance and Scale

| Table | Cluster by |
|---|---|
| Bronze (both tables) | Nothing |
| `member_country_lookup` | `member_name` (add `member_id` as a second key once it is a join column) |
| Country tables | `member_name` (never `country_code`, since each table holds one value) |
| `redemption_transaction` | (`member_id`, `txn_id`) |
| Reject tables | Nothing, append only |

- Use **liquid clustering** rather than partitioning. `member_name` and `member_id` are high cardinality, and partitioning on them would create millions of tiny folders.
- Delta has no traditional indexes. Speed comes from **data skipping** on min/max statistics, an optional **bloom filter**, and **deletion vectors** for cheap deletes.
- Enable **optimized writes and auto compaction** to avoid small files.
- De-duplicate only the incremental batch, then MERGE. Never window over the full history.
- The redemption explode is narrow and linear in the number of transactions. The merge key prefix is `member_id`, so the join to members prunes on the same column.
- The per-country merge loop runs once per country, not once per record, and is parallelised (6.7).
- `member_country_lookup` is millions of rows, not billions, so the lookup join is cheap.

## 11. Build Order and Status

| # | Step | Status |
|---|---|---|
| 1 | Landing folders for both feeds, file naming agreed | Done |
| 2 | Bronze tables and Auto Loader for both feeds | Done |
| 3 | `silver.country` from `country_reference.csv` | Done |
| 4 | Silver member: types, nulls, rejects, latest wins, derived columns | Done |
| 5 | `member_country_lookup` | Done |
| 6 | Country split: dedupe, movers, upsert, delete, lookup last, parallel merges | Done |
| 7 | Reconciliation check for lookup against country tables | Done |
| 8 | Update this design with the redemption silver layer | This document |
| 9 | `redemption_transaction` and `redemption_rejects` DDL, clean and validate module, batch module, notebook | Next |
| 10 | Unit tests for both silver jobs (rules, dedupe, merge guard, rerun) | Next |
| 11 | Add `member_id` to `member_country_lookup`, back-fill it | Next |
| 12 | Join view and data-quality checks (orphans, duplicates, counts) | Next |
| 13 | Close the open items in section 12 | Next |

Make small incremental commits at each step so the evolution is visible in the history.

## 12. Architect's Note: Are Separate Tables the Best Design?

**Member data.** For billions of records, physically separate country tables are not an ideal default:

- A country move touches two tables, so there is no atomicity.
- Skew: a few countries hold most rows while many hold very little, giving tiny tables and tiny files.
- Every schema change or maintenance policy is multiplied across hundreds of tables.
- Dirty country codes risk creating junk tables.

**Recommended production alternative:** one silver `members` table with liquid clustering on (`country_code`, `member_name`), and a **view per country**. A move becomes one atomic MERGE that updates `country_code`. Per-country access can use Unity Catalog row filters.

**Separate tables are justified when** there are data residency rules, different retention per country, or isolated consumers per country. This design builds separate country tables as the assignment requires, and records the alternative for future consideration.

**Redemption data.** The same question has a clear answer in the other direction: redemptions are **not** split by country. A single table, clustered by member, needs no multi-table coordination, and country is a join away through the lookup. Keeping the redemption table single is also what makes the SCD1 merge a single atomic operation, in contrast to the member country move.