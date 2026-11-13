# CS 236 Course Project – Phase 2 Report: Database Population

**Scope.** This report covers Section 2.2 (Database Population) of Phase 2. It describes the
PostgreSQL database design and the process used to load the three processed datasets with Spark
and verify them. The Spark analyses (Section 2.1) are documented separately.

The code is in `Database-Population/`, and the numbers below come from `./run_all.sh` (full log
in `output/run_log.txt`).

---

## 1. Setup

| Component | Version / setting |
|---|---|
| PostgreSQL | `postgres:16` Docker image, container `cs236-postgres` |
| Database / user / password | `cs236` / `cs236` / `cs236` (override with `PG_*` env vars) |
| Port | 5432 on the host |
| Data persistence | Docker volume `cs236-pgdata` |
| Spark | PySpark 3.5.1, local mode, all cores |
| JDBC driver | `org.postgresql:postgresql:42.7.3` (fetched by Spark via `spark.jars.packages`) |

`start_postgres.sh` wraps the `docker run` command from the project spec:

```bash
docker run -d --name cs236-postgres \
  -e POSTGRES_USER=cs236 -e POSTGRES_PASSWORD=cs236 -e POSTGRES_DB=cs236 \
  -p 5432:5432 -v cs236-pgdata:/var/lib/postgresql/data postgres:16
```

It also restarts the container if it already exists and waits until the server is ready.

## 2. Database design

The database has three tables, one per processed dataset. The full DDL is in
`sql/create_tables.sql`.

| Table | Rows | Columns | Primary key | Other unique | Size on disk |
|---|---|---|---|---|---|
| `nyc_clean` | 799,739 | 59 | `unique_key` | `request_id` | 764 MB |
| `la_clean` | 800,000 | 49 | `srnumber` | `request_id` | 412 MB |
| `unified` | 1,599,739 | 32 | `request_id` | (`source`, `source_id`) | 673 MB |

### 2.1 Column names

The table columns have **exactly the same names** as the Parquet datasets from Phase 1. This
keeps the database consistent with the Spark data, and Spark can insert by column name with no
renaming step. `verify_db.py` checks that the columns match with none missing and none extra.

### 2.2 Data types

| Spark type | PostgreSQL type | Used for | Reason |
|---|---|---|---|
| string (identifier / code) | `VARCHAR(n)` | IDs, `status`, `channel`, `source`, ZIP, agency code, borough | Short, bounded values. *n* is set above the longest value found in the data (e.g. longest `request_id` is 15 → `VARCHAR(24)`), so the limit doubles as a sanity check |
| string (free text) | `TEXT` | addresses, descriptions, request types, names | No natural maximum length (e.g. `resolution_description` is up to 915 characters). In PostgreSQL, `TEXT` is as fast as `VARCHAR` |
| string ZIP | `VARCHAR(5)` + `CHECK (~ '^[0-9]{5}$')` | `zipcode`, `incident_zip` | A ZIP is a code, not a number: leading zeros matter and math on it makes no sense |
| timestamp | `TIMESTAMP` (without time zone) | `created_at`, `closed_at`, `updated_at`, … | The source data has no time zone; values are each city's local time, stored as recorded. Using `TIMESTAMPTZ` would wrongly attach one zone to both cities |
| date | `DATE` | `created_date` | Calendar day of creation |
| double | `DOUBLE PRECISION` | `latitude`, `longitude`, `resolution_hours` | Same 64-bit float as Spark, so values come back unchanged |
| int (date parts) | `SMALLINT` | `created_year/month/day/hour` | Small ranges; 2 bytes instead of 4 |
| int (district IDs) | `INTEGER` | `council_district`, LA `cd`, `nc` | |
| bigint | `BIGINT` | NYC state-plane x/y coordinates | |
| boolean | `BOOLEAN` | flags (`is_valid_resolution`, LA `anonymous`, …) | |

### 2.3 Keys and uniqueness

- **`nyc_clean.unique_key`** and **`la_clean.srnumber`** are primary keys. They are the source
  systems' own IDs, and Phase 1 guarantees one row each.
- **`request_id`** is `UNIQUE` in both city tables and the **primary key** of `unified`. This is
  the cross-city ID from Phase 1 (`NYC-…` / `LA-…`), so a row can be traced from `unified` back
  to its city table.
- **`UNIQUE (source, source_id)`** on `unified` stops the same source record from being inserted
  twice under a different `request_id`.

IDs are stored as `VARCHAR`, not numbers. NYC keys are numeric, but LA keys look like
`1-4601720641`, and `unified.source_id` must hold both.

### 2.4 Nullability

The rule: **NOT NULL wherever the pipeline guarantees a value**, nullable wherever the source
data can be missing.

| NOT NULL | Nullable |
|---|---|
| IDs (`unique_key`, `srnumber`, `request_id`, `source_id`), `source`, `city` | `closed_at` (open requests, or nulled because it was invalid) |
| `created_at` (rows without it were dropped in Phase 1) | `zipcode`, `latitude`, `longitude` (missing or nulled as invalid) |
| `request_type` / `complaint_type` / `requesttype`, `agency` / `owner` | `resolution_hours` (only for valid resolutions) |
| standardized `status`, `channel` | `request_subtype`, `agency_name` (NYC only, so null for LA rows) |
| `closed_before_created`, `is_valid_resolution` | other optional source attributes |
| `created_date`, `created_year/month/day/hour/dow` | |

The NOT NULL columns were confirmed to have zero nulls in the Parquet data before they were
declared (18 in `nyc_clean`, 17 in `la_clean`, 17 in `unified`).

### 2.5 CHECK constraints

The database enforces the same rules as Phase 1's `validate.py`. Data added later, for example
through the Phase 3 WebUI, then can't break them.

| Constraint | Tables |
|---|---|
| `status IN ('OPEN','IN_PROGRESS','PENDING','CLOSED','CANCELLED','REFERRED','UNKNOWN')` | all |
| `channel IN ('PHONE','ONLINE','MOBILE','OTHER','UNKNOWN')` | all |
| `source` = the table's city (`'NYC311'` / `'LA311'`), or one of the two in `unified` | all |
| ZIP is 5 digits | all |
| `closed_at IS NULL OR closed_at >= created_at` | all |
| `resolution_hours >= 0`, and `is_valid_resolution = (resolution_hours IS NOT NULL)` | all |
| `created_month` 1–12, `created_day` 1–31, `created_hour` 0–23 | all |
| latitude / longitude inside the city's area (the same box as Phase 1); valid world range in `unified` | all |

These were tested by inserting bad rows into `unified`. PostgreSQL rejected each one:

| Bad row | Error |
|---|---|
| duplicate `request_id` | `violates unique constraint "unified_pkey"` |
| `status = 'DONE'` | `violates check constraint "unified_status_check"` |
| `zipcode = 'ABCDE'` | `violates check constraint "unified_zipcode_check"` |
| `closed_at` one day before `created_at` | `violates check constraint "unified_check"` |
| missing `created_at` | `violates not-null constraint` |

### 2.6 Indexes

Primary keys and UNIQUE constraints come with their own indexes. `sql/create_indexes.sql` adds
indexes for the columns that Phase 2 analyses and the Phase 3 WebUI filter or group on:

- `unified`: (`source`, `created_at`), (`source`, `zipcode`), (`source`, `request_type`), (`status`)
- `nyc_clean`: (`created_at`), (`incident_zip`); `la_clean`: (`created_at`), (`zipcode`)

They are created **after** the bulk load, which is faster than updating them for every inserted
row. `ANALYZE` then refreshes the query planner's statistics.

## 3. Population process

`load_to_postgres.py` runs three steps:

1. **Create tables.** Runs `sql/create_tables.sql` over one JDBC connection inside a
   transaction: it drops and recreates the three tables. Every run starts clean, so re-running
   never fails on duplicate keys.
2. **Load with Spark.** For each table, Spark reads the Parquet dataset and writes it with
   `DataFrame.write.jdbc(..., mode="append")`.
   - **Append into the pre-created tables**, not `mode="overwrite"`. If Spark created the tables
     itself, every string would become `TEXT` and there would be no keys, NOT NULL or CHECK
     constraints. Appending keeps our design.
   - **8 partitions** gives 8 parallel database connections, one per core.
   - **`batchsize=10000` plus the driver option `reWriteBatchedInserts=true`** sends each batch
     as one multi-row `INSERT`, much faster than one statement per row.
   - Spark's session time zone is UTC (as in Phase 1), so timestamps are stored exactly as they
     appear in Parquet.
3. **Create indexes and ANALYZE** (`sql/create_indexes.sql`).

The JDBC driver comes from `spark.jars.packages = org.postgresql:postgresql:42.7.3`. This is the
same as the spec's `spark-submit --packages ...`, but also works with plain `python`.

**Load times** (8 cores, local Docker):

| Table | Rows | Time |
|---|---|---|
| `nyc_clean` | 799,739 | 20.4 s |
| `la_clean` | 800,000 | 13.9 s |
| `unified` | 1,599,739 | 18.4 s |

## 4. Verification

`verify_db.py` reads every table back from PostgreSQL with Spark and compares it to the Parquet
dataset it came from. It runs 16 checks per table, **48 in total, and all pass.**

| Check | What it proves |
|---|---|
| Row count = Parquet row count | Nothing was lost or duplicated (799,739 / 800,000 / 1,599,739) |
| All Parquet columns present, no extra columns | Table design matches the Spark dataset |
| Each column's SQL type matches its Spark type (via `information_schema.columns`) | Types were kept |
| Primary key exists on the expected column, and that column is unique | Keys are in place |
| Same min/max `created_at` | Timestamps were not shifted by time zone handling |
| Same null counts for `closed_at`, ZIP, latitude | Nulls were kept as nulls, not turned into `''` or 0 |
| Same number of valid resolutions and same sum of `resolution_hours` | Numbers and booleans were kept |
| Same row count per `status` | Categorical values were kept |
| 2,000 random rows identical in every column (`exceptAll` both ways) | Values match row by row |

Status counts in PostgreSQL (`SELECT source, status, count(*) FROM unified GROUP BY 1, 2`)
match the Phase 1 numbers exactly:

| source | CLOSED | OPEN | IN_PROGRESS | PENDING | CANCELLED | REFERRED | UNKNOWN |
|---|---|---|---|---|---|---|---|
| NYC311 | 704,564 | 36,738 | 58,054 | 330 | — | — | 53 |
| LA311 | 730,126 | 16,081 | — | 17,972 | 34,725 | 1,096 | — |

## 5. Notes

- **Phase 1 fix found during this step.** While designing the tables from the Parquet schemas,
  we found that the cleaned NYC file had lost the time part of its original `created_date` and
  its original `city` values, because new columns with the same names overwrote them. Phase 1
  now renames those columns instead (see the Phase 1 report, §7). The cleaned datasets were
  regenerated before loading. Row counts and the unified dataset did not change.
- **Re-running is destructive by design.** `load_to_postgres.py` drops and recreates the
  tables, so any rows added later (e.g. through the WebUI) are removed by a reload.
- **Credentials** are course defaults for a local container. Change them with the `PG_*`
  environment variables for anything shared.

## 6. Reproducing

```bash
cd Database-Population
./start_postgres.sh      # PostgreSQL 16 in Docker
./run_all.sh             # create tables -> load with Spark -> create indexes -> verify
```

Phase 1 outputs must exist first (`../Dataset-Processing/run_all.sh`).
To look at the data:
`docker exec -it cs236-postgres psql -U cs236 -d cs236`.
