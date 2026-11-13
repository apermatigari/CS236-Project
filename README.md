# CS 236 Course Project: NYC 311 & LA MyLA311 Data Integration

CS 236 (Database Management Systems), Fall 2026. Team project.

This project builds a data integration and analytics pipeline over two public city
service-request datasets: New York City 311 and Los Angeles MyLA311. The datasets are explored,
cleaned and merged with PySpark. Phase 2 computes analytics and loads the results into a
Dockerized PostgreSQL database, and Phase 3 adds a WebUI to search, filter and insert requests.

## Status

| Phase | Section | Status |
|---|---|---|
| 1 | 1.1 Environment setup | Done |
| 1 | 1.2 Exploratory data analysis | Done (scripts + outputs in `EDA/`) |
| 1 | 1.3 Dataset processing | Done (24/24 validation checks pass) |
| 2 | 2.1 Spark analysis | Not started |
| 2 | 2.2 Database population | Done (48/48 database checks pass) |
| 3 | WebUI | Not started |

## Team

| Name | NetID |
|---|---|
| Andrew Permatigari | aperm003 |
| Aryan Sharma | ashar299 |

## Repository layout

```
CS236-Project/
├── README.md               ← this file
├── datasets/               ← put the raw CSVs here (not in git; see Data below)
├── EDA/                    ← Phase 1.2: exploratory data analysis
│   ├── NYC_EDA.py, LA_EDA.py, comparison_EDA.py
│   └── NYC_output.txt, LA_output.txt, comparison_output.txt   ← saved results
├── Dataset-Processing/     ← Phase 1.3: cleaning, schema mapping, merge, validation
│   ├── README.md           ← how to run
│   ├── REPORT.md           ← Phase 1 report (dataset processing)
│   ├── common.py, clean_nyc.py, clean_la.py, merge.py, validate.py, export_csv.py
│   ├── setup_env.sh, run_all.sh, requirements.txt
│   └── output/             ← generated data (git-ignored)
└── Database-Population/    ← Phase 2.2: PostgreSQL design + loading with Spark
    ├── README.md           ← how to run
    ├── REPORT.md           ← Phase 2 report (database population)
    ├── sql/                ← create_tables.sql, create_indexes.sql
    ├── load_to_postgres.py, verify_db.py, db_common.py
    ├── start_postgres.sh, run_all.sh, requirements.txt
    └── output/             ← run log (git-ignored)
```

## Data

The raw CSVs are **not** in the repo because of their size (about 1 GB total). Before
running anything:

1. Download the course dataset from UCR eLearn (UCR login required):
   https://elearn.ucr.edu/courses/238702/files/26557259/download?download_frd=1
2. Extract it if it's a zip, and put both CSVs in **`CS236-Project/datasets/`**:

```
CS236-Project/datasets/nyc_311_800k.csv
CS236-Project/datasets/la_311_800k.csv
```

Git ignores everything in `datasets/` except its README. To keep the files somewhere else,
set `CS236_DATA_DIR=/path/to/folder`. See [`datasets/README.md`](datasets/README.md) for details.
If the files are missing, the scripts stop with a message pointing here.

| File | Source | Rows |
|---|---|---|
| `nyc_311_800k.csv` | NYC 311 Service Requests | 800,000 |
| `la_311_800k.csv` | LA MyLA311 Service Requests | 800,000 |

## Environment

- Python 3.9+ with `pyspark==3.5.1`
- Java 17 (needed by Spark)
- Docker (needed from Phase 2 onward for PostgreSQL). On Linux, if Docker says `permission denied`,
  run `sudo usermod -aG docker $USER` and log out and back in.

`Dataset-Processing/setup_env.sh` sets up Python and Java without sudo. Check Docker with
`docker --version`.

### Quick start (everything built so far)

```bash
# 0. put the raw CSVs in datasets/ (see Data above)
cd Dataset-Processing
./setup_env.sh                 # one time: .venv with PySpark + Java 17 if missing
./run_all.sh                   # Phase 1.3: clean, merge, validate
cd ../Database-Population
./start_postgres.sh            # PostgreSQL 16 in Docker
./run_all.sh                   # Phase 2.2: load into PostgreSQL, verify
```

---

## Phase 1: Data Preparation & EDA

### 1.1 Environment setup
PySpark 3.5.1 runs locally (not in Docker), and Docker is installed for Phase 2. See
[Environment](#environment) above.

### 1.2 Exploratory Data Analysis
PySpark scripts in [`EDA/`](EDA/) profile each dataset and compare them:

| Script | What it covers | Saved output |
|---|---|---|
| `NYC_EDA.py` | Schema, dimensions, date ranges, sample rows, missing values, duplicates, distinct values, invalid/zero/negative numbers, coordinates, future and out-of-order dates, distributions by month/hour | `NYC_output.txt` |
| `LA_EDA.py` | The same checks for LA | `LA_output.txt` |
| `comparison_EDA.py` | Matching column names and types across both datasets, columns found in only one, and the categorical values of matched columns | `comparison_output.txt` |

**Run.** The scripts read the CSVs from the current folder, so run them from `datasets/`:

```bash
cd datasets
../.venv/bin/python ../EDA/NYC_EDA.py > ../EDA/NYC_output.txt   # same for LA_EDA.py, comparison_EDA.py
```

(Use the Python from the `.venv` that `setup_env.sh` created, wherever it lives.)

**Key findings** (from the saved outputs) and how Section 1.3 used them:

| Finding | NYC | LA | Used in 1.3 |
|---|---|---|---|
| Size | 800,000 rows × 44 columns | 800,000 rows × 34 columns | |
| Created-date range | 2026-07-22 → 2026-10-05 | 2024-01-01 → 2024-07-24 | Noted as a limitation (different periods) |
| Exact duplicate rows | 0 | 0 | Also removed 261 NYC rows identical except for the ID |
| Close time before created time | 626 | 0 | Flagged; close time set to null |
| Other dates before created time | 33,656 `resolution_action_updated_date` | 71,297 `servicedate` | Kept as recorded; not used for resolution time |
| Invalid ZIPs | 29 (1 × `No`, 28 outside NYC's ZIP range) | 39 (38 × `0`, 1 × `VE, 0`) | Set to null |
| Coordinates | All inside NYC | All inside LA | Range check kept for safety |
| Placeholder values | `Unspecified` in status, borough, channel (`UNKNOWN`) | empty strings | Converted to null |
| Schema differences | `snake_case` names, 44 cols | lowercase run-together names, 34 cols | Mapped to one unified schema |
| Category differences | 7 statuses, 5 channels | 6 statuses, 15 request sources | Standardized status and channel, originals kept |

The cleaning decisions based on these findings are explained in
[`Dataset-Processing/REPORT.md`](Dataset-Processing/REPORT.md).

### 1.3 Dataset Processing
Cleans both datasets, maps them to a shared schema, standardizes status and channel values,
adds derived attributes (unique `request_id`, `resolution_hours`, date parts), and merges them.
24 validation checks pass.

```bash
# first put the raw CSVs in CS236-Project/datasets/ (see Data above)
cd Dataset-Processing
./setup_env.sh && ./run_all.sh
```

| Output | Rows |
|---|---|
| Cleaned NYC | 799,739 |
| Cleaned LA | 800,000 |
| Unified dataset | 1,599,739 |

Details: [`Dataset-Processing/README.md`](Dataset-Processing/README.md) (how to run) and
[`Dataset-Processing/REPORT.md`](Dataset-Processing/REPORT.md) (decisions and results).

### Phase 1 deliverables

| Deliverable | Location |
|---|---|
| Report: EDA and key findings | Summary above + `EDA/*_output.txt` (full EDA write-up: _TODO_) |
| Report: data-quality issues, cleaning, mapping, standardization, unified dataset | `Dataset-Processing/REPORT.md` |
| Well-documented source code | `EDA/`, `Dataset-Processing/` |
| Cleaned NYC dataset | `Dataset-Processing/output/nyc_clean/` (Parquet), `output/csv/nyc_clean.csv` |
| Cleaned LA dataset | `Dataset-Processing/output/la_clean/` (Parquet), `output/csv/la_clean.csv` |
| Unified dataset | `Dataset-Processing/output/unified/` (Parquet), `output/csv/unified.csv` |
| 5-minute video | _TODO_ |

The datasets are git-ignored because of their size. Generate them with `run_all.sh` and
`export_csv.py`, then add them to the submission zip.

---

## Phase 2: Spark Analysis & Database Population

### 2.1 Spark Analysis
> **Placeholder.** Not started. Will run on the unified dataset (Parquet or the `unified` table):
> - Weekly request volume: requests per city in days 1–7 / 8–14 / 15–21 / 22–end (uses `created_day`)
> - Service-request hotspots: top 10 ZIPs per city, top 3 request types in each
> - Bonus: resolution performance (uses `resolution_hours`, `is_valid_resolution`), status and
>   channel breakdowns (uses standardized `status`, `channel`)

### 2.2 Database Population
Loads the cleaned NYC, cleaned LA, and unified datasets into PostgreSQL 16 (Docker) with
PySpark over JDBC. The tables have typed columns, primary keys, NOT NULL and CHECK constraints.
48 verification checks pass (row counts, types, keys, values).

```bash
# needs Docker and the Phase 1 outputs (Dataset-Processing/run_all.sh)
cd Database-Population
./start_postgres.sh && ./run_all.sh
```

| Table | Rows | Primary key |
|---|---|---|
| `nyc_clean` | 799,739 | `unique_key` |
| `la_clean` | 800,000 | `srnumber` |
| `unified` | 1,599,739 | `request_id` |

Details: [`Database-Population/README.md`](Database-Population/README.md) (how to run) and
[`Database-Population/REPORT.md`](Database-Population/REPORT.md) (design and results).

### Phase 2 deliverables

| Deliverable | Location |
|---|---|
| Report: Spark analyses and findings | _TODO (2.1)_ |
| Report: PostgreSQL database design, population process | `Database-Population/REPORT.md` |
| Well-documented source code | `Database-Population/` (+ _TODO 2.1_) |
| SQL files to create the tables | `Database-Population/sql/create_tables.sql`, `sql/create_indexes.sql` |
| 5-minute video | _TODO_ |

## Phase 3: WebUI
> **Placeholder.** Not started.
> - Search, filter and insert service requests through a web interface backed by PostgreSQL
