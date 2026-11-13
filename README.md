# CS 236 Course Project: NYC 311 & LA MyLA311 Data Integration

CS 236 (Database Management Systems), Fall 2026. Team project.

This project builds a data integration and analytics pipeline over two public city
service-request datasets: New York City 311 and Los Angeles MyLA311. The datasets are explored,
cleaned and merged with PySpark. Phase 2 computes analytics and loads the results into a
Dockerized PostgreSQL database, and Phase 3 adds a WebUI to search, filter and insert requests.

## Team

| Name | NetID |
|---|---|
| _Andrew Permatigari_ | _aperm003_ |
| _Aryan Sharma_ | _ashar299_ |

## Repository layout

```
CS236-Project/
├── README.md               ← this file
├── datasets/               ← put the raw CSVs here (not in git; see Data below)
├── EDA/                    ← Phase 1.2: exploratory data analysis
├── Dataset-Processing/     ← Phase 1.3: cleaning, schema mapping, merge, validation
│   ├── README.md           ← how to run
│   ├── REPORT.md           ← Phase 1 report (dataset processing)
│   ├── common.py, clean_nyc.py, clean_la.py, merge.py, validate.py, export_csv.py
│   └── output/             ← generated data (git-ignored)
└── Database-Population/    ← Phase 2.2: PostgreSQL design + loading with Spark
    ├── README.md           ← how to run
    ├── REPORT.md           ← Phase 2 report (database population)
    ├── sql/                ← create_tables.sql, create_indexes.sql
    ├── load_to_postgres.py, verify_db.py, db_common.py
    └── start_postgres.sh, run_all.sh
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

---

## Phase 1: Data Preparation & EDA

### 1.1 Environment setup
See [Environment](#environment) above.

### 1.2 Exploratory Data Analysis
> **Placeholder.** EDA is in [`EDA/`](EDA/).
>
> - _TODO: how to run the EDA scripts_
> - _TODO: summary of key findings (schemas, data quality, distributions, schema comparison)_
> - _TODO: link to the EDA section of the report_

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
| Report: EDA and key findings | _TODO (EDA)_ |
| Report: data-quality issues, cleaning, mapping, standardization, unified dataset | `Dataset-Processing/REPORT.md` |
| Well-documented source code | `EDA/`, `Dataset-Processing/` |
| Cleaned NYC / cleaned LA / unified datasets | `Dataset-Processing/output/` (CSV via `export_csv.py`) |
| 5-minute video | _TODO_ |

---

## Phase 2: Spark Analysis & Database Population

### 2.1 Spark Analysis
> **Placeholder.** Not started.
> - Weekly request volume, service-request hotspots (+ optional resolution performance, status/channel)

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
