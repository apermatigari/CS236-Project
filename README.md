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
├── EDA/                    ← Phase 1.2: exploratory data analysis
└── Dataset-Processing/     ← Phase 1.3: cleaning, schema mapping, merge, validation
    ├── README.md           ← how to run
    ├── REPORT.md           ← Phase 1 report (dataset processing)
    ├── common.py, clean_nyc.py, clean_la.py, merge.py, validate.py, export_csv.py
    └── output/             ← generated data (git-ignored)
```

## Data

The raw CSVs are **not** in the repo because of their size (about 1 GB total). Put them in a
`datasets/` folder next to this repo or inside it, or set `CS236_DATA_DIR`:

| File | Source | Rows |
|---|---|---|
| `nyc_311_800k.csv` | NYC 311 Service Requests | 800,000 |
| `la_311_800k.csv` | LA MyLA311 Service Requests | 800,000 |

## Environment

- Python 3.9+ with `pyspark==3.5.1`
- Java 17 (needed by Spark)
- Docker (needed from Phase 2 onward for PostgreSQL)

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
> **Placeholder.** Not started.
> - Weekly request volume, service-request hotspots (+ optional resolution performance, status/channel)
> - PostgreSQL schema (SQL files) and population via Spark

## Phase 3: WebUI
> **Placeholder.** Not started.
> - Search, filter and insert service requests through a web interface backed by PostgreSQL
