# Database-Population (Phase 2, Section 2.2)

Loads the three processed datasets (cleaned NYC, cleaned LA, unified) into PostgreSQL 16 running
in Docker, using PySpark over JDBC, and then checks the result. The design and results are
explained in **[REPORT.md](REPORT.md)**.

## Requirements

- Everything from [`../Dataset-Processing`](../Dataset-Processing/README.md): Python with
  `pyspark==3.5.1`, Java 17, and the raw CSVs in `CS236-Project/datasets/`
- **Phase 1 outputs:** run `../Dataset-Processing/run_all.sh` first. This folder reads its
  Parquet files from `../Dataset-Processing/output/`.
- **Docker**, usable without sudo. On Linux, if you get `permission denied` on
  `/var/run/docker.sock`, run `sudo usermod -aG docker $USER`, then log out and back in
  (or run `newgrp docker`).
- Internet access on the first run. Spark downloads the PostgreSQL JDBC driver
  (`org.postgresql:postgresql:42.7.3`) and Docker pulls `postgres:16`.

## Run

```bash
./start_postgres.sh   # start PostgreSQL 16 in Docker (container cs236-postgres)
./run_all.sh          # create tables -> load with Spark -> create indexes -> verify
```

A full run takes about 2 minutes. Output is saved to `output/run_log.txt`.

You can also run the steps on their own:

```bash
python load_to_postgres.py      # or: spark-submit --packages org.postgresql:postgresql:42.7.3 load_to_postgres.py
python verify_db.py
```

Use the Python from the project `.venv`. `run_all.sh` finds it automatically.

## Connection settings

| Variable | Default |
|---|---|
| `PG_HOST` | `localhost` |
| `PG_PORT` | `5432` |
| `PG_DB` | `cs236` |
| `PG_USER` | `cs236` |
| `PG_PASSWORD` | `cs236` |

`start_postgres.sh` and the Python scripts read the same variables, so any change applies to
both.

Open a SQL shell: `docker exec -it cs236-postgres psql -U cs236 -d cs236`

## Files

| File | Purpose |
|---|---|
| `sql/create_tables.sql` | Table definitions: types, primary keys, UNIQUE, NOT NULL, CHECK (drops existing tables) |
| `sql/create_indexes.sql` | Secondary indexes + ANALYZE, run after the load |
| `db_common.py` | Connection settings, Spark session with the JDBC driver, SQL-file runner |
| `load_to_postgres.py` | Creates tables, loads all three datasets with Spark, builds indexes |
| `verify_db.py` | 48 checks: row counts, columns and types, keys, value fingerprints, sampled rows |
| `start_postgres.sh` | Starts (or creates) the `postgres:16` container |
| `run_all.sh` | Runs load + verify and writes `output/run_log.txt` |
| `requirements.txt` | Python dependency (`pyspark==3.5.1`, same as Phase 1) |
| `REPORT.md` | Section 2.2 report |

## Tables

| Table | Rows | Primary key |
|---|---|---|
| `nyc_clean` | 799,739 | `unique_key` |
| `la_clean` | 800,000 | `srnumber` |
| `unified` | 1,599,739 | `request_id` |

## Stop / reset

```bash
docker stop cs236-postgres                                   # stop (data kept)
docker rm -f cs236-postgres && docker volume rm cs236-pgdata  # delete everything
```
