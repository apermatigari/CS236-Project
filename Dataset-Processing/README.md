# Dataset-Processing (Phase 1, Section 1.3)

PySpark pipeline that cleans the NYC 311 and LA MyLA311 datasets, saves a cleaned copy of each,
and merges them into one unified dataset. Decisions and results are explained in
**[REPORT.md](REPORT.md)**.

## Requirements

- Python 3.9+ with `pyspark==3.5.1` (`requirements.txt`)
- Java 17 (Spark needs a JVM)
- The raw CSVs `nyc_311_800k.csv` and `la_311_800k.csv` in a folder named `datasets`
  in this folder or any folder above it. Alternatively, point `CS236_DATA_DIR` to the folder.

`./setup_env.sh` creates `.venv/` with PySpark and, if no Java is installed, downloads a local
OpenJDK 17 into `.jdk/`. No sudo is needed, and it works on Linux and macOS.

## Run

```bash
./setup_env.sh                       # one time
./run_all.sh                         # clean_nyc -> clean_la -> merge -> validate
.venv/bin/python export_csv.py       # optional: CSV copies (--sample N, --only unified)
```

`run_all.sh` finds the nearest `.venv` and `.jdk` in this folder or above it. You can override
them with the `PYTHON` and `JAVA_HOME` environment variables. A full run takes a few minutes.

## Files

| File | Purpose |
|---|---|
| `common.py` | Paths, Spark session, shared cleaning helpers, derived attributes |
| `clean_nyc.py` | Cleans NYC → `output/nyc_clean/` |
| `clean_la.py` | Cleans LA → `output/la_clean/` |
| `merge.py` | Maps both to the unified schema → `output/unified/` |
| `validate.py` | 24 checks on schema, types, counts, ID uniqueness, derived attributes |
| `export_csv.py` | Writes Parquet outputs as CSV to `output/csv/` |
| `run_all.sh` / `setup_env.sh` | Pipeline runner / environment setup |
| `REPORT.md` | Phase 1 report for this section |

## Outputs (`output/`, git-ignored)

| Path | Contents | Rows |
|---|---|---|
| `nyc_clean/` | Cleaned NYC (all original columns + standardized/derived) | 799,739 |
| `la_clean/` | Cleaned LA (all original columns + standardized/derived) | 800,000 |
| `unified/` | Unified service-request dataset | 1,599,739 |
| `csv/` | CSV copies from `export_csv.py` | |
| `run_log.txt` | Cleaning summaries and validation results from the last run | |

The outputs are Parquet. To view them, load them with `spark.read.parquet(...)` or
`pandas.read_parquet(...)` (needs `pyarrow`), or use the CSV copies.
