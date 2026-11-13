"""
verify_db.py - Check that PostgreSQL holds exactly what the Spark datasets contain.

For each table (nyc_clean, la_clean, unified):
  1. Record count  - table row count == Parquet row count.
  2. Schema        - every Parquet column exists in the table with a matching SQL type
                     (e.g. Spark timestamp -> PostgreSQL timestamp without time zone),
                     and the table has no extra columns.
  3. Keys          - a primary key exists, and the key column has no duplicates.
  4. Values        - aggregate "fingerprints" match between Parquet and PostgreSQL:
                     min/max created_at, null counts of key optional columns, number of
                     valid resolutions, sum of resolution_hours, status distribution.
  5. Sample rows   - 2,000 random rows read back from PostgreSQL are identical to the
                     same rows in Parquet, in every column.
Exits with code 1 if any check fails.

Run:  python verify_db.py   (after load_to_postgres.py)
"""
import sys

from pyspark.sql import functions as F

from db_common import TABLES, spark, read_table, read_query

# Spark type -> PostgreSQL information_schema data_type values it may be stored as.
TYPE_MAP = {
    "string": {"character varying", "text"},
    "timestamp": {"timestamp without time zone"},
    "date": {"date"},
    "int": {"integer", "smallint"},
    "bigint": {"bigint"},
    "double": {"double precision"},
    "boolean": {"boolean"},
}
PRIMARY_KEYS = {"nyc_clean": "unique_key", "la_clean": "srnumber", "unified": "request_id"}
SAMPLE_ROWS = 2000


def check(name, passed, detail=""):
    """Print one PASS/FAIL line; return the result so checks can be combined."""
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))
    return passed


def fingerprint(df, zip_col):
    """Aggregates that should be identical in Parquet and in PostgreSQL."""
    row = df.agg(
        F.count("*").alias("rows"),
        F.min("created_at").alias("min_created"),
        F.max("created_at").alias("max_created"),
        F.sum(F.col("closed_at").isNull().cast("int")).alias("null_closed_at"),
        F.sum(F.col(zip_col).isNull().cast("int")).alias("null_zip"),
        F.sum(F.col("latitude").isNull().cast("int")).alias("null_latitude"),
        F.sum(F.col("is_valid_resolution").cast("int")).alias("valid_resolutions"),
        F.round(F.sum("resolution_hours"), 2).alias("sum_resolution_hours"),
    ).first().asDict()
    row["status_counts"] = {r["status"]: r["count"] for r in df.groupBy("status").count().collect()}
    return row


def verify_table(s, table, path):
    print(f"\n=== {table} ===")
    pq = s.read.parquet(path)
    db = read_table(s, table)
    key = PRIMARY_KEYS[table]
    ok = True

    # 1. record count
    n_pq, n_db = pq.count(), db.count()
    ok &= check("row count matches Parquet", n_pq == n_db, f"parquet={n_pq:,} postgres={n_db:,}")

    # 2. schema: names and types
    cols = {r["column_name"]: r for r in read_query(s, f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns WHERE table_name = '{table}'""").collect()}
    missing = [f.name for f in pq.schema if f.name not in cols]
    extra = sorted(set(cols) - set(pq.columns))
    bad_types = [f"{f.name}:{f.dataType.simpleString()}->{cols[f.name]['data_type']}"
                 for f in pq.schema if f.name in cols
                 and cols[f.name]["data_type"] not in TYPE_MAP[f.dataType.simpleString()]]
    ok &= check("all Parquet columns present", not missing, ", ".join(missing))
    ok &= check("no extra table columns", not extra, ", ".join(extra))
    ok &= check(f"SQL types match Spark types ({len(pq.columns)} columns)", not bad_types,
                ", ".join(bad_types))
    not_null = sum(1 for r in cols.values() if r["is_nullable"] == "NO")
    print(f"  info: {not_null} NOT NULL columns")

    # 3. keys
    pk = read_query(s, f"""
        SELECT kcu.column_name FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name
        WHERE tc.table_name = '{table}' AND tc.constraint_type = 'PRIMARY KEY'""").collect()
    ok &= check(f"primary key on {key}", [r[0] for r in pk] == [key])
    ok &= check(f"{key} unique in PostgreSQL", db.select(key).distinct().count() == n_db)

    # 4. value fingerprints
    zip_col = "incident_zip" if table == "nyc_clean" else "zipcode"
    fp_pq, fp_db = fingerprint(pq, zip_col), fingerprint(db, zip_col)
    for k in fp_pq:
        ok &= check(f"{k} matches", fp_pq[k] == fp_db[k],
                    "" if fp_pq[k] == fp_db[k] else f"parquet={fp_pq[k]} postgres={fp_db[k]}")

    # 5. full-row comparison on a random sample. PostgreSQL SMALLINT columns come back
    #    as Spark short, so the DB rows are cast to the Parquet schema before comparing.
    sample = pq.orderBy(F.rand(seed=236)).limit(SAMPLE_ROWS).cache()
    ids = [r[0] for r in sample.select(key).collect()]
    db_sample = db.filter(F.col(key).isin(ids)).select(
        [F.col(f.name).cast(f.dataType) for f in pq.schema])
    diff = sample.exceptAll(db_sample).count() + db_sample.exceptAll(sample).count()
    ok &= check(f"{SAMPLE_ROWS:,} sampled rows identical in every column", diff == 0,
                f"{diff} differing rows" if diff else "")
    return ok


def main():
    s = spark("verify_db")
    ok = True
    for table, path in TABLES.items():
        ok &= verify_table(s, table, path)
    print("\nALL DATABASE CHECKS PASSED" if ok else "\nSOME DATABASE CHECKS FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
