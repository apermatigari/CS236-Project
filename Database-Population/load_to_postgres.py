"""
load_to_postgres.py - Load the three processed datasets into PostgreSQL with PySpark.

Steps
-----
1. Run sql/create_tables.sql: drops and recreates nyc_clean, la_clean and unified with
   the designed types, keys and constraints. Recreating makes every run start from a
   clean state, so re-running never fails on duplicate primary keys.
2. For each table, read the Parquet dataset and append it through JDBC. Appending into
   the pre-created table, instead of letting Spark create it with mode="overwrite",
   keeps our column types and constraints. Spark would otherwise create plain TEXT /
   no-key tables.
3. Run sql/create_indexes.sql to build secondary indexes after the load.

Run (Postgres must be running, see start_postgres.sh; Phase 1 outputs must exist):
    python load_to_postgres.py
or, as in the project spec:
    spark-submit --packages org.postgresql:postgresql:42.7.3 load_to_postgres.py
"""
import time

from db_common import TABLES, JDBC_URL, JDBC_PROPS, spark, run_sql_file

# JDBC write tuning:
#   8 partitions = 8 parallel connections (one per local core).
#   batchsize = rows sent per batch (combined with reWriteBatchedInserts in the URL).
WRITE_PARTITIONS = 8
BATCH_SIZE = 10000


def main():
    s = spark("load_to_postgres")

    run_sql_file(s, "create_tables.sql")

    for table, path in TABLES.items():
        t0 = time.time()
        df = s.read.parquet(path)
        n = df.count()
        # Spark builds INSERT statements from the DataFrame's column names, so the
        # Parquet columns must match the table's columns (checked again in verify_db.py).
        (df.repartition(WRITE_PARTITIONS)
           .write
           .mode("append")
           .option("batchsize", BATCH_SIZE)
           .jdbc(JDBC_URL, table, properties=JDBC_PROPS))
        print(f"loaded {table:<10} {n:>10,} rows in {time.time() - t0:5.1f}s")

    run_sql_file(s, "create_indexes.sql")
    print("done")


if __name__ == "__main__":
    main()
