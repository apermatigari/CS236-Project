"""
db_common.py - Shared settings and helpers for loading the processed datasets into PostgreSQL
(Phase 2, Section 2.2).

Connection settings
-------------------
Read from environment variables, with defaults that match start_postgres.sh:

    PG_HOST=localhost  PG_PORT=5432  PG_DB=cs236  PG_USER=cs236  PG_PASSWORD=cs236

Input data
----------
The three Parquet datasets produced by ../Dataset-Processing (run its run_all.sh first).
Their paths are imported from Dataset-Processing/common.py so both phases always agree.
"""
import os
import sys

from pyspark.sql import SparkSession

HERE = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(HERE, "sql")

# Reuse the Phase 1 output paths instead of repeating them here.
sys.path.insert(0, os.path.join(HERE, "..", "Dataset-Processing"))
from common import NYC_CLEAN, LA_CLEAN, UNIFIED  # noqa: E402

# PostgreSQL table name -> Parquet folder. Order matters only for log readability.
TABLES = {"nyc_clean": NYC_CLEAN, "la_clean": LA_CLEAN, "unified": UNIFIED}

# --------------------------------------------------------------------------- connection
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "cs236")
PG_USER = os.environ.get("PG_USER", "cs236")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "cs236")

# reWriteBatchedInserts makes the driver send each JDBC batch as one multi-row INSERT,
# which loads the 1.6M unified rows several times faster than row-by-row inserts.
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?reWriteBatchedInserts=true"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

# PostgreSQL JDBC driver, as required by the project spec. Setting it in the session
# config means `python load_to_postgres.py` works without spark-submit; running with
# `spark-submit --packages org.postgresql:postgresql:42.7.3 ...` works too.
JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"


def spark(app):
    """Local Spark session with the PostgreSQL JDBC driver on the classpath.

    Uses the same UTC session time zone as Phase 1, so timestamps go into PostgreSQL
    `timestamp` columns exactly as they appear in the Parquet files.
    """
    s = (SparkSession.builder.appName(app).master("local[*]")
         .config("spark.jars.packages", JDBC_PACKAGE)
         .config("spark.driver.memory", "6g")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def run_sql_file(s, filename):
    """Run every statement in sql/<filename> over one JDBC connection.

    Spark's DataFrame API can't run DDL (CREATE TABLE etc.), so the PostgreSQL driver
    that Spark already loaded is used directly through the JVM. The SQL files contain
    only plain statements separated by ';' (no functions or quoted semicolons), so
    splitting on ';' is safe. All statements run in one transaction: if one fails,
    none are applied.
    """
    path = os.path.join(SQL_DIR, filename)
    with open(path) as f:
        # drop '--' comment lines, then split into statements
        text = "\n".join(line for line in f if not line.strip().startswith("--"))
    statements = [st.strip() for st in text.split(";") if st.strip()]

    # The driver jar from spark.jars.packages lives in Spark's class loader, which
    # java.sql.DriverManager can't see. So the driver class is loaded through Spark's
    # loader and asked for a connection directly.
    jvm = s.sparkContext._jvm
    loader = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
    driver = loader.loadClass("org.postgresql.Driver").newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", PG_USER)
    props.setProperty("password", PG_PASSWORD)
    conn = driver.connect(JDBC_URL, props)
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        for st in statements:
            stmt.execute(st)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(f"ran {filename} ({len(statements)} statements)")


def read_table(s, table):
    """Read a whole PostgreSQL table into a Spark DataFrame."""
    return s.read.jdbc(JDBC_URL, table, properties=JDBC_PROPS)


def read_query(s, sql):
    """Run a SELECT in PostgreSQL and return the result as a Spark DataFrame."""
    return (s.read.format("jdbc").option("url", JDBC_URL).option("query", sql)
            .options(**JDBC_PROPS).load())
