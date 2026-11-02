"""Shared helpers: Spark session, paths, and small cleaning utilities."""
import os
from pyspark.sql import SparkSession, functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATA_DIR = os.path.join(ROOT, "datasets")
OUT_DIR = os.path.join(HERE, "output")
NYC_RAW = os.path.join(DATA_DIR, "nyc_311_800k.csv")
LA_RAW = os.path.join(DATA_DIR, "la_311_800k.csv")
NYC_CLEAN = os.path.join(OUT_DIR, "nyc_clean")
LA_CLEAN = os.path.join(OUT_DIR, "la_clean")
UNIFIED = os.path.join(OUT_DIR, "unified")

# Values that mean "no data" in these files.
NULL_TOKENS = ["", "N/A", "NA", "NULL", "Unspecified", "UNSPECIFIED", "Unknown", "UNKNOWN"]
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSS"


def spark(app):
    s = (SparkSession.builder.appName(app).master("local[*]")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def read_csv(s, path):
    # Everything is read as string first; types are assigned explicitly during cleaning.
    return s.read.csv(path, header=True, multiLine=True, escape='"', inferSchema=False)


def clean_str(c):
    """Trim; map null-like tokens to null."""
    t = F.trim(F.col(c))
    return F.when(t.isin(NULL_TOKENS), None).otherwise(t)


def to_ts(c):
    return F.to_timestamp(F.col(c), TS_FMT)


def clean_zip(c, valid_prefix_regex):
    """Keep the first 5 digits if they form a ZIP in the city's range, else null."""
    z = F.regexp_extract(F.col(c), r"^(\d{5})", 1)
    return F.when(z.rlike(valid_prefix_regex), z).otherwise(None)


def in_box(lat, lon, box):
    la0, la1, lo0, lo1 = box
    return lat.between(la0, la1) & lon.between(lo0, lo1)


def add_derived(df):
    """Shared derived attributes, computed on standardized columns."""
    valid_close = (F.col("closed_at").isNotNull() & (F.col("closed_at") >= F.col("created_at"))
                   & (F.col("status") == "CLOSED"))
    return (df
            .withColumn("is_valid_resolution", valid_close)
            .withColumn("resolution_hours",
                        F.when(valid_close, F.round((F.col("closed_at").cast("long")
                                                     - F.col("created_at").cast("long")) / 3600.0, 4)))
            .withColumn("created_date", F.to_date("created_at"))
            .withColumn("created_year", F.year("created_at"))
            .withColumn("created_month", F.month("created_at"))
            .withColumn("created_day", F.dayofmonth("created_at"))
            .withColumn("created_hour", F.hour("created_at"))
            .withColumn("created_dow", F.date_format("created_at", "E")))


def write(df, path):
    df.write.mode("overwrite").parquet(path)


def mapper(col, mapping, default):
    """Map raw categorical values to a standard vocabulary; unmapped/null -> default."""
    expr = None
    for k, v in mapping.items():
        cond = F.col(col) == k
        expr = F.when(cond, v) if expr is None else expr.when(cond, v)
    return expr.otherwise(default)
