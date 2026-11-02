"""
common.py - Shared configuration and helpers for the Dataset Processing pipeline (Section 1.3).

All other scripts import from here so that paths, the Spark configuration, and the
cleaning rules shared by both cities are defined in exactly one place.

Path resolution
---------------
* Raw CSVs: not in git. Download them from UCR eLearn (DATA_URL below, login required)
  into ``CS236-Project/datasets/`` (see datasets/README.md). They are looked up in the
  CS236_DATA_DIR environment variable if set; otherwise in the closest folder named
  ``datasets`` that contains the CSVs, walking up from this file.
* Outputs: written to ``Dataset-Processing/output/`` (git-ignored).
"""
import os

from pyspark.sql import SparkSession, functions as F

# --------------------------------------------------------------------------- paths
HERE = os.path.dirname(os.path.abspath(__file__))


DATA_URL = "https://elearn.ucr.edu/courses/238702/files/26557259/download?download_frd=1"
RAW_FILES = ("nyc_311_800k.csv", "la_311_800k.csv")


def _find_data_dir():
    """Return the folder holding the raw CSVs (see module docstring).

    A ``datasets`` folder only counts if it actually contains the CSVs, so the
    repo's own ``datasets/`` (which holds just a README until the data is added)
    doesn't hide a populated folder higher up.
    """
    env = os.environ.get("CS236_DATA_DIR")
    if env:
        return env
    d = HERE
    while True:
        candidate = os.path.join(d, "datasets")
        if all(os.path.isfile(os.path.join(candidate, f)) for f in RAW_FILES):
            return candidate
        parent = os.path.dirname(d)
        if parent == d:
            raise FileNotFoundError(
                "Raw data not found. Download the course dataset from UCR eLearn:\n"
                f"  {DATA_URL}\n"
                "and put nyc_311_800k.csv and la_311_800k.csv in CS236-Project/datasets/ "
                "(see datasets/README.md), or set CS236_DATA_DIR to their folder.")
        d = parent


DATA_DIR = _find_data_dir()
OUT_DIR = os.path.join(HERE, "output")

NYC_RAW = os.path.join(DATA_DIR, "nyc_311_800k.csv")
LA_RAW = os.path.join(DATA_DIR, "la_311_800k.csv")
NYC_CLEAN = os.path.join(OUT_DIR, "nyc_clean")   # Parquet: cleaned NYC dataset
LA_CLEAN = os.path.join(OUT_DIR, "la_clean")     # Parquet: cleaned LA dataset
UNIFIED = os.path.join(OUT_DIR, "unified")       # Parquet: unified dataset

# --------------------------------------------------------------------------- constants
# Placeholder strings that mean "no value" in these files. They are turned into real
# nulls so missing data is handled the same way in every column and both cities.
NULL_TOKENS = ["", "N/A", "NA", "NULL", "Unspecified", "UNSPECIFIED", "Unknown", "UNKNOWN"]

# Both sources store timestamps as ISO-8601 text, e.g. 2024-01-01T00:00:06.000
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSS"


# --------------------------------------------------------------------------- spark / IO
def spark(app):
    """Create (or reuse) a local Spark session using all cores.

    The session time zone is pinned to UTC so timestamps are parsed and written the same
    way on every machine (the raw data has no zone; values are kept as recorded).
    """
    s = (SparkSession.builder.appName(app).master("local[*]")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false")  # keep logs readable
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def read_csv(s, path):
    """Read a raw 311 CSV with every column as a string.

    Schema inference is turned off on purpose: it can guess wrong types (e.g. ZIP codes as
    integers, which drops leading zeros) and it costs an extra pass over the file. Each
    cleaning script casts columns explicitly instead. ``multiLine``/``escape`` handle
    quoted fields that contain commas or line breaks.
    """
    return s.read.csv(path, header=True, multiLine=True, escape='"', inferSchema=False)


def write(df, path):
    """Save a DataFrame as Parquet (keeps column types, compressed, fast to reload)."""
    df.write.mode("overwrite").parquet(path)


# --------------------------------------------------------------------------- cleaning helpers
def clean_str(c):
    """Trim whitespace and turn placeholder values (NULL_TOKENS) into null."""
    t = F.trim(F.col(c))
    return F.when(t.isin(NULL_TOKENS), None).otherwise(t)


def to_ts(c):
    """Parse an ISO-8601 text column into a Spark timestamp (null if malformed)."""
    return F.to_timestamp(F.col(c), TS_FMT)


def clean_zip(c, valid_regex):
    """Keep a ZIP only if it starts with 5 digits that are in the city's ZIP range.

    ZIP+4 values (12345-6789) keep their first 5 digits. Anything else (e.g. ``0``,
    ``No``, or a ZIP from another state) becomes null, so it can't later show up as a
    fake hotspot in the postal-code analysis.
    """
    z = F.regexp_extract(F.col(c), r"^(\d{5})", 1)
    return F.when(z.rlike(valid_regex), z).otherwise(None)


def in_box(lat, lon, box):
    """True when (lat, lon) is inside box = (min_lat, max_lat, min_lon, max_lon)."""
    lat0, lat1, lon0, lon1 = box
    return lat.between(lat0, lat1) & lon.between(lon0, lon1)


def mapper(col, mapping, default):
    """Map raw categorical values to a standard vocabulary.

    Builds ``CASE WHEN col = k1 THEN v1 WHEN col = k2 THEN v2 ... ELSE default``.
    Unmapped values and nulls fall through to ``default``.
    """
    expr = None
    for raw_value, std_value in mapping.items():
        cond = F.col(col) == raw_value
        expr = F.when(cond, std_value) if expr is None else expr.when(cond, std_value)
    return expr.otherwise(default)


def add_derived(df):
    """Add the derived attributes shared by both cities.

    Expects the standardized columns ``created_at``, ``closed_at`` and ``status``.

    * is_valid_resolution - True only when the request is CLOSED and has a close time that
      is not before its created time. Requests that are open, cancelled, or closed without
      a usable close time get False, so they can't distort resolution statistics.
    * resolution_hours    - (closed_at - created_at) in hours, only for valid resolutions.
      Hours (not days) keep precision for the many requests closed the same day.
    * created_*           - date parts of created_at. created_day is what Phase 2 needs to
      split each month into the 1-7 / 8-14 / 15-21 / 22-end periods.
    """
    valid_close = (F.col("closed_at").isNotNull()
                   & (F.col("closed_at") >= F.col("created_at"))
                   & (F.col("status") == "CLOSED"))
    seconds_open = F.col("closed_at").cast("long") - F.col("created_at").cast("long")
    return (df
            .withColumn("is_valid_resolution", valid_close)
            .withColumn("resolution_hours", F.when(valid_close, F.round(seconds_open / 3600.0, 4)))
            .withColumn("created_date", F.to_date("created_at"))
            .withColumn("created_year", F.year("created_at"))
            .withColumn("created_month", F.month("created_at"))
            .withColumn("created_day", F.dayofmonth("created_at"))
            .withColumn("created_hour", F.hour("created_at"))
            .withColumn("created_dow", F.date_format("created_at", "E")))  # Mon, Tue, ...


def print_summary(title, rows):
    """Print a small 'step -> count' table so each run documents what cleaning changed."""
    print(f"\n--- {title} ---")
    for label, value in rows:
        print(f"  {label:<45} {value:>10,}")
