"""
validate.py - Check the unified dataset after merge.py.

Prints the unified schema, then a PASS/FAIL line per check:
  * schema / types  - key columns have the expected Spark types
  * record counts   - unified = NYC cleaned + LA cleaned, and per-source counts match
  * identifiers     - request_id is never null and is unique across both cities
  * categoricals    - status and channel only contain the standard values
  * derived values  - resolution_hours is non-negative and present exactly when
                      is_valid_resolution is true; no closed_at before created_at;
                      ZIPs are 5 digits or null; date parts agree with created_at
Finally prints status / channel / resolution breakdowns per city.
Exits with code 1 if any check fails, so it can be used in scripts.

Run:  python validate.py   (after merge.py)
"""
import sys

from pyspark.sql import functions as F, types as T

from common import NYC_CLEAN, LA_CLEAN, UNIFIED, spark

EXPECTED_TYPES = {
    "request_id": T.StringType(), "source": T.StringType(),
    "created_at": T.TimestampType(), "closed_at": T.TimestampType(),
    "updated_at": T.TimestampType(), "latitude": T.DoubleType(),
    "longitude": T.DoubleType(), "resolution_hours": T.DoubleType(),
    "council_district": T.IntegerType(), "is_valid_resolution": T.BooleanType(),
    "created_date": T.DateType(), "created_day": T.IntegerType(),
}
STATUSES = {"OPEN", "IN_PROGRESS", "PENDING", "CLOSED", "CANCELLED", "REFERRED", "UNKNOWN"}
CHANNELS = {"PHONE", "ONLINE", "MOBILE", "OTHER", "UNKNOWN"}


def check(name, passed):
    """Print one check result and return it so results can be combined."""
    print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    return passed


def main():
    s = spark("validate")
    u = s.read.parquet(UNIFIED)
    n_nyc = s.read.parquet(NYC_CLEAN).count()
    n_la = s.read.parquet(LA_CLEAN).count()

    u.printSchema()
    n = u.count()
    by_src = {r["source"]: r["count"] for r in u.groupBy("source").count().collect()}
    print(f"unified={n:,}  nyc_clean={n_nyc:,}  la_clean={n_la:,}  by_source={by_src}\n")

    def none_where(cond):
        """True when no row matches cond."""
        return u.filter(cond).limit(1).count() == 0

    ok = True
    # schema / types
    for c, t in EXPECTED_TYPES.items():
        ok &= check(f"type {c} is {t.simpleString()}", u.schema[c].dataType == t)
    # record counts
    ok &= check("row count = nyc_clean + la_clean", n == n_nyc + n_la)
    ok &= check("per-source counts match cleaned files", by_src == {"NYC311": n_nyc, "LA311": n_la})
    # identifiers
    ok &= check("request_id unique", u.select("request_id").distinct().count() == n)
    ok &= check("no null request_id / created_at",
                none_where(F.col("request_id").isNull() | F.col("created_at").isNull()))
    # categoricals
    ok &= check("status values in standard set",
                {r[0] for r in u.select("status").distinct().collect()} <= STATUSES)
    ok &= check("channel values in standard set",
                {r[0] for r in u.select("channel").distinct().collect()} <= CHANNELS)
    # derived attributes
    ok &= check("resolution_hours >= 0", none_where(F.col("resolution_hours") < 0))
    ok &= check("resolution_hours set iff is_valid_resolution",
                none_where(F.col("is_valid_resolution") != F.col("resolution_hours").isNotNull()))
    ok &= check("resolution only on CLOSED requests",
                none_where(F.col("is_valid_resolution") & (F.col("status") != "CLOSED")))
    ok &= check("closed_at >= created_at when present",
                none_where(F.col("closed_at") < F.col("created_at")))
    ok &= check("zipcode is 5 digits or null",
                none_where(F.col("zipcode").isNotNull() & ~F.col("zipcode").rlike(r"^\d{5}$")))
    ok &= check("created_* parts match created_at",
                none_where((F.year("created_at") != F.col("created_year"))
                           | (F.month("created_at") != F.col("created_month"))
                           | (F.dayofmonth("created_at") != F.col("created_day"))))

    # breakdowns (for the report)
    u.groupBy("source", "status").count().orderBy("source", "status").show(50, False)
    u.groupBy("source", "channel").count().orderBy("source", "channel").show(50, False)
    u.groupBy("source").agg(
        F.sum(F.col("is_valid_resolution").cast("int")).alias("valid_resolutions"),
        F.round(F.avg("resolution_hours"), 2).alias("avg_resolution_hours"),
        F.min("created_at").alias("first_created"),
        F.max("created_at").alias("last_created"),
    ).show(truncate=False)

    print("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
