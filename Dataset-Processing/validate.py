"""Validate the unified dataset: schema, types, counts, id uniqueness, derived attributes."""
from pyspark.sql import functions as F, types as T
from common import *

EXPECTED_TYPES = {"request_id": T.StringType(), "created_at": T.TimestampType(),
                  "closed_at": T.TimestampType(), "latitude": T.DoubleType(),
                  "longitude": T.DoubleType(), "resolution_hours": T.DoubleType(),
                  "council_district": T.IntegerType(), "is_valid_resolution": T.BooleanType()}
STATUSES = {"OPEN", "IN_PROGRESS", "PENDING", "CLOSED", "CANCELLED", "REFERRED", "UNKNOWN"}
CHANNELS = {"PHONE", "ONLINE", "MOBILE", "OTHER", "UNKNOWN"}


def check(name, cond):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}")
    return cond


def main():
    s = spark("validate")
    u, nyc, la = (s.read.parquet(p) for p in (UNIFIED, NYC_CLEAN, LA_CLEAN))
    u.printSchema()
    n, n_nyc, n_la = u.count(), nyc.count(), la.count()
    by_src = {r["source"]: r["count"] for r in u.groupBy("source").count().collect()}
    print(f"unified={n} nyc_clean={n_nyc} la_clean={n_la} by_source={by_src}")

    ok = True
    for c, t in EXPECTED_TYPES.items():
        ok &= check(f"type {c} is {t.simpleString()}", u.schema[c].dataType == t)
    ok &= check("row count = nyc + la", n == n_nyc + n_la)
    ok &= check("per-source counts match", by_src == {"NYC311": n_nyc, "LA311": n_la})
    ok &= check("request_id unique", u.select("request_id").distinct().count() == n)
    ok &= check("no null request_id/created_at",
                u.filter(F.col("request_id").isNull() | F.col("created_at").isNull()).count() == 0)
    ok &= check("status in standard set",
                {r[0] for r in u.select("status").distinct().collect()} <= STATUSES)
    ok &= check("channel in standard set",
                {r[0] for r in u.select("channel").distinct().collect()} <= CHANNELS)
    ok &= check("resolution_hours >= 0",
                u.filter(F.col("resolution_hours") < 0).count() == 0)
    ok &= check("resolution_hours present iff valid resolution",
                u.filter(F.col("is_valid_resolution") != F.col("resolution_hours").isNotNull()).count() == 0)
    ok &= check("closed_at >= created_at when present",
                u.filter(F.col("closed_at") < F.col("created_at")).count() == 0)
    ok &= check("zipcode is 5 digits or null",
                u.filter(F.col("zipcode").isNotNull() & ~F.col("zipcode").rlike(r"^\d{5}$")).count() == 0)
    ok &= check("created_* parts consistent",
                u.filter(F.year("created_at") != F.col("created_year")).count() == 0)

    u.groupBy("source", "status").count().orderBy("source", "status").show(50, False)
    u.groupBy("source", "channel").count().orderBy("source", "channel").show(50, False)
    u.groupBy("source").agg(F.sum(F.col("is_valid_resolution").cast("int")).alias("valid_resolutions"),
                            F.round(F.avg("resolution_hours"), 2).alias("avg_res_hours")).show()
    print("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED")


if __name__ == "__main__":
    main()
