"""
clean_nyc.py - Clean the NYC 311 dataset and save it as output/nyc_clean (Parquet).

Steps (in order):
  1. Normalize strings: trim, placeholder values -> null. The raw status and channel
     text is copied first (status_raw / channel_raw) so no original value is lost.
  2. Assign types: timestamps, doubles for coordinates, ints for numeric codes.
  3. Drop rows missing the identifier or created time (required for every later step).
  4. Remove duplicates: by unique_key, then rows identical in every other column
     (the same complaint submitted twice under different keys).
  5. Validate location: ZIPs outside NYC's range and coordinates outside NYC -> null.
  6. Temporal consistency: closed_date before created_date -> flagged and nulled.
  7. Add standardized columns (source, city, request_id, status, channel, ...) and the
     derived attributes from common.add_derived.

The output keeps every original NYC column (cleaned and typed) plus the new columns, so
the cleaned file can be reused even for attributes the unified schema leaves out.

Run:  python clean_nyc.py
"""
from pyspark.sql import functions as F

from common import (NYC_RAW, NYC_CLEAN, spark, read_csv, write, clean_str, to_ts,
                    clean_zip, in_box, mapper, add_derived, print_summary)

# NYC status -> standard status. "Assigned" and "Started" mean work has begun, so they
# are grouped with "In Progress". "Unspecified" (and nulls) fall to UNKNOWN.
STATUS_MAP = {"Closed": "CLOSED", "Open": "OPEN", "In Progress": "IN_PROGRESS",
              "Assigned": "IN_PROGRESS", "Started": "IN_PROGRESS", "Pending": "PENDING"}

# NYC's open_data_channel_type is already coarse; it becomes the shared vocabulary.
CHANNEL_MAP = {"ONLINE": "ONLINE", "PHONE": "PHONE", "MOBILE": "MOBILE", "OTHER": "OTHER"}

NYC_BOX = (40.45, 40.95, -74.30, -73.65)  # lat/lon box around the five boroughs
NYC_ZIP = r"^1(0[0-4]|1[0-6])\d\d$"       # NYC ZIPs: 100xx-104xx and 110xx-116xx

TIMESTAMP_COLS = ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]


def main():
    s = spark("clean_nyc")
    raw = read_csv(s, NYC_RAW)
    n_raw = raw.count()

    # 1) Keep the original categorical text, then normalize every string column.
    df = (raw.withColumn("status_raw", F.trim("status"))
             .withColumn("channel_raw", F.trim("open_data_channel_type"))
             .withColumn("zip_raw", F.col("incident_zip")))
    df = df.select([clean_str(c).alias(c) if c not in ("status_raw", "channel_raw") else F.col(c)
                    for c in df.columns])

    # 2) Types. ZIP stays a string (it is a code, not a number).
    for c in TIMESTAMP_COLS:
        df = df.withColumn(c, to_ts(c))
    df = (df.withColumn("latitude", F.col("latitude").cast("double"))
            .withColumn("longitude", F.col("longitude").cast("double"))
            .withColumn("x_coordinate_state_plane", F.col("x_coordinate_state_plane").cast("long"))
            .withColumn("y_coordinate_state_plane", F.col("y_coordinate_state_plane").cast("long"))
            .withColumn("council_district", F.col("council_district").cast("int"))
            .withColumn("incident_zip", clean_zip("incident_zip", NYC_ZIP)))

    # 3) Required fields.
    df = df.filter(F.col("unique_key").isNotNull() & F.col("created_date").isNotNull())
    n_required = df.count()

    # 4) Duplicates. zip_raw is a helper column and is ignored in the content comparison.
    df = df.dropDuplicates(["unique_key"])
    n_after_id_dedup = df.count()
    df = df.dropDuplicates([c for c in df.columns if c not in ("unique_key", "zip_raw")])
    n_after_content_dedup = df.count()

    # 5) Coordinates outside NYC are treated as invalid (set to null, row kept).
    has_coords = F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    coords_ok = in_box(F.col("latitude"), F.col("longitude"), NYC_BOX)
    df = (df.withColumn("_bad_coords", has_coords & ~coords_ok)
            .withColumn("latitude", F.when(coords_ok, F.col("latitude")))
            .withColumn("longitude", F.when(coords_ok, F.col("longitude"))))

    # 6) A request can't close before it was created. The row is kept (the request is
    #    real) but the close time is untrustworthy, so it is nulled and the row flagged.
    df = (df.withColumn("closed_before_created",
                        F.coalesce(F.col("closed_date") < F.col("created_date"), F.lit(False)))
            .withColumn("closed_date",
                        F.when(~F.col("closed_before_created"), F.col("closed_date"))))

    # 7) Standardized / derived columns used by the unified schema.
    df = (df.withColumn("source", F.lit("NYC311"))
            .withColumn("city", F.lit("New York"))
            .withColumn("request_id", F.concat(F.lit("NYC-"), F.col("unique_key")))
            .withColumn("created_at", F.col("created_date"))
            .withColumn("closed_at", F.col("closed_date"))
            .withColumn("status", mapper("status_raw", STATUS_MAP, "UNKNOWN"))
            .withColumn("channel", mapper("channel_raw", CHANNEL_MAP, "UNKNOWN"))
            .withColumn("borough", F.upper("borough")))
    df = add_derived(df).cache()

    # Counts for the cleaning summary, computed before the helper columns are dropped.
    stats = df.agg(
        F.sum((F.col("zip_raw").isNotNull() & F.col("incident_zip").isNull()).cast("int")),
        F.sum(F.col("_bad_coords").cast("int")),
        F.sum(F.col("closed_before_created").cast("int")),
        F.sum(F.col("is_valid_resolution").cast("int")),
    ).first()

    write(df.drop("zip_raw", "_bad_coords"), NYC_CLEAN)
    print_summary("NYC cleaning summary", [
        ("raw rows", n_raw),
        ("rows missing id/created_date (dropped)", n_raw - n_required),
        ("duplicate unique_key (dropped)", n_required - n_after_id_dedup),
        ("duplicate content, different key (dropped)", n_after_id_dedup - n_after_content_dedup),
        ("invalid ZIP -> null", stats[0]),
        ("coordinates outside NYC -> null", stats[1]),
        ("closed before created (flagged, closed->null)", stats[2]),
        ("valid resolutions", stats[3]),
        ("rows written", n_after_content_dedup),
    ])


if __name__ == "__main__":
    main()
