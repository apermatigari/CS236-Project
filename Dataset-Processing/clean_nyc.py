"""Clean NYC 311 and save a cleaned copy (Parquet) with original + standardized columns."""
from pyspark.sql import functions as F
from common import *

STATUS_MAP = {"Closed": "CLOSED", "Open": "OPEN", "In Progress": "IN_PROGRESS",
              "Assigned": "IN_PROGRESS", "Started": "IN_PROGRESS", "Pending": "PENDING"}
CHANNEL_MAP = {"ONLINE": "ONLINE", "PHONE": "PHONE", "MOBILE": "MOBILE", "OTHER": "OTHER"}
NYC_BOX = (40.45, 40.95, -74.30, -73.65)   # bounding box of the five boroughs
NYC_ZIP = r"^1(0[0-4]|1[0-6])\d\d$"        # 100xx-104xx, 110xx-116xx


def main():
    s = spark("clean_nyc")
    raw = read_csv(s, NYC_RAW)
    n_raw = raw.count()

    # 1) Trim / null-normalize every string column (keep raw status/channel for traceability).
    df = raw.select([clean_str(c).alias(c) for c in raw.columns]) \
            .withColumn("status_raw", F.trim("status")) \
            .withColumn("channel_raw", F.trim("open_data_channel_type"))

    # 2) Types.
    for c in ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]:
        df = df.withColumn(c, to_ts(c))
    df = (df.withColumn("latitude", F.col("latitude").cast("double"))
            .withColumn("longitude", F.col("longitude").cast("double"))
            .withColumn("x_coordinate_state_plane", F.col("x_coordinate_state_plane").cast("long"))
            .withColumn("y_coordinate_state_plane", F.col("y_coordinate_state_plane").cast("long"))
            .withColumn("council_district", F.col("council_district").cast("int"))
            .withColumn("incident_zip", clean_zip("incident_zip", NYC_ZIP)))

    # 3) Required fields: drop rows without id or created time.
    df = df.filter(F.col("unique_key").isNotNull() & F.col("created_date").isNotNull())
    n_req = df.count()

    # 4) Duplicates: same id, then identical content under different ids.
    df = df.dropDuplicates(["unique_key"])
    content = [c for c in df.columns if c != "unique_key"]
    df = df.dropDuplicates(content)
    n_dedup = df.count()

    # 5) Invalid coordinates -> null (outside NYC).
    ok = in_box(F.col("latitude"), F.col("longitude"), NYC_BOX)
    df = (df.withColumn("latitude", F.when(ok, F.col("latitude")))
            .withColumn("longitude", F.when(ok, F.col("longitude"))))

    # 6) Temporal: closed before created is invalid -> flag and null closed time.
    df = df.withColumn("closed_before_created", F.col("closed_date") < F.col("created_date")) \
           .withColumn("closed_before_created", F.coalesce("closed_before_created", F.lit(False))) \
           .withColumn("closed_date", F.when(~F.col("closed_before_created"), F.col("closed_date")))

    # 7) Standardized columns used by the unified schema.
    df = (df.withColumn("source", F.lit("NYC311"))
            .withColumn("city", F.lit("New York"))
            .withColumn("request_id", F.concat(F.lit("NYC-"), F.col("unique_key")))
            .withColumn("created_at", F.col("created_date"))
            .withColumn("closed_at", F.col("closed_date"))
            .withColumn("status", mapper("status_raw", STATUS_MAP, "UNKNOWN"))
            .withColumn("channel", mapper("channel_raw", CHANNEL_MAP, "UNKNOWN"))
            .withColumn("borough", F.upper("borough")))
    df = add_derived(df)

    write(df, NYC_CLEAN)
    print(f"NYC raw={n_raw} after_required={n_req} after_dedup={n_dedup} "
          f"closed_before_created={df.filter('closed_before_created').count()}")


if __name__ == "__main__":
    main()
