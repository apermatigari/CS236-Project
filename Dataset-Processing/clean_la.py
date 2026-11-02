"""Clean LA MyLA311 and save a cleaned copy (Parquet) with original + standardized columns."""
from pyspark.sql import functions as F
from common import *

STATUS_MAP = {"Closed": "CLOSED", "Open": "OPEN", "Pending": "PENDING",
              "Cancelled": "CANCELLED", "Referred Out": "REFERRED", "Forward": "REFERRED"}
# LA has many fine-grained sources; collapse to the NYC-style channel vocabulary.
CHANNEL_MAP = {"Call": "PHONE", "Voicemail": "PHONE", "Queue Initiated Customer Call": "PHONE",
               "Radio": "PHONE", "Mobile App": "MOBILE", "Self Service": "ONLINE",
               "Email": "ONLINE", "Web Form": "ONLINE", "Social": "ONLINE",
               "Driver Self Report": "OTHER", "Council's Office": "OTHER", "Fax": "OTHER",
               "Walk-in": "OTHER", "Letter": "OTHER", "City Attorney": "OTHER"}
LA_BOX = (33.65, 34.40, -118.70, -118.10)  # bounding box of the City of LA
LA_ZIP = r"^9[01]\d{3}$"                   # 900xx-918xx


def main():
    s = spark("clean_la")
    raw = read_csv(s, LA_RAW)
    n_raw = raw.count()

    df = raw.select([clean_str(c).alias(c) for c in raw.columns]) \
            .withColumn("status_raw", F.trim("status")) \
            .withColumn("channel_raw", F.trim("requestsource"))

    # Types. Y/N flags -> boolean.
    for c in ["createddate", "updateddate", "servicedate", "closeddate"]:
        df = df.withColumn(c, to_ts(c))
    for c in ["anonymous", "addressverified", "approximateaddress"]:
        df = df.withColumn(c, F.when(F.col(c) == "Y", True).when(F.col(c) == "N", False))
    df = (df.withColumn("latitude", F.col("latitude").cast("double"))
            .withColumn("longitude", F.col("longitude").cast("double"))
            .withColumn("cd", F.col("cd").cast("int"))
            .withColumn("nc", F.col("nc").cast("int"))
            .withColumn("zipcode", clean_zip("zipcode", LA_ZIP)))

    df = df.filter(F.col("srnumber").isNotNull() & F.col("createddate").isNotNull())
    n_req = df.count()

    df = df.dropDuplicates(["srnumber"])
    df = df.dropDuplicates([c for c in df.columns if c != "srnumber"])
    n_dedup = df.count()

    ok = in_box(F.col("latitude"), F.col("longitude"), LA_BOX)
    df = (df.withColumn("latitude", F.when(ok, F.col("latitude")))
            .withColumn("longitude", F.when(ok, F.col("longitude"))))

    df = df.withColumn("closed_before_created",
                       F.coalesce(F.col("closeddate") < F.col("createddate"), F.lit(False))) \
           .withColumn("closeddate", F.when(~F.col("closed_before_created"), F.col("closeddate")))

    df = (df.withColumn("source", F.lit("LA311"))
            .withColumn("city", F.lit("Los Angeles"))
            .withColumn("request_id", F.concat(F.lit("LA-"), F.col("srnumber")))
            .withColumn("created_at", F.col("createddate"))
            .withColumn("closed_at", F.col("closeddate"))
            .withColumn("status", mapper("status_raw", STATUS_MAP, "UNKNOWN"))
            .withColumn("channel", mapper("channel_raw", CHANNEL_MAP, "UNKNOWN")))
    df = add_derived(df)

    write(df, LA_CLEAN)
    print(f"LA raw={n_raw} after_required={n_req} after_dedup={n_dedup} "
          f"closed_before_created={df.filter('closed_before_created').count()}")


if __name__ == "__main__":
    main()
