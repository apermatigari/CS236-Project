"""
clean_la.py - Clean the LA MyLA311 dataset and save it as output/la_clean (Parquet).

Same steps as clean_nyc.py, adapted to LA's columns:
  1. Normalize strings (raw status/source text kept in status_raw / channel_raw).
  2. Assign types: timestamps, Y/N flags -> boolean, coordinates -> double, codes -> int.
  3. Drop rows missing srnumber or createddate.
  4. Remove duplicates (by srnumber, then identical content under a different number).
  5. Validate location: ZIPs outside LA's range and coordinates outside LA -> null.
  6. Temporal consistency: closeddate before createddate -> flagged and nulled.
  7. Add standardized columns and derived attributes.

The output keeps every original LA column plus the new columns.

Run:  python clean_la.py
"""
from pyspark.sql import functions as F

from common import (LA_RAW, LA_CLEAN, spark, read_csv, write, clean_str, to_ts,
                    clean_zip, in_box, mapper, add_derived, print_summary)

# LA status -> standard status. "Referred Out" and "Forward" both mean the request was
# passed to another agency, so they share REFERRED. CANCELLED has no NYC equivalent but
# is kept as its own value because merging it into CLOSED would make cancelled requests
# look resolved.
STATUS_MAP = {"Closed": "CLOSED", "Open": "OPEN", "Pending": "PENDING",
              "Cancelled": "CANCELLED", "Referred Out": "REFERRED", "Forward": "REFERRED"}

# LA's requestsource has 15 fine-grained values; they are collapsed into NYC's coarser
# channel names so both cities can be compared. The original is kept in channel_raw.
CHANNEL_MAP = {
    # spoken / phone-based
    "Call": "PHONE", "Voicemail": "PHONE", "Queue Initiated Customer Call": "PHONE",
    "Radio": "PHONE",
    # app
    "Mobile App": "MOBILE",
    # web / electronic
    "Self Service": "ONLINE", "Email": "ONLINE", "Web Form": "ONLINE", "Social": "ONLINE",
    # internal or rare channels with no NYC equivalent
    "Driver Self Report": "OTHER", "Council's Office": "OTHER", "Fax": "OTHER",
    "Walk-in": "OTHER", "Letter": "OTHER", "City Attorney": "OTHER",
}

LA_BOX = (33.65, 34.40, -118.70, -118.10)  # lat/lon box around the City of Los Angeles
LA_ZIP = r"^9[01]\d{3}$"                  # LA-area ZIPs: 900xx-918xx

TIMESTAMP_COLS = ["createddate", "updateddate", "servicedate", "closeddate"]
YN_COLS = ["anonymous", "addressverified", "approximateaddress"]


def main():
    s = spark("clean_la")
    raw = read_csv(s, LA_RAW)
    n_raw = raw.count()

    # 1) Keep the original categorical text, then normalize every string column.
    df = (raw.withColumn("status_raw", F.trim("status"))
             .withColumn("channel_raw", F.trim("requestsource"))
             .withColumn("zip_raw", F.col("zipcode")))
    df = df.select([clean_str(c).alias(c) if c not in ("status_raw", "channel_raw") else F.col(c)
                    for c in df.columns])

    # 2) Types.
    for c in TIMESTAMP_COLS:
        df = df.withColumn(c, to_ts(c))
    for c in YN_COLS:  # "Y"/"N" text -> True/False; anything else -> null
        df = df.withColumn(c, F.when(F.col(c) == "Y", True).when(F.col(c) == "N", False))
    df = (df.withColumn("latitude", F.col("latitude").cast("double"))
            .withColumn("longitude", F.col("longitude").cast("double"))
            .withColumn("cd", F.col("cd").cast("int"))   # council district
            .withColumn("nc", F.col("nc").cast("int"))   # neighborhood council id
            .withColumn("zipcode", clean_zip("zipcode", LA_ZIP)))

    # 3) Required fields.
    df = df.filter(F.col("srnumber").isNotNull() & F.col("createddate").isNotNull())
    n_required = df.count()

    # 4) Duplicates.
    df = df.dropDuplicates(["srnumber"])
    n_after_id_dedup = df.count()
    df = df.dropDuplicates([c for c in df.columns if c not in ("srnumber", "zip_raw")])
    n_after_content_dedup = df.count()

    # 5) Coordinates outside LA -> null.
    has_coords = F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    coords_ok = in_box(F.col("latitude"), F.col("longitude"), LA_BOX)
    df = (df.withColumn("_bad_coords", has_coords & ~coords_ok)
            .withColumn("latitude", F.when(coords_ok, F.col("latitude")))
            .withColumn("longitude", F.when(coords_ok, F.col("longitude"))))

    # 6) Close time before create time -> flag and null the close time.
    df = (df.withColumn("closed_before_created",
                        F.coalesce(F.col("closeddate") < F.col("createddate"), F.lit(False)))
            .withColumn("closeddate",
                        F.when(~F.col("closed_before_created"), F.col("closeddate"))))

    # 7) Standardized / derived columns.
    df = (df.withColumn("source", F.lit("LA311"))
            .withColumn("city", F.lit("Los Angeles"))
            .withColumn("request_id", F.concat(F.lit("LA-"), F.col("srnumber")))
            .withColumn("created_at", F.col("createddate"))
            .withColumn("closed_at", F.col("closeddate"))
            .withColumn("status", mapper("status_raw", STATUS_MAP, "UNKNOWN"))
            .withColumn("channel", mapper("channel_raw", CHANNEL_MAP, "UNKNOWN")))
    df = add_derived(df).cache()

    stats = df.agg(
        F.sum((F.col("zip_raw").isNotNull() & F.col("zipcode").isNull()).cast("int")),
        F.sum(F.col("_bad_coords").cast("int")),
        F.sum(F.col("closed_before_created").cast("int")),
        F.sum(F.col("is_valid_resolution").cast("int")),
        F.sum(((F.col("status") == "CLOSED") & F.col("closed_at").isNull()).cast("int")),
    ).first()

    write(df.drop("zip_raw", "_bad_coords"), LA_CLEAN)
    print_summary("LA cleaning summary", [
        ("raw rows", n_raw),
        ("rows missing id/createddate (dropped)", n_raw - n_required),
        ("duplicate srnumber (dropped)", n_required - n_after_id_dedup),
        ("duplicate content, different id (dropped)", n_after_id_dedup - n_after_content_dedup),
        ("invalid ZIP -> null", stats[0]),
        ("coordinates outside LA -> null", stats[1]),
        ("closed before created (flagged, closed->null)", stats[2]),
        ("status Closed but no close date", stats[4]),
        ("valid resolutions", stats[3]),
        ("rows written", n_after_content_dedup),
    ])


if __name__ == "__main__":
    main()
