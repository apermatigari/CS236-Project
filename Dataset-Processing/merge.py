"""
merge.py - Align the cleaned NYC and LA datasets to one schema and write output/unified.

How it works
------------
MAPPING lists every unified column with the source column it comes from in each city.
``None`` means the city has no equivalent, and the column is filled with null for that
city. Both cities are projected onto the same columns in the same order and with the same
types, then combined with unionByName (which matches by name, so column order mistakes
can't silently swap values).

Columns that exist in only one city and aren't useful for cross-city analysis (e.g. NYC
due_date, LA mobileos) are left out here but remain in the per-city cleaned files.

Run:  python merge.py   (after clean_nyc.py and clean_la.py)
"""
from pyspark.sql import functions as F

from common import NYC_CLEAN, LA_CLEAN, UNIFIED, spark, write

# unified column -> (NYC cleaned column, LA cleaned column)
MAPPING = {
    # identity / provenance
    "request_id":            ("request_id", "request_id"),     # NYC-<key> / LA-<srnumber>
    "source":                ("source", "source"),             # NYC311 / LA311
    "source_id":             ("unique_key", "srnumber"),       # original identifier
    "city":                  ("city", "city"),
    # time
    "created_at":            ("created_at", "created_at"),
    "closed_at":             ("closed_at", "closed_at"),
    "updated_at":            ("resolution_action_updated_date", "updateddate"),
    # what was requested and who handles it
    "request_type":          ("complaint_type", "requesttype"),
    "request_subtype":       ("descriptor", None),             # LA has no sub-type
    "agency":                ("agency", "owner"),              # short code, e.g. NYPD / LASAN
    "agency_name":           ("agency_name", None),            # LA only has the code
    # status and channel: standardized value + original text
    "status":                ("status", "status"),
    "status_raw":            ("status_raw", "status_raw"),
    "channel":               ("channel", "channel"),
    "channel_raw":           ("channel_raw", "channel_raw"),
    # location
    "address":               ("incident_address", "address"),
    "street_name":           ("street_name", "streetname"),
    "zipcode":               ("incident_zip", "zipcode"),
    "district":              ("borough", "apc"),               # NYC borough / LA planning area
    "council_district":      ("council_district", "cd"),
    "police_precinct":       ("police_precinct", "policeprecinct"),
    "latitude":              ("latitude", "latitude"),
    "longitude":             ("longitude", "longitude"),
    # quality flags and derived attributes
    "closed_before_created": ("closed_before_created", "closed_before_created"),
    "is_valid_resolution":   ("is_valid_resolution", "is_valid_resolution"),
    "resolution_hours":      ("resolution_hours", "resolution_hours"),
    "created_date":          ("created_date", "created_date"),
    "created_year":          ("created_year", "created_year"),
    "created_month":         ("created_month", "created_month"),
    "created_day":           ("created_day", "created_day"),
    "created_hour":          ("created_hour", "created_hour"),
    "created_dow":           ("created_dow", "created_dow"),
}

# Columns whose type must be forced so both sides match exactly. Everything else keeps
# the type it already has in the cleaned files (string unless listed here).
TYPES = {"council_district": "int", "latitude": "double", "longitude": "double",
         "resolution_hours": "double", "updated_at": "timestamp"}


def align(df, side):
    """Project one cleaned dataset onto the unified schema.

    side = 0 for NYC, 1 for LA (index into the MAPPING tuples).
    """
    cols = []
    for out_name, sources in MAPPING.items():
        src = sources[side]
        if src is None:
            # City has no equivalent: null of the right type.
            cols.append(F.lit(None).cast(TYPES.get(out_name, "string")).alias(out_name))
        else:
            target = TYPES.get(out_name, df.schema[src].dataType.simpleString())
            cols.append(F.col(src).cast(target).alias(out_name))
    return df.select(cols)


def main():
    s = spark("merge")
    nyc = align(s.read.parquet(NYC_CLEAN), 0)
    la = align(s.read.parquet(LA_CLEAN), 1)
    write(nyc.unionByName(la), UNIFIED)
    print(f"unified rows = {s.read.parquet(UNIFIED).count():,}")


if __name__ == "__main__":
    main()
