"""Align both cleaned datasets to one schema and write the unified dataset."""
from pyspark.sql import functions as F
from common import *

# Unified column -> (NYC expression, LA expression)
MAPPING = {
    "request_id":          ("request_id", "request_id"),
    "source":              ("source", "source"),
    "source_id":           ("unique_key", "srnumber"),
    "city":                ("city", "city"),
    "created_at":          ("created_at", "created_at"),
    "closed_at":           ("closed_at", "closed_at"),
    "updated_at":          ("resolution_action_updated_date", "updateddate"),
    "request_type":        ("complaint_type", "requesttype"),
    "request_subtype":     ("descriptor", None),
    "agency":              ("agency", "owner"),
    "agency_name":         ("agency_name", None),
    "status":              ("status", "status"),
    "status_raw":          ("status_raw", "status_raw"),
    "channel":             ("channel", "channel"),
    "channel_raw":         ("channel_raw", "channel_raw"),
    "address":             ("incident_address", "address"),
    "street_name":         ("street_name", "streetname"),
    "zipcode":             ("incident_zip", "zipcode"),
    "district":            ("borough", "apc"),          # NYC borough / LA Area Planning Commission
    "council_district":    ("council_district", "cd"),
    "police_precinct":     ("police_precinct", "policeprecinct"),
    "latitude":            ("latitude", "latitude"),
    "longitude":           ("longitude", "longitude"),
    "closed_before_created": ("closed_before_created", "closed_before_created"),
    "is_valid_resolution": ("is_valid_resolution", "is_valid_resolution"),
    "resolution_hours":    ("resolution_hours", "resolution_hours"),
    "created_date":        ("created_date", "created_date"),
    "created_year":        ("created_year", "created_year"),
    "created_month":       ("created_month", "created_month"),
    "created_day":         ("created_day", "created_day"),
    "created_hour":        ("created_hour", "created_hour"),
    "created_dow":         ("created_dow", "created_dow"),
}
TYPES = {"council_district": "int", "latitude": "double", "longitude": "double",
         "resolution_hours": "double", "updated_at": "timestamp"}


def align(df, idx):
    cols = []
    for out, src in MAPPING.items():
        e = F.col(src[idx]) if src[idx] else F.lit(None)
        cols.append(e.cast(TYPES.get(out, df.schema[src[idx]].dataType.simpleString()
                                     if src[idx] else "string")).alias(out))
    return df.select(cols)


def main():
    s = spark("merge")
    nyc = align(s.read.parquet(NYC_CLEAN), 0)
    la = align(s.read.parquet(LA_CLEAN), 1)
    unified = nyc.unionByName(la)
    write(unified, UNIFIED)
    print(f"unified rows={s.read.parquet(UNIFIED).count()}")


if __name__ == "__main__":
    main()
