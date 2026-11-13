-- ============================================================================
-- create_tables.sql - PostgreSQL tables for the three processed datasets
-- (CS 236 Phase 2, Section 2.2).
--
--   nyc_clean  - cleaned NYC 311 requests   (one row per NYC unique_key)
--   la_clean   - cleaned LA MyLA311 requests (one row per LA srnumber)
--   unified    - unified service requests    (one row per request_id)
--
-- Column names are the same as the Spark (Parquet) datasets, so Spark can insert
-- by column name. Type choices:
--   * Identifiers / short codes  -> VARCHAR(n), with n above the longest value seen
--   * Free text (addresses, descriptions, names) -> TEXT (no arbitrary length limit)
--   * Spark timestamp -> TIMESTAMP (without time zone). The source data has no
--     time zone; values are each city's local time, stored as recorded.
--   * Spark date -> DATE, double -> DOUBLE PRECISION, boolean -> BOOLEAN
--   * Spark int date parts (year, month, day, hour) -> SMALLINT (small ranges)
--   * Spark int district ids -> INTEGER, Spark bigint state-plane coords -> BIGINT
-- NOT NULL is used for identifiers, created_at, and every column the pipeline
-- always fills (standardized and derived columns). Source attributes that may be
-- missing in the raw data stay nullable. CHECK constraints apply the same rules as
-- Dataset-Processing/validate.py, so invalid rows can't be inserted later
-- (e.g. through the Phase 3 WebUI).
--
-- Run: done automatically by load_to_postgres.py, or by hand with
--   docker exec -i cs236-postgres psql -U cs236 -d cs236 < sql/create_tables.sql
-- WARNING: drops and recreates the tables (all loaded data is removed).
-- ============================================================================

DROP TABLE IF EXISTS unified;
DROP TABLE IF EXISTS nyc_clean;
DROP TABLE IF EXISTS la_clean;

-- ----------------------------------------------------------------------------
-- Cleaned NYC 311: all original NYC columns + standardized/derived columns
-- ----------------------------------------------------------------------------
CREATE TABLE nyc_clean (
    -- identifiers
    unique_key                     VARCHAR(16)  PRIMARY KEY,           -- original NYC id
    request_id                     VARCHAR(24)  NOT NULL UNIQUE,       -- 'NYC-' || unique_key
    source                         VARCHAR(8)   NOT NULL CHECK (source = 'NYC311'),
    city                           VARCHAR(16)  NOT NULL,              -- 'New York'
    -- times
    created_at                     TIMESTAMP    NOT NULL,              -- raw created_date
    closed_at                      TIMESTAMP,                          -- raw closed_date (null if invalid)
    due_date                       TIMESTAMP,
    resolution_action_updated_date TIMESTAMP,
    -- request
    agency                         VARCHAR(10)  NOT NULL,
    agency_name                    TEXT         NOT NULL,
    complaint_type                 TEXT         NOT NULL,
    descriptor                     TEXT,
    descriptor_2                   TEXT,
    location_type                  TEXT,
    resolution_description         TEXT,
    -- location
    incident_zip                   VARCHAR(5)   CHECK (incident_zip ~ '^[0-9]{5}$'),
    incident_address               TEXT,
    street_name                    TEXT,
    cross_street_1                 TEXT,
    cross_street_2                 TEXT,
    intersection_street_1          TEXT,
    intersection_street_2          TEXT,
    address_type                   VARCHAR(20),
    incident_city                  TEXT,                               -- raw NYC "city" (e.g. BROOKLYN)
    landmark                       TEXT,
    facility_type                  TEXT,
    community_board                TEXT,
    council_district               INTEGER,
    police_precinct                VARCHAR(20),
    bbl                            VARCHAR(10),
    borough                        VARCHAR(16),
    x_coordinate_state_plane       BIGINT,
    y_coordinate_state_plane       BIGINT,
    latitude                       DOUBLE PRECISION CHECK (latitude  BETWEEN 40.45 AND 40.95),
    longitude                      DOUBLE PRECISION CHECK (longitude BETWEEN -74.30 AND -73.65),
    location                       TEXT,
    -- other NYC-only attributes
    open_data_channel_type         VARCHAR(10),
    park_facility_name             TEXT,
    park_borough                   VARCHAR(16),
    vehicle_type                   TEXT,
    taxi_company_borough           VARCHAR(16),
    taxi_pick_up_location          TEXT,
    bridge_highway_name            TEXT,
    bridge_highway_direction       TEXT,
    road_ramp                      TEXT,
    bridge_highway_segment         TEXT,
    -- standardized categories (+ original text)
    status                         VARCHAR(12)  NOT NULL
        CHECK (status IN ('OPEN','IN_PROGRESS','PENDING','CLOSED','CANCELLED','REFERRED','UNKNOWN')),
    status_raw                     TEXT,
    channel                        VARCHAR(8)   NOT NULL
        CHECK (channel IN ('PHONE','ONLINE','MOBILE','OTHER','UNKNOWN')),
    channel_raw                    TEXT,
    -- quality flags and derived attributes
    closed_before_created          BOOLEAN      NOT NULL,
    is_valid_resolution            BOOLEAN      NOT NULL,
    resolution_hours               DOUBLE PRECISION CHECK (resolution_hours >= 0),
    created_date                   DATE         NOT NULL,
    created_year                   SMALLINT     NOT NULL,
    created_month                  SMALLINT     NOT NULL CHECK (created_month BETWEEN 1 AND 12),
    created_day                    SMALLINT     NOT NULL CHECK (created_day BETWEEN 1 AND 31),
    created_hour                   SMALLINT     NOT NULL CHECK (created_hour BETWEEN 0 AND 23),
    created_dow                    VARCHAR(3)   NOT NULL,
    -- cross-column rules
    CHECK (closed_at IS NULL OR closed_at >= created_at),
    CHECK (is_valid_resolution = (resolution_hours IS NOT NULL))
);

-- ----------------------------------------------------------------------------
-- Cleaned LA MyLA311: all original LA columns + standardized/derived columns
-- ----------------------------------------------------------------------------
CREATE TABLE la_clean (
    -- identifiers
    srnumber                       VARCHAR(16)  PRIMARY KEY,           -- original LA id
    request_id                     VARCHAR(24)  NOT NULL UNIQUE,       -- 'LA-' || srnumber
    source                         VARCHAR(8)   NOT NULL CHECK (source = 'LA311'),
    city                           VARCHAR(16)  NOT NULL,              -- 'Los Angeles'
    -- times
    created_at                     TIMESTAMP    NOT NULL,              -- raw createddate
    closed_at                      TIMESTAMP,                          -- raw closeddate (null if invalid)
    updateddate                    TIMESTAMP,
    servicedate                    TIMESTAMP,
    -- request
    actiontaken                    TEXT,
    owner                          VARCHAR(10)  NOT NULL,              -- agency code
    requesttype                    TEXT         NOT NULL,
    requestsource                  TEXT,
    createdbyuserorganization      TEXT,
    mobileos                       VARCHAR(16),
    anonymous                      BOOLEAN,
    assignto                       TEXT,
    -- location
    address                        TEXT,
    housenumber                    VARCHAR(10),
    direction                      VARCHAR(2),
    streetname                     TEXT,
    suffix                         VARCHAR(10),
    zipcode                        VARCHAR(5)   CHECK (zipcode ~ '^[0-9]{5}$'),
    addressverified                BOOLEAN,
    approximateaddress             BOOLEAN,
    latitude                       DOUBLE PRECISION CHECK (latitude  BETWEEN 33.65 AND 34.40),
    longitude                      DOUBLE PRECISION CHECK (longitude BETWEEN -118.70 AND -118.10),
    location                       TEXT,
    tbmpage                        VARCHAR(8),                         -- Thomas Bros. map grid
    tbmcolumn                      VARCHAR(2),
    tbmrow                         VARCHAR(2),
    apc                            TEXT,                               -- Area Planning Commission
    cd                             INTEGER,                            -- council district
    cdmember                       TEXT,
    nc                             INTEGER,                            -- neighborhood council id
    ncname                         TEXT,
    policeprecinct                 VARCHAR(20),
    -- standardized categories (+ original text)
    status                         VARCHAR(12)  NOT NULL
        CHECK (status IN ('OPEN','IN_PROGRESS','PENDING','CLOSED','CANCELLED','REFERRED','UNKNOWN')),
    status_raw                     TEXT,
    channel                        VARCHAR(8)   NOT NULL
        CHECK (channel IN ('PHONE','ONLINE','MOBILE','OTHER','UNKNOWN')),
    channel_raw                    TEXT,
    -- quality flags and derived attributes
    closed_before_created          BOOLEAN      NOT NULL,
    is_valid_resolution            BOOLEAN      NOT NULL,
    resolution_hours               DOUBLE PRECISION CHECK (resolution_hours >= 0),
    created_date                   DATE         NOT NULL,
    created_year                   SMALLINT     NOT NULL,
    created_month                  SMALLINT     NOT NULL CHECK (created_month BETWEEN 1 AND 12),
    created_day                    SMALLINT     NOT NULL CHECK (created_day BETWEEN 1 AND 31),
    created_hour                   SMALLINT     NOT NULL CHECK (created_hour BETWEEN 0 AND 23),
    created_dow                    VARCHAR(3)   NOT NULL,
    CHECK (closed_at IS NULL OR closed_at >= created_at),
    CHECK (is_valid_resolution = (resolution_hours IS NOT NULL))
);

-- ----------------------------------------------------------------------------
-- Unified service requests (both cities, shared schema from merge.py)
-- ----------------------------------------------------------------------------
CREATE TABLE unified (
    request_id             VARCHAR(24)  PRIMARY KEY,                  -- NYC-<id> / LA-<id>
    source                 VARCHAR(8)   NOT NULL CHECK (source IN ('NYC311','LA311')),
    source_id              VARCHAR(16)  NOT NULL,                     -- id in the source system
    city                   VARCHAR(16)  NOT NULL,
    created_at             TIMESTAMP    NOT NULL,
    closed_at              TIMESTAMP,
    updated_at             TIMESTAMP,
    request_type           TEXT         NOT NULL,
    request_subtype        TEXT,                                      -- NYC only
    agency                 VARCHAR(10)  NOT NULL,
    agency_name            TEXT,                                      -- NYC only
    status                 VARCHAR(12)  NOT NULL
        CHECK (status IN ('OPEN','IN_PROGRESS','PENDING','CLOSED','CANCELLED','REFERRED','UNKNOWN')),
    status_raw             TEXT,
    channel                VARCHAR(8)   NOT NULL
        CHECK (channel IN ('PHONE','ONLINE','MOBILE','OTHER','UNKNOWN')),
    channel_raw            TEXT,
    address                TEXT,
    street_name            TEXT,
    zipcode                VARCHAR(5)   CHECK (zipcode ~ '^[0-9]{5}$'),
    district               TEXT,                                      -- NYC borough / LA APC
    council_district       INTEGER,
    police_precinct        VARCHAR(20),
    latitude               DOUBLE PRECISION CHECK (latitude  BETWEEN -90  AND 90),
    longitude              DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),
    closed_before_created  BOOLEAN      NOT NULL,
    is_valid_resolution    BOOLEAN      NOT NULL,
    resolution_hours       DOUBLE PRECISION CHECK (resolution_hours >= 0),
    created_date           DATE         NOT NULL,
    created_year           SMALLINT     NOT NULL,
    created_month          SMALLINT     NOT NULL CHECK (created_month BETWEEN 1 AND 12),
    created_day            SMALLINT     NOT NULL CHECK (created_day BETWEEN 1 AND 31),
    created_hour           SMALLINT     NOT NULL CHECK (created_hour BETWEEN 0 AND 23),
    created_dow            VARCHAR(3)   NOT NULL,
    -- the same source record can't appear twice
    UNIQUE (source, source_id),
    CHECK (closed_at IS NULL OR closed_at >= created_at),
    CHECK (is_valid_resolution = (resolution_hours IS NOT NULL))
);
