-- ============================================================================
-- create_indexes.sql - Secondary indexes, created AFTER the bulk load
-- (CS 236 Phase 2, Section 2.2).
--
-- Building indexes once after loading is much faster than updating them for
-- every inserted row. Primary keys and UNIQUE constraints already have their own
-- indexes (from create_tables.sql).
--
-- The indexes target the columns the Phase 2 analyses and Phase 3 WebUI filter
-- and group on: city + time, city + postal code, city + request type, status.
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_unified_source_created ON unified (source, created_at);
CREATE INDEX IF NOT EXISTS idx_unified_source_zip     ON unified (source, zipcode);
CREATE INDEX IF NOT EXISTS idx_unified_source_type    ON unified (source, request_type);
CREATE INDEX IF NOT EXISTS idx_unified_status         ON unified (status);

CREATE INDEX IF NOT EXISTS idx_nyc_created            ON nyc_clean (created_at);
CREATE INDEX IF NOT EXISTS idx_nyc_zip                ON nyc_clean (incident_zip);
CREATE INDEX IF NOT EXISTS idx_la_created             ON la_clean (created_at);
CREATE INDEX IF NOT EXISTS idx_la_zip                 ON la_clean (zipcode);

-- refresh planner statistics after the bulk load
ANALYZE nyc_clean;
ANALYZE la_clean;
ANALYZE unified;
