# Dataset Processing (Section 1.3)

PySpark pipeline that cleans the NYC 311 and LA MyLA311 datasets, saves a cleaned copy of each,
and merges them into one unified dataset.

## Environment

- Python 3.11 venv at `../.venv` with `pyspark==3.5.1` (`requirements.txt`)
- OpenJDK 17 (Temurin) unpacked at `../.jdk/` (Spark needs Java; installed locally, no sudo)
- `setup_env.sh` recreates both. Docker is **not** installed yet (needs sudo; see 1.1).

Run everything: `./run_all.sh` (takes a few minutes on 8 cores).

## Files

| File | Purpose |
|---|---|
| `common.py` | Spark session, paths, shared cleaning helpers, derived-feature logic |
| `clean_nyc.py` | Cleans NYC → `output/nyc_clean/` |
| `clean_la.py` | Cleans LA → `output/la_clean/` |
| `merge.py` | Maps both to the unified schema → `output/unified/` |
| `validate.py` | Checks schema, types, counts, ID uniqueness, derived attributes |
| `export_csv.py` | Exports outputs to `output/csv/*.csv` (`--sample N`, `--only unified`) |

All outputs are Parquet, so the column types are kept.

## 1. Clean the datasets

| Issue | Fix | Why |
|---|---|---|
| Whitespace, placeholder values (`""`, `N/A`, `Unspecified`, `Unknown`) | Trimmed, converted to null | So missing values are handled the same way everywhere |
| Everything is read as strings | Timestamps parsed; lat/long → double; district IDs → int; LA `Y/N` flags → boolean | Gives correct types for date math and the database later |
| Missing required fields | Rows with no ID or no created date are dropped (none found) | A request can't be identified or placed in time without these |
| Duplicates | Duplicates by ID removed, then rows identical in every column except the ID removed. **NYC: 261 removed**, LA: 0 | The same complaint submitted more than once would inflate counts |
| Invalid ZIP codes (`0`, `VE, 0`, `No`) | Kept only 5-digit ZIPs in the city's range (NYC 100xx–116xx, LA 900xx–918xx), others set to null | Bad ZIPs would show up as fake hotspots |
| Coordinates outside the city's area | Set to null (bounding-box check) | Prevents bad geographic data |
| Temporal: closed before created (**NYC: 626**, LA: 0) | Flagged in `closed_before_created`, `closed_at` set to null, row kept | The close time can't be trusted but the request itself is still valid |

## 2. Adjust the schema (unified columns)

| Unified | NYC source | LA source |
|---|---|---|
| `source_id` | `unique_key` | `srnumber` |
| `created_at` / `closed_at` / `updated_at` | `created_date` / `closed_date` / `resolution_action_updated_date` | `createddate` / `closeddate` / `updateddate` |
| `request_type` | `complaint_type` | `requesttype` |
| `request_subtype` | `descriptor` | — (null) |
| `agency` / `agency_name` | `agency` / `agency_name` | `owner` / — |
| `address`, `street_name` | `incident_address`, `street_name` | `address`, `streetname` |
| `zipcode` | `incident_zip` | `zipcode` |
| `district` | `borough` | `apc` (Area Planning Commission) |
| `council_district` | `council_district` | `cd` |
| `police_precinct` | `police_precinct` | `policeprecinct` |
| `latitude`, `longitude` | same | same |

Columns that exist in only one source (e.g. NYC `due_date` or LA `mobileos`) are left out of the
unified table but are still in the per-city cleaned files.

## 3. Standardize values

The original value is kept next to each standardized one (`status_raw`, `channel_raw`), so nothing is lost.

**Status**: `CLOSED`, `OPEN`, `IN_PROGRESS` (NYC In Progress/Assigned/Started), `PENDING`,
`CANCELLED` (LA), `REFERRED` (LA Referred Out/Forward), `UNKNOWN` (NYC Unspecified).

**Channel** (uses NYC's channel names): `PHONE` (LA Call/Voicemail/Radio/Queue call), `ONLINE`
(LA Self Service/Email/Web Form/Social), `MOBILE` (LA Mobile App), `OTHER` (LA Driver Self Report,
Council's Office, Fax, Walk-in, Letter, City Attorney), `UNKNOWN`.

Request types are kept as-is. The two cities use completely different lists, so mapping them to
shared categories would lose detail.

## 4. Derived attributes

- `source` (`NYC311` / `LA311`) and `city`
- `request_id` = `NYC-<unique_key>` / `LA-<srnumber>`, which stays unique across both cities
- `resolution_hours`: (closed_at − created_at) in hours, set only when status is CLOSED and the close time is valid
- `is_valid_resolution`: true when `resolution_hours` is set
- `created_date`, `created_year`, `created_month`, `created_day`, `created_hour`, `created_dow`
  (`created_day` is used for the weekly-period analysis in Phase 2)

## 5–6. Persist and merge

Cleaned per-city files: `output/nyc_clean/` (799,739 rows), `output/la_clean/` (800,000 rows).
Each keeps all original columns plus the standardized ones. `merge.py` puts both into the same
column order and types and combines them with `unionByName` → `output/unified/` (1,599,739 rows).

## 7. Validation (`validate.py`, all 19 checks pass)

Expected column types; unified row count = NYC + LA, and per-source counts match; `request_id` unique
and never null; `created_at` never null; status/channel values come only from the standard lists;
`resolution_hours` ≥ 0 and set only on valid resolutions; no `closed_at` before `created_at`; ZIPs are
5 digits or null; date parts match `created_at`.

Valid resolutions: NYC 703,951, LA 728,135. About 2k LA requests are marked Closed but have no
close date, so they get no resolution time.
