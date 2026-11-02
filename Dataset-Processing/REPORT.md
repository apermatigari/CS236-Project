# CS 236 Course Project – Phase 1 Report: Dataset Processing

**Scope.** This report covers Section 1.3 (Dataset Processing) of Phase 1. It describes the
data-quality issues and cleaning decisions, the schema mapping and standardization decisions,
and how the unified dataset was built and validated. Exploratory data analysis (Section 1.2) is
documented separately.

All processing uses PySpark 3.5.1. The code is in `Dataset-Processing/`, and the numbers below
come from `./run_all.sh` (full log in `output/run_log.txt`).

---

## 1. Input data

| | NYC 311 | LA MyLA311 |
|---|---|---|
| File | `nyc_311_800k.csv` | `la_311_800k.csv` |
| Rows | 800,000 | 800,000 |
| Columns | 44 | 34 |
| Identifier | `unique_key` | `srnumber` |
| Created-date range | 2026-07-22 → 2026-10-05 | 2024-01-01 → 2024-07-24 |

The raw files aren't in the git repo. They come from UCR eLearn
(https://elearn.ucr.edu/courses/238702/files/26557259/download?download_frd=1) and go in
`CS236-Project/datasets/` (see `datasets/README.md`).

Both files are read with every column as text (`inferSchema=False`). Types are then set
explicitly during cleaning. This prevents wrong guesses, such as ZIP codes read as integers.

## 2. Pipeline overview

```
nyc_311_800k.csv ──► clean_nyc.py ──► output/nyc_clean/ ─┐
                                                          ├─► merge.py ──► output/unified/ ──► validate.py
la_311_800k.csv  ──► clean_la.py  ──► output/la_clean/  ─┘
```

Both cleaning scripts follow the same seven steps. The shared rules live in `common.py`, so
both cities are treated the same way:

1. Normalize strings
2. Assign types
3. Check required fields
4. Remove duplicates
5. Validate location
6. Check temporal consistency
7. Standardize values and add derived attributes

## 3. Data-quality issues and cleaning decisions

| # | Issue | Decision | Justification | NYC | LA |
|---|---|---|---|---|---|
| 1 | Extra whitespace and placeholder text (`""`, `N/A`, `NULL`, `Unspecified`, `Unknown`) | Trim every string column and convert placeholders to null | Missing data is then handled the same way in every column and both cities; otherwise `"Unspecified"` would be counted as a real category | all columns | all columns |
| 2 | All columns are text | Timestamps parsed (`yyyy-MM-dd'T'HH:mm:ss.SSS`); latitude/longitude → double; council district, LA `cd`/`nc` → int; LA `Y/N` flags → boolean; ZIP kept as a string | Correct types are needed for date math, numeric filters and the PostgreSQL tables in Phase 2. ZIP is a code, not a number | 4 timestamp cols | 4 timestamp cols, 3 flags |
| 3 | Missing identifier or created time | Drop the row | A request can't be uniquely identified, or placed in time for any analysis, without these | 0 dropped | 0 dropped |
| 4 | Duplicate identifiers | Keep one row per ID | Each ID must be unique | 0 | 0 |
| 5 | Same complaint stored twice: identical in every column except the ID | Keep one copy | Repeated submissions would inflate request counts and hotspot rankings | **261 dropped** | 0 |
| 6 | Invalid ZIP codes (e.g. `0`, `VE, 0`, `No`, or out of the city's range) | Keep a ZIP only if its first 5 digits are in the city's range (NYC 100xx–104xx, 110xx–116xx; LA 900xx–918xx); otherwise null. The row is kept | Phase 2 ranks postal codes; bad ZIPs would appear as fake hotspots. The rest of the record is still valid | **29 → null** | **39 → null** |
| 7 | Coordinates outside the city | Bounding-box check per city; values outside → null | Protects later map or location queries | 0 | 0 |
| 8 | **Temporal:** close time earlier than created time | Flag the row (`closed_before_created = true`), set the close time to null, keep the row | The request itself is real, but its close time can't be trusted. Keeping the row preserves request counts; nulling the close time keeps it out of resolution statistics | **626 flagged** | 0 |
| 9 | **Temporal:** status is Closed but there is no close time | Keep as is; such rows are marked `is_valid_resolution = false` | No resolution time can be computed, but the status is still informative | 0 | **1,991** |

Rows are only dropped for missing required fields or duplicates. All other problems null the
single bad value and keep the row, so no request is lost because of one bad field.

**Rows after cleaning:** NYC 800,000 → **799,739**; LA 800,000 → **800,000**.

## 4. Schema adjustment (mapping)

The two sources name and structure the same information differently. Overlapping attributes
are mapped to one set of unified names (`merge.py`, `MAPPING`):

| Unified column | Type | NYC source | LA source | Notes |
|---|---|---|---|---|
| `request_id` | string | `NYC-` + `unique_key` | `LA-` + `srnumber` | New; unique across both cities |
| `source` | string | `NYC311` | `LA311` | New |
| `source_id` | string | `unique_key` | `srnumber` | Original ID kept for traceability |
| `city` | string | New York | Los Angeles | New |
| `created_at` | timestamp | `created_date` | `createddate` | |
| `closed_at` | timestamp | `closed_date` | `closeddate` | Null if invalid (see §3 #8) |
| `updated_at` | timestamp | `resolution_action_updated_date` | `updateddate` | |
| `request_type` | string | `complaint_type` | `requesttype` | Values kept as is (see §5) |
| `request_subtype` | string | `descriptor` | — | LA has no sub-type |
| `agency` | string | `agency` | `owner` | Both are short agency codes |
| `agency_name` | string | `agency_name` | — | LA gives only the code |
| `status` / `status_raw` | string | `status` | `status` | Standardized + original |
| `channel` / `channel_raw` | string | `open_data_channel_type` | `requestsource` | Standardized + original |
| `address` | string | `incident_address` | `address` | |
| `street_name` | string | `street_name` | `streetname` | |
| `zipcode` | string | `incident_zip` | `zipcode` | Validated 5-digit ZIP |
| `district` | string | `borough` | `apc` | Top-level area: NYC borough / LA Area Planning Commission |
| `council_district` | int | `council_district` | `cd` | |
| `police_precinct` | string | `police_precinct` | `policeprecinct` | |
| `latitude`, `longitude` | double | same | same | |

**Type mismatches resolved:** both cities' timestamps become Spark `timestamp`. Coordinates
become `double` and council districts `int`. When a column exists in only one city, the other
city gets a typed null.

**Attributes in only one source:** for example NYC `due_date`, `bbl`, `community_board`,
`park_facility_name`, and LA `mobileos`, `anonymous`, `ncname`, `tbmpage`. These are left out
of the unified table because they have no counterpart in the other city. They are still in
the per-city cleaned files, so nothing is lost.

## 5. Value standardization

The original text is always stored next to the standardized value (`status_raw`,
`channel_raw`). Analyses can use the shared categories without losing the source detail.

### Status

| Standard | NYC values | LA values | Rationale |
|---|---|---|---|
| `CLOSED` | Closed | Closed | |
| `OPEN` | Open | Open | |
| `IN_PROGRESS` | In Progress, Assigned, Started | — | All mean work has begun |
| `PENDING` | Pending | Pending | |
| `CANCELLED` | — | Cancelled | Kept separate: merging into CLOSED would make cancelled requests look resolved |
| `REFERRED` | — | Referred Out, Forward | Both mean passed to another agency |
| `UNKNOWN` | Unspecified / missing | missing | |

### Submission channel

NYC already uses a short list of channels (ONLINE, PHONE, MOBILE, OTHER, UNKNOWN), so that
list became the shared one. LA's 15 sources were mapped onto it:

| Standard | LA `requestsource` values |
|---|---|
| `PHONE` | Call, Voicemail, Queue Initiated Customer Call, Radio |
| `MOBILE` | Mobile App |
| `ONLINE` | Self Service, Email, Web Form, Social |
| `OTHER` | Driver Self Report, Council's Office, Fax, Walk-in, Letter, City Attorney |
| `UNKNOWN` | missing |

### Request type — intentionally not merged

NYC has many fine-grained complaint types (e.g. *Noise – Residential*, *Illegal Parking*). LA
has 12 sanitation- and streetlight-focused types (e.g. *Bulky Items*, *Graffiti Removal*). The
lists barely overlap. Forcing them into shared categories would lose detail, and the Phase 2
hotspot analysis needs the specific type within each city. Request types are therefore only
trimmed and null-normalized, not renamed.

## 6. Created attributes

| Attribute | Definition | Purpose |
|---|---|---|
| `source`, `city` | Constant per input file | Tell the cities apart after merging |
| `request_id` | `NYC-<unique_key>` / `LA-<srnumber>` | The prefix keeps IDs unique even if both cities use the same number |
| `closed_before_created` | close time < created time in the raw data | Records the temporal fix in §3 #8 |
| `is_valid_resolution` | status = CLOSED **and** close time present **and** close ≥ created | Decides which rows can be used for resolution time |
| `resolution_hours` | (`closed_at` − `created_at`) in hours, 4 decimals; null unless `is_valid_resolution` | Service-request resolution time (Phase 2 bonus analysis). Hours keep precision for same-day closures |
| `created_date`, `created_year`, `created_month`, `created_day`, `created_hour`, `created_dow` | Parts of `created_at` | `created_day` is used to split each month into the 1–7 / 8–14 / 15–21 / 22–end periods in Phase 2 |

## 7. Persisting the cleaned datasets

Each cleaned dataset keeps **all original columns** (cleaned and typed) plus the standardized
and derived columns. All outputs are saved as Parquet, which keeps column types and loads quickly
for Phase 2. `export_csv.py` produces CSV copies for submission or viewing.

| Dataset | Path | Rows |
|---|---|---|
| Cleaned NYC | `output/nyc_clean/` (CSV: `output/csv/nyc_clean.csv`) | 799,739 |
| Cleaned LA | `output/la_clean/` (CSV: `output/csv/la_clean.csv`) | 800,000 |
| Unified | `output/unified/` (CSV: `output/csv/unified.csv`) | 1,599,739 |

## 8. Merge

`merge.py` maps each cleaned dataset to the unified schema in §4, with the same column names,
order and types. Columns missing in one city are filled with typed nulls. The two tables are
then combined with `unionByName`. Matching by name instead of position means a column-order
mistake can't silently put values in the wrong column.

## 9. Validation

`validate.py` runs 24 checks on the unified dataset. **All 24 pass.**

| Area | Checks |
|---|---|
| Schema / types | 12 key columns have the expected type (e.g. `created_at` timestamp, `latitude` double, `council_district` int, `is_valid_resolution` boolean, `created_date` date) |
| Record counts | Unified = NYC cleaned + LA cleaned (1,599,739 = 799,739 + 800,000); per-source counts match the cleaned files |
| Identifiers | `request_id` is unique and never null; `created_at` is never null |
| Categoricals | `status` and `channel` contain only values from the standard lists |
| Derived attributes | `resolution_hours` ≥ 0; set exactly when `is_valid_resolution`; only on CLOSED rows; no `closed_at` before `created_at`; ZIPs are 5 digits or null; `created_year/month/day` match `created_at` |

Summary of the unified dataset:

| | NYC311 | LA311 |
|---|---|---|
| Rows | 799,739 | 800,000 |
| CLOSED | 704,564 | 730,126 |
| OPEN | 36,738 | 16,081 |
| IN_PROGRESS | 58,054 | — |
| PENDING | 330 | 17,972 |
| CANCELLED | — | 34,725 |
| REFERRED | — | 1,096 |
| UNKNOWN | 53 | — |
| Channel PHONE / ONLINE / MOBILE / OTHER / UNKNOWN | 163,510 / 419,093 / 150,738 / 789 / 65,609 | 314,677 / 129,997 / 277,390 / 77,936 / 0 |
| Valid resolutions | 703,951 | 728,135 |

## 10. Limitations and notes

- **Different time periods.** NYC covers Jul–Oct 2026 and LA covers Jan–Jul 2024. Time
  comparisons between the cities should keep this in mind.
- **Time zones.** Raw timestamps have no time zone and are kept as recorded (local time in each
  city). Resolution time is a difference within one city, so it is not affected.
- **Coarse channel mapping.** Mapping LA's 15 sources to 5 channels loses detail, which is why
  `channel_raw` is kept.
- **Bounding-box check.** The coordinate check uses a rectangle around each city, not the exact
  city boundary. It catches clearly wrong points (none were found), not points just outside the
  city limits.

## 11. Reproducing

1. Download the raw CSVs from UCR eLearn (link in §1) into `CS236-Project/datasets/`.
2. Run:

```bash
cd Dataset-Processing
./setup_env.sh     # one time: venv with pyspark==3.5.1, local Java 17 if needed
./run_all.sh       # clean -> merge -> validate (log in output/run_log.txt)
python export_csv.py   # optional CSV copies in output/csv/
```
