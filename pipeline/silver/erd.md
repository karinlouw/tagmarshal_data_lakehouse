# Silver ERD (normalized view for exploration)

This ERD is meant to help you **explore and understand the Silver layer**.

It includes:
- **(A) Current physical Silver model** (what exists today)
- **(B) Proposed normalized exploration model** (recommended mental model / optional views)

> Gold is not the right place for this: Gold is analytics/aggregations. A normalized ERD belongs with Silver because Silver is your conformed “source of truth” layer.

---

## A) Current physical Silver model (as-built)

Key tables in `iceberg.silver` today:
- `fact_telemetry_event` (one row per location slot / GPS fix)
- `dim_facility_topology` (per-course unit/nine ranges; used to map sections → nines/units)
- `dim_course_profile` (human-entered course metadata)

Notes about keys:
- `fact_telemetry_event` does **not** have a single `event_id`. The effective uniqueness after Silver dedup is:
  - **(round_id, location_index, fix_timestamp)** (where `fix_timestamp` may be NULL)

```mermaid
erDiagram
  SILVER_FACT_TELEMETRY_EVENT {
    string  round_id          "Round identifier (Mongo ObjectId)"
    string  course_id         "Course identifier"
    date    ingest_date       "Ingest date"
    int     location_index    "0-based position in locations[]"
    timestamp fix_timestamp   "Nullable"
    boolean is_timestamp_missing
    boolean is_location_padding
    int     hole_number       "Nullable"
    int     section_number    "Nullable"
    int     nine_number       "Nullable"
    double  latitude          "Nullable"
    double  longitude         "Nullable"
    double  pace_gap          "Nullable"
    boolean is_cache          "Nullable"
  }

  SILVER_DIM_FACILITY_TOPOLOGY {
    string  facility_id       "Course id"
    int     unit_id
    string  unit_name
    int     nine_number
    int     section_start
    int     section_end
  }

  SILVER_DIM_COURSE_PROFILE {
    string  course_id
    string  course_type
    string  volume_profile
    int     peak_season_start_month
    int     peak_season_end_month
    string  notes
    string  source
  }

  SILVER_DIM_COURSE_PROFILE ||--o{ SILVER_FACT_TELEMETRY_EVENT : "course_id"
  SILVER_DIM_FACILITY_TOPOLOGY ||--o{ SILVER_FACT_TELEMETRY_EVENT : "course_id + nine_number"
```

Relationship caveat (topology):
- `dim_facility_topology` is really a **range** mapping by `section_number` (between `section_start` and `section_end`) plus `facility_id`.
- Silver ETL uses that range to infer `nine_number`. In Gold, we often join by `(course_id, nine_number)`.

---

## B) Proposed normalized exploration model (recommended)

This is the “clean mental model” that makes exploration easier. It can be implemented as:
- **Views** in Trino, or
- **dbt models** (in Silver, not Gold), or
- Just used as the ERD you reference while querying the single Silver fact table.

```mermaid
erDiagram
  DIM_COURSE {
    string course_id PK
  }

  DIM_DEVICE {
    string device_id PK
  }

  DIM_TOPOLOGY_UNIT {
    string course_id FK
    int    unit_id
    string unit_name
    int    nine_number
    int    section_start
    int    section_end
  }

  DIM_ROUND {
    string   round_id PK
    string   course_id FK
    string   device_id FK
    timestamp round_start_time "Nullable"
    timestamp round_end_time   "Nullable"
    int      start_hole        "Nullable"
    int      start_section     "Nullable"
    int      end_section       "Nullable"
    boolean  is_nine_hole      "Nullable"
    boolean  is_complete       "Nullable"
    int      goal_time         "Nullable"
  }

  FACT_TELEMETRY_FIX {
    string   round_id FK
    int      location_index
    timestamp fix_timestamp "Nullable"
    boolean  is_timestamp_missing
    boolean  is_location_padding
    int      hole_number    "Nullable"
    int      section_number "Nullable"
    int      hole_section   "Nullable"
    int      nine_number    "Nullable"
    double   latitude       "Nullable"
    double   longitude      "Nullable"
    double   pace           "Nullable"
    double   pace_gap       "Nullable"
    double   positional_gap "Nullable"
    boolean  is_cache       "Nullable"
    boolean  is_projected   "Nullable"
    boolean  is_problem     "Nullable"
    double   battery_percentage "Nullable"
  }

  DIM_COURSE ||--o{ DIM_ROUND : "course_id"
  DIM_DEVICE ||--o{ DIM_ROUND : "device_id"
  DIM_ROUND  ||--o{ FACT_TELEMETRY_FIX : "round_id"
  DIM_COURSE ||--o{ DIM_TOPOLOGY_UNIT : "course_id"
```

### How it maps to today’s Silver columns

- **`FACT_TELEMETRY_FIX`**: this is basically `silver.fact_telemetry_event` (already long/row-per-fix).
- **`DIM_ROUND`**: group `silver.fact_telemetry_event` by `round_id` and take a stable aggregation of round-level fields:
  - `MAX(start_hole)`, `MAX(goal_time)`, `BOOL_OR(is_complete)`, etc.
- **`DIM_DEVICE`**: distinct `device` values from Silver.
- **`DIM_TOPOLOGY_UNIT`**: `silver.dim_facility_topology` (already exists).
- **`DIM_COURSE_PROFILE`**: `silver.dim_course_profile` (already exists; optional metadata).

---

## Optional: DBML file for dbdiagram.io

If you prefer dbdiagram.io, use `pipeline/silver/erd.dbml` (included in repo) and paste it into dbdiagram to get an interactive diagram.


