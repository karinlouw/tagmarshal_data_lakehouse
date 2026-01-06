# Tagmarshal Data Lakehouse Presentation Notes

This document serves as a working presentation outline and a repository for key talking points, data nuances, and missing requirements to be communicated to the client.

## 1. Executive Summary & Architecture
*   **Local-First, Cloud-Ready:** The architecture is designed to run entirely locally using Docker (MinIO, Spark, Trino, Airflow) but is "AWS Ready" via configuration changes.
*   **ELT Pattern:** Adopts Extract-Load-Transform (ELT) over traditional ETL. Raw data (Bronze) is immutable. Transformation logic sits in Silver/Gold layers, allowing for reprocessing of history if logic changes.
*   **Format:** Migration from MongoDB JSON/CSV to Apache Iceberg (Parquet) for high-performance analytics.

## 2. Dynamic Topology Data Model
We have implemented a "Topology First" approach to handle the complex nature of golf courses.

*   **The Problem:** "Hole 1" is ambiguous. On a 27-hole course (Bradshaw), "Hole 1" could be Red #1, White #1, or Blue #1. On a 9-hole loop course (American Falls), "Hole 1" could be the 1st hole of the day or the 10th hole (second loop).
*   **The Solution:**
    *   **Automated Discovery (data-driven):** `generate-topology` scans Silver telemetry and infers the course layout from **observed section ranges**. It does not assume “3 sections per hole” (tee/fairway/green); some holes contain 4+ sections in the telemetry.
    *   **Outlier-resistant boundaries:** Topology inference uses **frequency-aware section ranges** so that rare GPS/geofence artifacts do not distort `section_start/section_end`.
        *   Uses only section numbers with at least `TM_TOPOLOGY_MIN_FIXES_PER_SECTION` fixes (default: 25).
        *   Applies a small pad (`TM_TOPOLOGY_RELIABLE_RANGE_PAD`, default: 2) so legitimate edge sections are retained.
    *   **Two inference strategies (covers Bradshaw + standard 18-hole courses):**
        *   **Strategy A (hole numbers span 1..18 / 1..27):** derive units from hole-number bands (1–9, 10–18, 19–27) and compute `section_start/section_end` from min/max observed `section_number` for those holes.
        *   **Strategy B (hole numbers reset 1..9 per unit, common on 27-hole facilities):** scan `section_number` in order and detect “unit boundaries” where the dominant `hole_number` resets (e.g., 9 → 1). This avoids hardcoding “27 sections per nine”.
    *   **Configuration Table:** Generates `pipeline/silver/seeds/dim_facility_topology.csv` (and seeds `iceberg.silver.dim_facility_topology`) to map `course_id` → `{unit_id, nine_number, section_start, section_end}`.
    *   **Enrichment:** The Silver ETL tags each fix with a robust `nine_number`:
        *   Prefer **device-reported** `current_nine` when present (critical for loop courses like American Falls).
        *   Otherwise use the inferred topology join.

## 2.5 Course Types Reference (Simple Dimension)
We maintain a simple, queryable record of “course types” for pilot/demo communication and downstream logic.

*   **Seed file:** `pipeline/silver/seeds/dim_course_profile.csv`
*   **Iceberg table:** `iceberg.silver.dim_course_profile`
*   **Command:** `just seed-course-profile`

## 2.6 Seasonality (Inferred From Telemetry)
We infer seasonality from the data by counting rounds by:
*   **Month** (includes `month_name`)
*   **Weekday** (includes `weekday_name`)

Gold outputs:
*   `iceberg.gold.course_rounds_by_month` (includes `pct_total` for easy ranking)
*   `iceberg.gold.course_rounds_by_weekday`

## 3. Handling Facility Nuances (Specific Examples)

### Bradshaw Farm GC (27-Hole Complex)
*   **Challenge:** High volume, 3 distinct sets of 9 holes.
*   **Solution:** The topology model dynamically assigns fixes to Unit 1, 2, or 3 based on inferred **section ranges**. This supports accurate comparisons such as “Unit 1 pace” vs “Unit 2 pace”, including cases where hole numbers repeat (1–9) for each unit.

### Indian Creek (Random Starts)
*   **Challenge:** Low volume, Shotgun starts (starting on random holes).
*   **Solution:** Processing is non-sequential. We track performance independently of start time. Topology inference is based on section layout, not play order.
*   **Missing Data:** We need to confirm if we have the static `hole_duration_goal` reference data to normalize pace for shotgun starts. (Currently relying on device-reported pace).

    **Shotgun Start Validation (Trino):** start hole distribution
    ```sql
    SELECT
      start_hole,
      COUNT(DISTINCT round_id) AS rounds
    FROM iceberg.silver.fact_telemetry_event
    WHERE course_id = 'indiancreek'
      AND start_hole IS NOT NULL
    GROUP BY start_hole
    ORDER BY rounds DESC, start_hole;
    ```
    *Saved query:* `pipeline/queries/examples/indiancreek_shotgun_start_distribution.sql`

### American Falls (9-Hole Loop)
*   **Challenge:** 9-hole course played twice to form an 18-hole round.
*   **Solution:** We differentiate "Hole 5 (First Loop)" from "Hole 5 (Second Loop)" using the `nine_number` column.
    *   `nine_number = 1`: First 9 holes (Fresh).
    *   `nine_number = 2`: Second 9 holes (Fatigued/Traffic).
*   **Analytics Value:** Enables analysis of "Fatigue Factor" – do players play the same physical hole slower the second time around?

    **Verification Example (Trino):**
    ```sql
    SELECT nine_number, AVG(avg_pace_sec) as avg_pace 
    FROM iceberg.gold.fact_round_hole_performance 
    WHERE course_id = 'americanfalls' AND hole_number = 5 
    GROUP BY nine_number;
    ```
    *Result:*
    *   Nine 1: 207s (Faster)
    *   Nine 2: 247s (Slower) → **+40s Fatigue/Traffic delay detected.**

## 4. Pipeline Flow Diagram
```
Files in `/data/`         -> bronze_ingest_dag (Justfile bronze-upload*)
  • CSV/JSON land in MinIO → (Landing zone)
                             ↓
                         spark ETL
                           (just silver / silver_etl_dag)
  • `generate_topology.py` -> analyzes Silver data -> writes `dim_facility_topology.csv`
  • `seed_topology.py`     -> uploads CSV to MinIO & writes Iceberg `dim_facility_topology`
                           ↓
                         Silver table (`fact_telemetry_event`)
                             ↓
                         dbt Gold models (just gold / gold_dbt_dag)
                             • `fact_round_hole_performance` joins topology & telemetry
                             • other dbt models (pace_summary, signal quality, etc.)
```
*   **Observation:** The `just pipeline-all` command chains bronze upload → Silver ETL → Gold dbt run, so the above flow executes automatically when the full pipeline is triggered.

## 5. Missing Information & Client Requests
We need to request the following from the client to fully realize the vision:

*   **Topology Metadata:** The `dim_facility_topology` is currently inferred. We need the physical attributes:
    *   `par` (Par 3/4/5)
    *   `handicap_index` (Difficulty)
    *   `hole_duration_goal` (Target time per hole) - **CRITICAL** for accurate shotgun pace analysis.
    *   `geo_center_lat`/`geo_center_lon` (For map visualization).
*   **Business Rules:** Confirmation on specific "Goal Time" logic for fractional rounds.

## 6. Next Steps
1.  Finalize Gold Layer aggregation (`fact_round_hole_performance`).
2.  Implement Data Quality gates (Great Expectations).
3.  Build Streamlit Dashboard for client demo.
