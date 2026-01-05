# Tagmarshal Lakehouse - Implementation & Presentation Notes

**Status:** Work in Progress
**Last Updated:** 2026-01-05

## 1. Executive Summary
We have successfully implemented a modern "Data Lakehouse" architecture that moves away from rigid, hardcoded ETL to a dynamic, discovery-based pipeline. This allows Tagmarshal to onboard complex facilities (27-holes, loops, random starts) without custom engineering for each client.

## 2. Key Architectural Achievements

### 🧩 Dynamic Topology Model
*   **Problem:** Legacy systems struggled with "non-standard" courses (e.g., Bradshaw's 27 holes, American Falls' 9-hole loops).
*   **Solution:** We decoupled the *Physical Course* from the *Logical Round*.
*   **How it works:**
    1.  **Auto-Discovery:** The pipeline scans incoming data to "learn" the course shape (e.g., "This course has 54 sections, so it must be two 9s").
    2.  **Configuration:** It generates a `dim_facility_topology` table that maps physical sections to logical units (Front/Back/Red/White).
    3.  **No Hardcoding:** Adding a new course is as simple as dropping the CSV file; the system adapts automatically.

### 🧱 The "Fact Round Hole Performance" Table
*   **What it is:** The critical middle layer for analytics.
*   **Why it matters:** It bridges the gap between raw GPS pings (too granular) and full-round summaries (too high-level).
*   **Enables:**
    *   Comparing "Hole 5 (Fresh)" vs. "Hole 5 (Fatigued)" on 9-hole loops.
    *   Identifying specific bottleneck holes regardless of start order (Shotgun starts).

## 3. Facility Nuances & Handling
*   **Bradshaw Farm GC (27 Holes):**
    *   *Challenge:* Ambiguity of "Hole 1" (Red vs. White vs. Blue).
    *   *Solution:* Topology mapping links sections 1-27 → Unit 1, 28-54 → Unit 2, etc.
*   **Indian Creek (Random Starts):**
    *   *Challenge:* Rounds start on any hole; sequence is non-linear.
    *   *Solution:* We preserve the `sequence_order` (order played) distinct from `hole_number` (flag number).
*   **American Falls (9-Hole Loops):**
    *   *Challenge:* Playing the same physical hole twice in one round.
    *   *Solution:* We use `nine_number` (1 vs 2) to distinguish the first pass from the second pass.

## 4. Missing Data & Client Requirements
We have identified several data points required for the full "Gold Standard" implementation that are currently missing from the source files:

*   **⚠️ Par & Handicap:** Critical for "Strokes Gained" style analysis and difficulty weighting.
*   **⚠️ Hole Duration Goal:** Essential for calculating "True Pace" in shotgun starts (where the round goal is dynamic). Currently relying on device-calculated pace.
*   **⚠️ Geo-Center Coordinates:** Required for advanced spatial clustering/heatmaps beyond raw point plotting.

**Action Item:** Request a `facility_metadata.csv` export from the client containing: `course_id`, `hole_number`, `par`, `handicap`, `target_duration_seconds`.

## 5. Next Steps
1.  Complete the `fact_round_hole_performance` Gold model.
2.  Implement "Pace Normalization" logic once target times are available.
3.  Build the "Bottleneck Detection" dashboard view.
