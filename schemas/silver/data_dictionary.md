# Silver Layer Data Dictionary

## Table: `fact_telemetry_event`

One row per GPS fix (location reading) from a device during a round of golf.

---

## Round Identification

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `round_id` | VARCHAR | Unique identifier for the round (MongoDB ObjectId) | `6861b0ba410113f1d4ef2309` |
| `course_id` | VARCHAR | Course identifier (folder name in Bronze layer) | `bradshawfarmgc` |
| `ingest_date` | VARCHAR | Date the data was ingested (YYYY-MM-DD format) | `2025-06-28` |

---

## Round Configuration

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `start_hole` | INTEGER | Which hole the round started on (1-18, or any for shotgun) | `1` (normal), `10` (back 9), `5` (shotgun) |
| `start_section` | INTEGER | Cumulative section number where round started | `4` |
| `end_section` | INTEGER | Cumulative section number where round ended | `57` |
| `is_nine_hole` | BOOLEAN | Whether this is a 9-hole round (vs 18-hole) | `true` = 9-hole round |
| `current_nine` | INTEGER | Which 9 the device is currently on | `1` = front 9, `2` = back 9, `3` = third 9 (27-hole) |
| `goal_time` | INTEGER | Target time for the round in seconds (set by course, often unrealistic) | `15840` = 4 hours 24 minutes |
| `is_complete` | BOOLEAN | Whether the round was completed (≥75% sections visited) | `true` = finished round |

---

## Location Within Round

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `location_index` | INTEGER | Position in the round's location array (0-based) | `0` = first fix, `52` = 53rd fix |
| `hole_number` | INTEGER | Which hole the device is on (1-18 or 1-27) | `7` |
| `section_number` | INTEGER | Cumulative section across entire course (1-57 for 18 holes) | `21` = hole 7, section 1 |
| `hole_section` | INTEGER | Section within the current hole (resets per hole) | `1` = tee, `2` = fairway, `3` = green |
| `nine_number` | INTEGER | Which 9-hole set this section belongs to | `1` = holes 1-9, `2` = holes 10-18, `3` = holes 19-27 |

---

## GPS & Timing

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `fix_timestamp` | TIMESTAMP | When the GPS fix was recorded | `2025-06-28 14:32:15` |
| `event_date` | DATE | Date extracted from fix_timestamp | `2025-06-28` |
| `latitude` | DOUBLE | GPS latitude coordinate | `34.15306` |
| `longitude` | DOUBLE | GPS longitude coordinate | `-84.44809` |
| `geometry_wkt` | VARCHAR | Well-Known Text representation of the point | `POINT(-84.448 34.153)` |

---

## Pace Metrics (Critical for Analysis)

| Column | Type | Description | Example | Interpretation |
|--------|------|-------------|---------|----------------|
| `pace` | DOUBLE | Seconds behind/ahead of goal time | `527.14` | **Positive = behind schedule**, negative = ahead |
| `pace_gap` | DOUBLE | Time gap to the group ahead (seconds) | `899.827` | Higher = more spacing; **spike indicates bottleneck forming** |
| `positional_gap` | DOUBLE | Position relative to group ahead | `224.054` | **Positive = falling behind**, negative = catching up |

### Understanding Pace Metrics

**`pace`** (Less Reliable)
- Relative to `goal_time` which is set by the course and often unrealistic
- Useful for comparing holes within the same course
- NOT good for cross-course comparison

**`pace_gap`** (More Reliable for Bottlenecks)
- Shows actual spacing between consecutive groups
- A **spike** in pace_gap indicates groups bunching up (bottleneck)
- First group of the day has no pace_gap (NULL)
- Use this to identify WHERE bottlenecks form

**`positional_gap`** (Best for Individual Group Analysis)
- Shows if a group is catching up or falling behind the group ahead
- Positive = behind (potential delay)
- Negative = catching up (faster than group ahead)
- Use this to identify WHICH groups are causing problems

---

## Device & Status Flags

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `is_cache` | BOOLEAN | Fix was stored offline and uploaded later | `true` = device was offline |
| `is_projected` | BOOLEAN | Location was projected (estimated), not measured | `true` = estimated position |
| `is_problem` | BOOLEAN | Group meets course-defined problem criteria | `true` = flagged as problem |
| `is_timestamp_missing` | BOOLEAN | Fix had no valid timestamp in source data | `true` = timestamp was NULL/empty |
| `battery_percentage` | DOUBLE | Device battery level (0-100) | `85.0` = 85% battery |

### Data Quality Flags

**`is_timestamp_missing`** (New for Audit Phase)
- Tracks records where the original timestamp was NULL or empty
- Allows us to analyse data completeness without losing records
- Use to filter when timestamp is required for analysis
- Example: `WHERE is_timestamp_missing = false` for time-series analysis

---

## Column Availability by Course

Not all columns have data for all courses:

| Course | pace | pace_gap | positional_gap | Notes |
|--------|------|----------|----------------|-------|
| americanfalls | ✅ 100% | ✅ 91% | ✅ 91% | Good coverage |
| bradshawfarmgc | ✅ 100% | ✅ 94% | ⚠️ 82% | Some missing positional_gap |
| erinhills | ✅ 100% | ✅ 92% | ✅ 88% | Good coverage |
| indiancreek | ✅ 100% | ❌ 0% | ❌ 0% | **No gap metrics** |
| pinehurst4 | ✅ 100% | ✅ 93% | ✅ 92% | Good coverage |

---

## Recommended Columns for Analysis

### Bottleneck Analysis (Where do delays occur?)
1. **`pace_gap`** - Identify sections where groups bunch up
2. **`section_number`** - Aggregate by course section
3. **`hole_number`** - Summarise by hole
4. Filter: `pace_gap IS NOT NULL`

### Slow Group Analysis (Who is causing delays?)
1. **`positional_gap`** - Identify groups falling behind
2. **`pace`** - Secondary metric for goal time deviation
3. **`is_problem`** - If populated, direct flag for problem groups

### Round Performance Analysis
1. **`is_complete`** - Filter to complete rounds only
2. **`goal_time`** - Compare actual vs expected
3. **`start_hole`** - Identify shotgun vs normal starts

### GPS/GIS Analysis
1. **`latitude`, `longitude`** - Point coordinates
2. **`geometry_wkt`** - WKT format for GIS tools
3. Aggregate by `section_number` for heat maps

---

## Key Insights from Terminology Glossary

> "T-gap (pace_gap) is more important than goal time for measuring actual pace adherence"

> "Relative Position Gap (positional_gap): Better measure than goal time because it accounts for actual field conditions"

> "You're all slow, but that's one person's fault - can't go faster than group ahead"

**Bottom line:** Use `pace_gap` and `positional_gap` for bottleneck analysis, not `pace` alone.

