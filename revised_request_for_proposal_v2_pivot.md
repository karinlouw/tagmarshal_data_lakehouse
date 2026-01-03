# Revised scope of work: Tagmarshal data strategy project

**Date:** 12 December 2025

---

## Project overview

This updated scope reflects what we've learned so far and focuses on delivering value with the data that's actually available—your processed round strings—while setting up the architecture to handle raw telemetry and booking data down the line.

**Goal:** Build a scalable lakehouse on AWS that brings together your round data in one place, with the flexibility to add raw telemetry and booking data when you're ready.

---

## 1. Strategic approach: Twin ingestion

The plan uses two parallel data paths so we can deliver operational value now while keeping the door open for machine learning later.

- **Operational path (Gold standard):** Bring in your existing round strings data as-is. This keeps dashboards consistent with your current reporting logic.

- **Innovation path (Bronze foundation):** Set up storage to accept raw telemetry (GPS pings) and booking data in the future. This gets the groundwork in place for ML and advanced analytics without slowing down what we're building now.

---

## 2. Scope overview

### 2.1 Ingestion

- **Pilot scope:** Working with historical CSV exports from the 5 pilot courses you've provided.
- **How it works:** Building a local "lakehouse in a box" to nail down the data model and transformation logic before we move to the cloud.
- **Scaling up:** Once AWS is set up, we'll automate ingestion for more courses. We'll start with 20-50 courses to validate the pipeline, then ingest the full fleet.

### 2.2 Storage and architecture

We're using a medallion architecture, structured around what's available now:

| Layer | What it does |
|-------|--------------|
| **Bronze** (Raw) | Landing zone for incoming files and future API data. |
| **Silver** (Cleaned) | Standardised data—UTC timestamps, validated schemas, flattened structures where needed. |
| **Gold** (Reporting) | Business-ready tables for dashboards (pace of play, round counts, etc.). |

### 2.3 Data quality

- **Visibility into your data:** As part of Phase 1, we'll surface key data quality metrics—completeness, consistency, anomalies—so you can see the current state of your data across the pilot courses. This helps identify issues early and guides decisions about what to prioritise as we scale.

---

## 3. Deliverables

### Phase 1: Foundation and local build

- **Conceptual data model:** A visual diagram showing how courses, rounds, and (eventually) bookings and players connect.
- **Local lakehouse:** A working pipeline running locally on sample data to validate the transformation logic.
- **Data quality report:** A view of the pilot data showing completeness, consistency, and any anomalies across the 5 courses.
- **Infrastructure specs:** What's needed to set up the AWS environment.

### Phase 2: AWS deployment and validation

- **Cloud deployment:** Moving the local pipeline into your AWS environment.
- **Validation:** Testing against a larger dataset (target: 20-50 courses) to catch edge cases.
- **Data health dashboard:** A view showing data quality metrics, completeness, and any anomalies to help guide improvements.

### Phase 3 and 4: Rollout and insights

- **Glue catalog:** Setting up the data catalog so you can query everything via Athena.
- **Pilot dashboard:** A working dashboard showing key metrics of your data
- **Production roadmap:** A clear plan for rolling this out to the full 650-course fleet.

---

## 4. Timeline (13 weeks)

We're keeping the original 13-week timeline to allow enough breathing room for discovery, testing, and proper documentation.

### Phase 1: Discovery and connectivity (weeks 1–3)

- Map out the 5 pilot courses.
- Build local data pipeline and transformation scripts using provided CSVs.
- Draft the conceptual data model.
- Produce data quality report for the pilot courses.

### Phase 2: Ingestion and stability (weeks 4–6)

- Deploy AWS infrastructure (buckets, permissions).
- Test pipeline with a larger batch of data (20 courses).
- Validate AWS data against source systems.

### Phase 3: Transformations and logic (weeks 7–10)

- Finalise silver/gold logic (UTC conversion, schema checks).
- Expand data quality checks for the larger dataset.

### Phase 4: Insights and scale planning (weeks 11–13)

- Optimise query performance and cost.
- Build pilot dashboard.
- Plan full automation for the global fleet.

---

## 5. Budget guidance

Services cost remains the same as the original proposal. By focusing on the data that's available now, we reduce delivery risk and get to value faster.
