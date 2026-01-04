# Tag Marshal Terminology Glossary

**Purpose:** This document defines key terminology specific to Tag Marshal and the golf industry as discussed in the data discovery meeting. Use this as a reference when working with the data and system.

---

## Core Data Concepts

### Fix
- **Definition:** A single GPS position reading from a device
- **Details:**
  - Devices send fixes approximately every 25 seconds (device-dependent)
  - Contains: latitude, longitude, timestamp, device ID, battery level, GPS accuracy metrics
  - Goes through **Gatekeeper** first, then **Grindr** processing
  - Multiple fixes occur within each section, but only the **first fix per section** is recorded in round strings
- **Example:** A device at hole 3, fairway sends a fix with coordinates and timestamp

### Round / Round String
- **Definition:** A complete record representing one group's attempt to play golf (complete or incomplete)
- **Details:**
  - **One record = one round** (not multiple rounds)
  - Created when device enters the **trigger zone** (typically second zone)
  - Contains aggregated data: all sections visited, timestamps, pace metrics, location arrays
  - Can be: complete (18 holes), incomplete (didn't finish), 9-hole, or secondary (multiple devices tracking same group)
- **Key Point:** This is the primary table that will be migrated to the data lake
- **Example:** A round string contains all the sections a group visited from start to finish, with timing data

### Round String
- **Definition:** Same as "Round" - the historical information of rounds stored in the database
- **Note:** The team uses "round string" and "round" interchangeably, but "round string" is the technical database term

---

## System Components & Processes

### Grindr
- **Definition:** Tag Marshal's internal process that performs geospatial calculations
- **What it does:**
  - Takes positional fixes (GPS coordinates) and determines which section/zone the device is in
  - Performs expensive geospatial joins between course mapping and device positions
  - Processes every fix that comes into the system
  - Device-agnostic: doesn't care what device type sent the data, only uses positional data
- **Output:** Produces round information with section assignments
- **Fun fact:** Named before the dating app existed (or maybe intentionally - team debate!)

### Gatekeeper
- **Definition:** System component that processes raw data packets from classic devices
- **What it does:**
  - Receives comma-separated variable (CSV) packets from devices
  - Parses data into structured columns
  - Uses device ID to route to central system
  - Central system identifies which course the device belongs to
  - Routes fix data to the appropriate course server
- **Note:** Two-way devices bypass Gatekeeper and call APIs directly

### Central System
- **Definition:** Management system that coordinates all 700 courses
- **What it contains:**
  - Table of all courses
  - Table of all devices
  - Mapping between courses and devices
  - Device-to-course routing logic
- **Note:** Does NOT store fix data - that goes to individual course servers

### Course Server
- **Definition:** Individual MongoDB database for each of the 700 courses
- **What it contains:**
  - All fix data for that course
  - Round strings for that course
  - Course-specific configuration
- **Key Point:** No course ID in the data - each database is course-specific

---

## Golf Course Structure

### Section / Zone
- **Definition:** A geospatial polygon defining a specific part of the golf course
- **Details:**
  - Each hole is divided into multiple sections
  - **Par 4:** Typically 3 sections (tee box, fairway, green)
  - **Par 5:** Typically 4 sections (tee box, 2 fairways, green)
  - Sections are polygons drawn on the course map
  - Each section has: goal time, hole number, hole section number, cumulative section number
- **Example:** "Hole 1, Section 2" = the fairway of the first hole

### Hole Section
- **Definition:** The section number within a specific hole (1, 2, 3, or 4)
- **Details:**
  - Resets for each hole
  - Section 1 = usually tee box
  - Section 2+ = fairway sections
  - Last section = usually green
- **Example:** Hole 3, hole section 2 = second section of hole 3 (likely the fairway)

### Cumulative Section
- **Definition:** Sequential numbering across the entire course (1 to ~57 for 18 holes)
- **Details:**
  - Does NOT reset between holes
  - Continues counting: hole 1 sections = 1,2,3; hole 2 sections = 4,5,6; etc.
  - Used for calculating cumulative expected time through the course
- **Example:** Hole 3, section 1 might be cumulative section 7

### Trigger Zone
- **Definition:** The section where a round officially starts (typically the second section, not the first)
- **Details:**
  - Round starts when device enters trigger zone (not first tee box)
  - First tee box (zone 1) is skipped because players wait around there
  - System applies 3-minute average for first tee shot (subtracted from entry time)
  - Also used to determine round end (final green zone)
- **Why:** Players hang around first tee box, so starting there would give inaccurate start times

### Halfway / Halfway House
- **Definition:** A section between the front 9 and back 9 holes (typically between holes 9 and 10)
- **Details:**
  - Can be a significant time factor
  - Cultural differences: South Africa = sit-down meal (20-25 minutes), Germany = grab-and-go (1 minute)
  - Has its own goal time allocation
- **Example:** Some courses allocate 14 minutes, but Germans take 1 minute

---

## Round Types & Status

### Complete Round
- **Definition:** A round that finished properly
- **Criteria:**
  - Entered at least 75% of all sections
  - Typically takes ~4 hours for 18 holes
  - Reached the final green zone
- **Note:** Incomplete rounds are excluded from average round time calculations

### Incomplete Round
- **Definition:** A round that didn't finish properly
- **Reasons:**
  - Group left early
  - Didn't cover 75% of sections
  - Hole was closed, had to skip
  - Mapping error (cart path different than mapped)
- **Handling:** Marked as incomplete, typically excluded from reporting

### 9-Hole Round
- **Definition:** A round that only covers the first 9 holes
- **Details:**
  - System detects if group continues to hole 10 within reasonable time
  - If they do, it becomes an 18-hole round
  - If they don't, it's marked as 9-hole
  - Takes approximately half the time of 18-hole round

### Primary Round
- **Definition:** The main round record for a group (first device to enter trigger zone)
- **Details:**
  - For walking courses: every round is primary (one device per group)
  - For cart courses: first cart to enter trigger zone becomes primary
  - Used for all reporting and analytics
- **Example:** In a 4-cart group, the first cart's round is primary

### Secondary Round
- **Definition:** Additional round records for the same group (other devices tracking the same group)
- **Details:**
  - Only applies to cart courses with multiple devices per group
  - Created if another device enters trigger zone within 2 minutes of primary
  - System checks: "Did anyone start a round in this hole within last 120 seconds?"
  - Typically ignored in reporting to avoid double-counting
  - Not very sophisticated - just time-based matching
- **Example:** Cart 2 in a 4-cart group creates a secondary round

---

## Timing & Pace Metrics

### Goal Time
- **Definition:** The expected/desired time to play a section or entire round
- **Details:**
  - **NOT calculated** - set by the golf course (client)
  - Can be unrealistic (too fast or too slow)
  - Initial calculation based on course's desired total round time (e.g., 4 hours 20 minutes)
  - Divided by hole par values and section ratios (e.g., 20% for tee shot)
  - Can be adjusted manually after 2-3 weeks of actual data
  - Can vary by time of day (e.g., faster in morning, slower at peak)
- **Warning:** Don't rely too heavily on goal times - they're client-set, not data-driven
- **Example:** Goal time for hole 1 might be 11 minutes 46 seconds

### Goal Name
- **Definition:** Identifier for which goal time profile is being used
- **Details:**
  - Default goal time = "default"
  - Time-of-day variations = "weekday mornings", "peak time", etc.
  - Links to goal time fraction (percentage adjustment)
- **Example:** "Weekday mornings" might have a 0.8 fraction (20% faster)

### Goal Time Fraction
- **Definition:** Percentage multiplier applied to goal times
- **Details:**
  - Applied equally to all sections
  - 0.8 = 20% reduction (faster pace expected)
  - 1.0 = standard goal time
  - Applied at section level
- **Example:** If goal time is 5 hours and fraction is 0.8, effective goal time is 4 hours

### Expected Time
- **Definition:** The actual average time it takes to play (calculated from historical data)
- **Details:**
  - Determined after 2-3 weeks of data collection
  - Compared to goal times for manual adjustment
  - Factors: time of day, course congestion, number of groups ahead
  - More reliable than goal time (data-driven)
- **Example:** Expected time for hole 1 might be 12 minutes 30 seconds (actual average)

### Pace of Play
- **Definition:** How fast or slow a group is moving through the course
- **Details:**
  - Measured relative to goal time (pace metric)
  - Measured relative to other groups (T-gap, pace gap)
  - Deteriorates throughout the day (morning faster, afternoon slower)
  - Affected by structural bottlenecks (shorter holes after longer ones)

### Pace Gap
- **Definition:** Time difference between two consecutive groups
- **Details:**
  - Measured throughout the round
  - Fluctuates as groups speed up or slow down
  - First group has no pace gap (no one ahead)
- **Example:** Group 1 starts at 7:00, Group 2 starts at 7:10 = 10-minute gap initially

### T-Gap (Tee Gap)
- **Definition:** Time gap relative to expected tee time interval
- **Details:**
  - Expected interval (e.g., 10 minutes = 600 seconds)
  - Actual gap minus expected gap = T-gap
  - **Positive T-gap** = delayed (behind schedule)
  - **Negative T-gap** = fast (ahead of schedule)
  - **Zero T-gap** = ideal
  - More important than goal time for measuring pace adherence
- **Example:** If expected gap is 10 minutes (600s) but actual is 13:50 (830s), T-gap is +230 seconds (delayed)

### Relative Position Gap
- **Definition:** How far ahead or behind a group is relative to the group in front
- **Details:**
  - **Positive** = behind (bad)
  - **Negative** = too fast (also bad sometimes)
  - **Zero** = ideal spacing
  - Better measure than goal time because it accounts for actual field conditions
- **Key Insight:** "You're all slow, but that's one person's fault" - can't go faster than group ahead

### Start Accuracy
- **Definition:** How close actual start time is to booked tee time
- **Details:**
  - Measured in minutes difference
  - "Within 2 minutes" is considered accurate
  - Calculated per section
- **Example:** Booked for 10:00, actually started at 10:03 = 3 minutes off (inaccurate)

### Waste Time
- **Definition:** Time spent waiting (measured when group ahead is blocking)
- **Details:**
  - Measured from when you arrive at tee until group ahead leaves fairway
  - Subtracted from measured start time
  - Used to account for delays caused by others
- **Example:** Arrive at tee at 10:00, group ahead leaves at 10:05 = 5 minutes waste time

---

## Status Indicators

### On Pace
- **Definition:** Group is meeting expected times
- **Details:**
  - Meeting cumulative expected time as they enter each zone
  - No alerts generated

### Slow
- **Definition:** Behind pace but NOT being delayed by others
- **Details:**
  - Off pace relative to goal time
  - No one within 2 zones ahead causing delay
  - Group is the problem (not being delayed)

### Delayed
- **Definition:** Behind pace because someone ahead is slow
- **Details:**
  - Off pace relative to goal time
  - Someone within 2 zones ahead is slow
  - Group is being held up by others
  - Not the group's fault

### Delaying
- **Definition:** Slow and causing others behind to wait
- **Details:**
  - Slow relative to goal time
  - Someone within 2 zones behind is being delayed
  - This group is the problem causing delays

### Held Up By
- **Definition:** Device ID of the group causing delay
- **Details:**
  - Not always the group immediately ahead
  - Could be multiple groups ahead in a chain
  - Identifies the "head of the snake" (leading problem group)

### Holding Up
- **Definition:** Boolean indicating if this group is delaying others
- **Details:**
  - True if someone behind is being delayed
  - Can be both "held up" and "holding up" (in the middle of a delay chain)
- **Logic:** Check backward first (is someone behind?), then forward (is someone ahead slower?)

### isProblem
- **Definition:** Boolean flag indicating if group meets problem criteria
- **Details:**
  - Criteria example: 10 minutes behind goal time AND 10 minutes behind group ahead
  - Can't recover until back within threshold
  - Course-specific criteria

---

## Device & Hardware Terms

### Classic Tag
- **Definition:** Original IoT device for walking courses (attached to golf bags)
- **Details:**
  - Off-the-shelf IoT unit with SIM card
  - GPS chip for location
  - Cellular network connection
  - Sends coordinates only (doesn't know which section it's in)
  - Caches data when offline
  - ~2MB data per month
  - Heartbeat every hour when idle, sends when moving (accelerometer)

### Two-Way Device
- **Definition:** Android-based screen device for carts (8-inch or 10-inch)
- **Details:**
  - Can send data to golfer and receive data from golfer
  - Downloads course sections and stores on device
  - Knows which section it's in (more accurate)
  - Runs offline, still provides GPS yardage
  - Locked down with MDM (Mobile Device Management)
  - More accurate zone entry than classic tags

### Automotive Tracker
- **Definition:** Cart-installed tracker (no screen)
- **Details:**
  - Automotive-grade hardware
  - Can have buzzer (only for geofencing, not pace)
  - Professional installation with wiring harnesses

### Tag Number
- **Definition:** Human-readable identifier for devices (e.g., "Tag 19")
- **Details:**
  - Used by clients for easy identification
  - Links to tee sheet assignments
  - Not always sequential (some courses use "Tag 20 and 21" for same cart)
  - Different from system ID (long unique string) and IMEI (hardware ID)

### Device ID
- **Definition:** Unique system identifier for each device
- **Details:**
  - Long alphanumeric string
  - Used internally for all data processing
  - Mapped to course via central system
  - Different from tag number (human-readable)

### IMEI
- **Definition:** Hardware identifier for devices
- **Details:**
  - International Mobile Equipment Identity
  - Manufacturer-assigned hardware ID
  - Used for device identification at hardware level

---

## Booking & Player Terms

### Tee Sheet / T-Sheet
- **Definition:** Booking system that assigns tee times to groups
- **Details:**
  - ~17% of courses have tee sheet integration
  - ~50% manually assign devices to bookings (accurate data)
  - Contains: player count, player names, player IDs, booked tee time, shotgun indicator, start hole
  - Links to round strings via tee sheet ID
- **Note:** Team uses "T-sheet" and "tee sheet" interchangeably

### Auto-Assign
- **Definition:** System automatically matches device to closest tee time
- **Details:**
  - Not very accurate (people don't always start at booked time)
  - System says: "Closest booking is 10:10, assuming you're that group"
  - Not widely used due to inaccuracy
- **Example:** Device enters trigger zone at 10:14, system assigns to 10:10 booking

### Manual Assign
- **Definition:** Course staff manually links device to booking
- **Details:**
  - More accurate than auto-assign
  - ~50% of courses do this
  - Creates reliable link between round and player data

### Shotgun Start
- **Definition:** All groups start simultaneously on different holes
- **Details:**
  - 18 groups start at same time (e.g., 7:00 AM)
  - Each group on a different hole (holes 1-18)
  - Siren goes off, everyone starts
  - Complex to track (round data can start at location 37 = hole 12)
  - Timeline loops around course
  - Some courses run multiple shotguns per day
  - Requires tee sheet to identify (has indicator field)

### Start Hole
- **Definition:** Which hole a group started on
- **Details:**
  - Normal rounds: usually hole 1 or 10
  - Shotgun rounds: could be any hole (1-18)
  - Important for shotgun round tracking

---

## Data Structure Terms

### Locations Array
- **Definition:** Array within round string containing all sections visited
- **Details:**
  - Each location has: hole number, hole section, section number, start time
  - Contains fix IDs for that section
  - Contains coordinates for each fix
  - Makes CSV export very long (nested data)
- **Example:** `locations[0]` = first section entered, with all its fix data

### First Fix / Last Fix
- **Definition:** Fix IDs linking to the fixes table
- **Details:**
  - `first_fix_id` = link to first fix of the round
  - `last_fix_id` = link to last fix of the round
  - Used for data lineage (tracing back to original GPS data)
  - Start time calculated from first fix minus 3 minutes

### Current Section
- **Definition:** The section where the round ended
- **Details:**
  - Usually the last section in the locations array
  - Indicates where group finished
- **Example:** Current section 55 = finished in section 55 (likely hole 18, section 4)

### Start Section / End Section
- **Definition:** First and last sections of the round
- **Details:**
  - Start section = where round began (trigger zone)
  - End section = where round finished (final green)

---

## Technical Terms

### Cached Fix
- **Definition:** Fix that was stored on device when offline, then uploaded later
- **Details:**
  - Boolean flag: `isCached` (true/false)
  - Grindr treats cached fixes differently
  - If signal gap of 5 minutes, device is placed at current location (not moved back)
  - Important for data quality

### DOP (Dilution of Precision)
- **Definition:** GPS accuracy metric
- **Details:**
  - Lower DOP = better accuracy
  - Only available from some device types
  - Values like 12, 5 indicate accuracy level
  - Team needs to clarify exact meaning with tech team

### SNR (Signal-to-Noise Ratio)
- **Definition:** GPS signal strength indicator
- **Details:**
  - Higher SNR = better signal
  - Only available from some device types
  - Used with DOP to assess GPS accuracy

### Geofence
- **Definition:** Client-drawn polygon for restricted areas
- **Details:**
  - Different from course sections
  - Used to keep people out of protected areas
  - Not used much according to team
  - Boolean flag: within geofence or not

### Projected
- **Definition:** Forecasted location when device goes offline
- **Details:**
  - Currently turned off (was causing confusion)
  - Future: show icon where device might be
  - Would use pace from first 3-4 holes to project forward
- **Note:** Not currently in use

---

## Data Lake Architecture Terms

### Bronze Layer
- **Definition:** Raw data storage (source of truth)
- **Details:**
  - Format: JSON (as-is from API)
  - Purpose: Safety net, can always restart transformations from here
  - Storage: Cheap, text-based
  - Content: Round strings dumped exactly as received

### Silver Layer
- **Definition:** Normalized, relational structure
- **Details:**
  - Format: Parquet files (columnar storage)
  - Structure: Separate tables (devices, rounds, players, tee sheets)
  - Benefits: 95% compression, efficient SQL queries, only reads relevant columns
  - Process: AWS Glue crawler builds schema, data read as relational database

### Gold Layer
- **Definition:** Pre-aggregated business metrics
- **Details:**
  - Format: Parquet files
  - Content: Pre-calculated values (e.g., average pace per course)
  - Purpose: Fast dashboard queries without real-time calculations
- **Example:** "Average pace of play for Course X = 4 hours 15 minutes" (pre-calculated)

### Partitioning
- **Definition:** Organizing data files for efficient querying
- **Details:**
  - Default: Partition by course, then by date
  - Could also partition by 9/18/27 holes
  - Critical for cost control (only reads relevant data)
  - Determines query performance and AWS costs

---

## Industry-Specific Golf Terms

### Par
- **Definition:** Expected number of strokes for a hole
- **Details:**
  - **Par 3:** Short hole, typically 3 sections
  - **Par 4:** Medium hole, typically 3 sections (tee, fairway, green)
  - **Par 5:** Long hole, typically 4 sections (tee, 2 fairways, green)
  - Used in goal time calculations

### Tee Box
- **Definition:** Starting area of each hole where players hit their first shot
- **Details:**
  - Usually section 1 of each hole
  - Goal time typically ~180 seconds (3 minutes) for first tee
  - Players wait here before starting

### Fairway
- **Definition:** Mowed area between tee box and green
- **Details:**
  - Can be one or two sections (par 4 vs par 5)
  - Where players hit their second/third shots
  - Time measured from entering fairway to entering green

### Green
- **Definition:** The putting surface around the hole
- **Details:**
  - Final section of each hole
  - Where players putt to finish the hole
  - Final green triggers round end (adds ~5 minutes for completion)

### Marshal
- **Definition:** Course staff who monitor and manage pace of play
- **Details:**
  - Used to be called "marshals" (old term)
  - Now called "player systems" or course staff
  - Have curated interactions with slow groups
  - Use data-backed conversations (don't say "slow", say "out of position")

---

## Common Abbreviations

- **TM:** Tag Marshal (the company/product)
- **T-gap:** Tee gap (time between groups)
- **DOP:** Dilution of Precision (GPS accuracy)
- **SNR:** Signal-to-Noise Ratio (GPS signal strength)
- **MDM:** Mobile Device Management (device lockdown software)
- **IMEI:** International Mobile Equipment Identity (hardware ID)
- **LoRa:** Long Range (low-power communication technology)
- **IoT:** Internet of Things (connected devices)
- **CSV:** Comma-Separated Values (data format)
- **JSON:** JavaScript Object Notation (data format)
- **API:** Application Programming Interface
- **AWS:** Amazon Web Services
- **S3:** Simple Storage Service (AWS storage)
- **Parquet:** Columnar data storage format

---

## Quick Reference: Data Flow

1. **Device** → Sends GPS fix (coordinates)
2. **Gatekeeper** (for classic devices) → Parses CSV packet, routes to central system
3. **Central System** → Identifies which course device belongs to
4. **Course Server** → Receives fix via API endpoint
5. **Grindr** → Performs geospatial calculation, determines section/zone
6. **Round String** → Created from processed fixes (first fix per section recorded)

---

## Important Notes

- **"Round" and "Round String"** are the same thing - use interchangeably
- **"T-sheet" and "Tee Sheet"** are the same thing - booking system
- **"Section" and "Zone"** are the same thing - geospatial polygons
- **Goal times are NOT reliable** - they're client-set, often unrealistic
- **T-gap is more important** than goal time for measuring actual pace adherence
- **Primary rounds** are used for reporting; secondary rounds are typically ignored
- **700 courses** = 700 separate MongoDB databases (no course ID in data)

---

*Last Updated: Based on data discovery meeting transcripts*

