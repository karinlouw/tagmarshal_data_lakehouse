# Bronze Layer Schema

Raw data landing zone - stores CSV/JSON exactly as received from source.

## Storage Location

```
s3://tm-lakehouse-landing/
└── course_id={course_id}/
    └── ingest_date={YYYY-MM-DD}/
        └── {filename}.csv (or .json)
```

## File Formats

#Todo: What about the nine holes?
### CSV Format (Flattened)

| Column | Type | Description |
|--------|------|-------------|
| `_id` | STRING | Unique round identifier (MongoDB ObjectId) |
| `course` | STRING | Course name from source system |
| `locations[N].startTime` | STRING | ISO timestamp for location N |
| `locations[N].hole` | INTEGER | Hole number at location N |
| `locations[N].section` | INTEGER | Section number at location N |
| `locations[N].pace` | DOUBLE | Pace value at location N |
| `locations[N].paceGap` | DOUBLE | Gap to group ahead |
| `locations[N].positionalGap` | DOUBLE | Positional gap |
| `locations[N].latitude` | DOUBLE | GPS latitude |
| `locations[N].longitude` | DOUBLE | GPS longitude |
| `locations[N].batteryPercentage` | DOUBLE | Device battery level |
| `locations[N].isCache` | BOOLEAN | Offline data flag |
| `startHole` | INTEGER | Round start hole |
| `goalTime` | INTEGER | Target time (seconds) |
| `isComplete` | BOOLEAN | Round completion flag |

**Note:** N ranges from 0 to ~52 (max 53 locations per round).

### JSON Format (MongoDB Export)

```json
{
  "_id": {"$oid": "6861b0ba410113f1d4ef2309"},
  "course": "bradshawfarmgc",
  "locations": [
    {
      "startTime": {"$date": "2025-06-28T14:32:15Z"},
      "hole": 1,
      "section": 1,
      "pace": 0.0,
      "latitude": 34.15306,
      "longitude": -84.44809
    }
  ],
  "startHole": 1,
  "goalTime": 15840
}
```
## Validation Rules

**Minimal validation to preserve all data:**

1. File must contain `_id` column/field (required for round identification)
2. File must contain `course` column/field (required for partitioning)
3. File must have at least one data row/record

**What we DON'T require:**
- ❌ `locations[0].startTime` - Not required (handled by Silver ETL)
- ❌ `locations` array - Not required (empty rounds are valid)
- ❌ Any specific location fields - All handled by Silver ETL

**Why minimal validation?**
- We want to preserve ALL data, including edge cases
- Empty rounds, missing timestamps, NULL values are all valid
- The Silver ETL handles missing fields and NULL values gracefully

## Data Quality

- **No transformation** - Data stored exactly as received (byte-for-byte copy)
# Todo: we need to make sure this checks properly
- **Idempotent upload** - Re-uploading same file skips if exists (prevents duplicates)
- **Format detection** - Automatic CSV vs JSON detection based on extension/content

## Related Commands

```bash
just bronze-upload course_id=bradshawfarmgc input=data/rounds.csv
```

