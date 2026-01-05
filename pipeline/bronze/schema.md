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
# Todo: how were these validation rules determined? Do we have any empty columns for those fields?
## Validation Rules

1. File must contain `_id` column/field
2. File must contain `course` column/field
3. File must have at least one `locations[0].startTime` entry
4. File must have at least one data row/record

## Data Quality

- **No transformation** - data stored exactly as received
# Todo: we need to make sure this checks properly
- **Idempotent upload** - re-uploading same file skips if exists 
- **Format detection** - automatic CSV vs JSON detection

## Related Commands

```bash
just bronze-upload course_id=bradshawfarmgc input=data/rounds.csv
```

