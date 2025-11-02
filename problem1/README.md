# Metro Transit Database
## 1. Schema Design Decisions

### Natural Keys vs Surrogate Keys
We use **surrogate keys (synthetic IDs)** such as `line_id SERIAL`, `stop_id SERIAL`, and `trip_id VARCHAR` rather than natural names as primary keys.

Reasons:
- Stability: Names (e.g., street/station names) may change, while synthetic IDs remain stable.
- Efficiency: Surrogate keys are small, indexable, and optimize join performance.
- Flexibility: Allows separate uniqueness enforcement on human-readable attributes (e.g., `line_name UNIQUE`).

Some tables (such as `trips`) use **natural keys** when they already provide globally unique identifiers.

## 2. Constraints Added

| Table | Constraint Type                                          | Example                                      | Purpose |
|------|----------------------------------------------------------|----------------------------------------------|---------|
| **lines** | `CHECK (vehicle_type IN ('bus','rail'))`                 | Restricts valid vehicle modes                | Prevents invalid transportation modes |
| **stops** | `UNIQUE (stop_name)`                                     | Ensures one record per named stop            | Avoids accidental duplicates |
| **line_stops** | `UNIQUE (line_id, sequence_number)`                      | Ensures unique stop order per line           | Preserves correct stop sequencing |
| **trips** | `UNIQUE (line_id, scheduled_departure, vehicle_id)`      | Prevents duplicate trip definitions          | Maintains trip uniqueness |
| **stop_events** | `CHECK (passengers_on >= 0 AND passengers_off >= 0)`     | No negative ridership counts                 | Ensures data sanity |

These constraints enforce logical correctness throughout the dataset.


## 3. Most Difficult Query

**Query:** Identify trips where **three or more stops** were delayed by at least **2 minutes**.

```sql
SELECT trip_id, COUNT(*) AS delayed_stop_count
FROM stop_events
WHERE actual_time > scheduled_time + interval '2 minutes'
GROUP BY trip_id
HAVING COUNT(*) >= 3;
```

All requires **timestamp arithmetic** to compare scheduled vs. actual times.
Need to uses **aggregation** and **HAVING** to count delayed stops per trip.
Need to ensures proper performance using **indexes on timestamp fields**.

### 4. Foreign Key Enforcement

| Relationship                          | Enforced Constraint | Prevented Invalid Data |
|---------------------------------------|--------------------|------------------------|
| `line_stops.line_id : lines.line_id`  | Every stop must belong to an existing line | Avoids orphaned stop entries |
| `line_stops.stop_id : stops.stop_id`  | Stop must exist in `stops` table | Prevents missing referenced stops |
| `trips.line_id :lines.line_id`        | Each trip must run on a valid line | Avoids logically invalid trips |
| `stop_events.trip_id : trips.trip_id` | Each stop event must reference an actual trip | Prevents ghost/untracked events |

If a trip or stop is missing, **PostgreSQL rejects** inserts referencing them.

### 5. Why SQL Fits This Domain

- **Relational Structure:** Transit data forms natural relationships among lines, stops, trips, and events.
- **Data Integrity:** ACID guarantees ensure correctness of schedules and ridership tracking.
- **Declarative Query Power:** Supports analytics such as delay patterns and congestion trends.
- **Constraint Enforcement:** FKs and CHECKs ensure logically valid operational data.
- **Extensible Schema:** New fields (e.g., GPS locations, delay causes) can be added without redesign.

SQL is therefore **well-suited** for modeling and querying public transit systems.

