import argparse, json, sys
import psycopg2
from psycopg2.extras import RealDictCursor

def connect(args):
    dsn = f"host={args.host} port={args.port} dbname={args.dbname} user={args.user} password={args.password}"
    return psycopg2.connect(dsn)

def rows_to_json(name, desc, rows):
    return json.dumps({
        "query": name,
        "description": desc,
        "results": rows,
        "count": len(rows)
    }, default=str, ensure_ascii=False, indent=2)

# Q1: List all stops on Route 20 in order
def q1(conn):
    sql = """
    SELECT s.stop_name, ls.sequence_number AS sequence, ls.time_offset_minutes AS time_offset
    FROM line_stops ls
    JOIN lines l   ON l.line_id = ls.line_id
    JOIN stops s   ON s.stop_id = ls.stop_id
    WHERE l.line_name = 'Route 20'
    ORDER BY ls.sequence_number;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q2: Trips during morning rush (7-9 AM)
def q2(conn):
    sql = """
    SELECT t.trip_id, l.line_name, t.scheduled_departure
    FROM trips t
    JOIN lines l ON l.line_id = t.line_id
    WHERE (t.scheduled_departure::time) >= TIME '07:00:00'
      AND (t.scheduled_departure::time) <  TIME '09:00:00'
    ORDER BY t.scheduled_departure;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q3: Transfer stops (stops on 2+ routes)
def q3(conn):
    sql = """
    SELECT s.stop_name, COUNT(DISTINCT ls.line_id) AS line_count
    FROM line_stops ls
    JOIN stops s ON s.stop_id = ls.stop_id
    GROUP BY s.stop_id, s.stop_name
    HAVING COUNT(DISTINCT ls.line_id) >= 2
    ORDER BY line_count DESC, s.stop_name;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q4: Complete route for trip T0001
def q4(conn):
    sql = """
    WITH trip_line AS (
      SELECT line_id FROM trips WHERE trip_id = 'T0001'
    )
    SELECT s.stop_name, ls.sequence_number AS sequence, ls.time_offset_minutes AS time_offset
    FROM line_stops ls
    JOIN trip_line tl ON tl.line_id = ls.line_id
    JOIN stops s ON s.stop_id = ls.stop_id
    ORDER BY ls.sequence_number;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q5: Routes serving both Wilshire / Veteran and Le Conte / Broxton
def q5(conn):
    sql = """
    WITH target_stops AS (
      SELECT stop_id FROM stops WHERE stop_name IN ('Wilshire / Veteran','Le Conte / Broxton')
    ),
    lines_with_both AS (
      SELECT ls.line_id
      FROM line_stops ls
      WHERE ls.stop_id IN (SELECT stop_id FROM target_stops)
      GROUP BY ls.line_id
      HAVING COUNT(DISTINCT ls.stop_id) = 2
    )
    SELECT l.line_name
    FROM lines_with_both b JOIN lines l ON l.line_id = b.line_id
    ORDER BY l.line_name;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q6: Average ridership by line
def q6(conn):
    sql = """
    SELECT l.line_name,
           AVG(se.passengers_on + se.passengers_off)::numeric(10,2) AS avg_passengers
    FROM stop_events se
    JOIN trips t ON t.trip_id = se.trip_id
    JOIN lines l ON l.line_id = t.line_id
    GROUP BY l.line_name
    ORDER BY l.line_name;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q7: Top 10 busiest stops (total_activity)
def q7(conn):
    sql = """
    SELECT s.stop_name,
           SUM(se.passengers_on + se.passengers_off) AS total_activity
    FROM stop_events se
    JOIN stops s ON s.stop_id = se.stop_id
    GROUP BY s.stop_id, s.stop_name
    ORDER BY total_activity DESC, s.stop_name
    LIMIT 10;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q8: Count delays by line (> 2 minutes late)
def q8(conn):
    sql = """
    SELECT l.line_name, COUNT(*) AS delay_count
    FROM stop_events se
    JOIN trips t ON t.trip_id = se.trip_id
    JOIN lines l ON l.line_id = t.line_id
    WHERE se.actual_time > se.scheduled_time + INTERVAL '2 minutes'
    GROUP BY l.line_name
    ORDER BY delay_count DESC, l.line_name;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q9: Trips with 3+ delayed stops
def q9(conn):
    sql = """
    SELECT se.trip_id, COUNT(*) AS delayed_stop_count
    FROM stop_events se
    WHERE se.actual_time > se.scheduled_time + INTERVAL '2 minutes'
    GROUP BY se.trip_id
    HAVING COUNT(*) >= 3
    ORDER BY delayed_stop_count DESC, se.trip_id;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

# Q10: Stops with above-average ridership (boardings only)
def q10(conn):
    sql = """
    WITH per_stop AS (
      SELECT s.stop_id, s.stop_name,
             SUM(se.passengers_on) AS total_boardings
      FROM stop_events se
      JOIN stops s ON s.stop_id = se.stop_id
      GROUP BY s.stop_id, s.stop_name
    ),
    avg_board AS (
      SELECT AVG(total_boardings) AS avg_b FROM per_stop
    )
    SELECT p.stop_name, p.total_boardings
    FROM per_stop p, avg_board a
    WHERE p.total_boardings > a.avg_b
    ORDER BY p.total_boardings DESC, p.stop_name;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql); return list(cur.fetchall())

QUERIES = {
    "Q1": ("Red Line stops in order (Route 20)", q1),
    "Q2": ("Trips during morning rush (7-9 AM)", q2),
    "Q3": ("Transfer stops (2+ routes)", q3),
    "Q4": ("Complete route for trip T0001", q4),
    "Q5": ("Routes serving both target stops", q5),
    "Q6": ("Average ridership by line", q6),
    "Q7": ("Top 10 busiest stops", q7),
    "Q8": ("Count delays by line (>2 min)", q8),
    "Q9": ("Trips with 3+ delayed stops", q9),
    "Q10": ("Stops with above-average ridership", q10),
}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--dbname", required=True)
    p.add_argument("--user", default="transit")
    p.add_argument("--password", default="transit123")
    p.add_argument("--query", choices=list(QUERIES.keys()))
    p.add_argument("--all", action="store_true")
    p.add_argument("--format", choices=["table", "json"], default="table")
    args = p.parse_args()

    if not args.all and not args.query:
        print("Please specify --query Qn or --all", file=sys.stderr)
        sys.exit(2)

    with connect(args) as conn:
        if args.all:
            for key in QUERIES:
                title, fn = QUERIES[key]
                rows = fn(conn)
                if args.format == "json":
                    print(rows_to_json(key, title, rows))
                else:
                    print(f"\n== {key}: {title} ==")
                    for r in rows: print(dict(r))
            return

        title, fn = QUERIES[args.query]
        rows = fn(conn)
        if args.format == "json":
            print(rows_to_json(args.query, title, rows))
        else:
            print(f"== {args.query}: {title} ==")
            for r in rows: print(dict(r))

if __name__ == "__main__":
    main()
