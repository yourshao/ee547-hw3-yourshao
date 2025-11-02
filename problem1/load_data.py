import argparse
import csv
import datetime as dt
import os
import sys
import psycopg2
from psycopg2.extras import execute_batch

# -----------------------------
# Utilities
# -----------------------------

def log(msg):
    print(msg, flush=True)

def iso_to_ts(s):
    # CSV: "YYYY-MM-DD HH:MM:SS"
    return dt.datetime.fromisoformat(s.strip())

def read_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def run_sql_file(conn, path):
    with conn.cursor() as cur, open(path, 'r', encoding='utf-8') as f:
        cur.execute(f.read())

def get_line_id_map(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT line_id, line_name FROM lines;")
        return {name: _id for _id, name in cur.fetchall()}

def get_stop_id_map(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT stop_id, stop_name FROM stops;")
        return {name: _id for _id, name in cur.fetchall()}

# -----------------------------
# Loaders
# -----------------------------

def load_lines(conn, path):
    rows = read_csv(path)
    with conn.cursor() as cur:
        sql = """
            INSERT INTO lines (line_name, vehicle_type)
            VALUES (%s, %s)
            ON CONFLICT (line_name) DO NOTHING;
        """
        data = [(r['line_name'].strip(), r['vehicle_type'].strip()) for r in rows]
        execute_batch(cur, sql, data, page_size=200)
    return len(rows)

def load_stops(conn, path):
    rows = read_csv(path)
    with conn.cursor() as cur:
        sql = """
            INSERT INTO stops (stop_name, latitude, longitude)
            VALUES (%s, %s, %s)
            ON CONFLICT (stop_name) DO NOTHING;
        """
        data = []
        for r in rows:
            data.append((
                r['stop_name'].strip(),
                float(r['latitude']),
                float(r['longitude'])
            ))
        execute_batch(cur, sql, data, page_size=500)
    return len(rows)

def load_line_stops(conn, path):
    rows = read_csv(path)
    line_map = get_line_id_map(conn)
    stop_map = get_stop_id_map(conn)

    missing_lines, missing_stops = set(), set()
    to_insert = []

    for r in rows:
        line_name = r['line_name'].strip()
        stop_name = r['stop_name'].strip()
        seq = int(r['sequence'])
        offset = int(r['time_offset'])

        line_id = line_map.get(line_name)
        stop_id = stop_map.get(stop_name)

        if line_id is None:
            missing_lines.add(line_name)
            continue
        if stop_id is None:
            missing_stops.add(stop_name)
            continue

        to_insert.append((line_id, stop_id, seq, offset))

    if missing_lines:
        log(f"WARNING line_stops missing lines: {sorted(missing_lines)}")
    if missing_stops:
        log(f"WARNING line_stops missing stops: {sorted(missing_stops)}")

    with conn.cursor() as cur:
        sql = """
            INSERT INTO line_stops (line_id, stop_id, sequence_number, time_offset_minutes)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        execute_batch(cur, sql, to_insert, page_size=500)
    return len(to_insert)

def load_trips(conn, path):
    rows = read_csv(path)
    line_map = get_line_id_map(conn)

    to_insert, missing_lines = [], set()
    for r in rows:
        trip_id = r['trip_id'].strip()
        line_name = r['line_name'].strip()
        sched_depart = iso_to_ts(r['scheduled_departure'])
        vehicle_id = r['vehicle_id'].strip()

        line_id = line_map.get(line_name)
        if line_id is None:
            missing_lines.add(line_name)
            continue

        to_insert.append((trip_id, line_id, sched_depart, vehicle_id))

    if missing_lines:
        log(f"WARNING trips missing lines: {sorted(missing_lines)}")

    with conn.cursor() as cur:
        sql = """
            INSERT INTO trips (trip_id, line_id, scheduled_departure, vehicle_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (trip_id) DO NOTHING;
        """
        execute_batch(cur, sql, to_insert, page_size=500)
    return len(to_insert)

def load_stop_events(conn, path):
    rows = read_csv(path)
    stop_map = get_stop_id_map(conn)

    to_insert, missing_stops = [], set()
    for r in rows:
        trip_id = r['trip_id'].strip()
        stop_name = r['stop_name'].strip()
        stop_id = stop_map.get(stop_name)
        if stop_id is None:
            missing_stops.add(stop_name)
            continue

        scheduled = iso_to_ts(r['scheduled'])
        actual = iso_to_ts(r['actual'])
        on_cnt = int(r['passengers_on'])
        off_cnt = int(r['passengers_off'])

        to_insert.append((trip_id, stop_id, scheduled, actual, on_cnt, off_cnt))

    if missing_stops:
        log(f"WARNING stop_events missing stops: {sorted(missing_stops)}")

    with conn.cursor() as cur:
        sql = """
            INSERT INTO stop_events
            (trip_id, stop_id, scheduled_time, actual_time, passengers_on, passengers_off)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trip_id, stop_id, scheduled_time) DO NOTHING;
        """
        execute_batch(cur, sql, to_insert, page_size=1000)
    return len(to_insert)

# -----------------------------
# Main
# -----------------------------

def main():
    parser = argparse.ArgumentParser(description="Load transit CSV data into PostgreSQL.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)

    # 文件路径参数：schema 在当前 problem1 下；data 在上层 ../data
    parser.add_argument("--schema", default="schema.sql",
                        help="Path to schema.sql (default: ./schema.sql)")
    parser.add_argument("--datadir", required=True,
                        help="Path to the CSV folder (e.g., ../data)")

    args = parser.parse_args()

    # 解决相对路径：相对于当前脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.abspath(os.path.join(script_dir, args.schema))
    datadir = os.path.abspath(os.path.join(script_dir, args.datadir))

    files = {
        "lines":        os.path.join(datadir, "lines.csv"),
        "stops":        os.path.join(datadir, "stops.csv"),
        "line_stops":   os.path.join(datadir, "line_stops.csv"),
        "trips":        os.path.join(datadir, "trips.csv"),
        "stop_events":  os.path.join(datadir, "stop_events.csv"),
    }

    # 检查文件是否存在
    missing = [k for k, p in files.items() if not os.path.exists(p)]
    if missing:
        log(f"ERROR missing CSV files for: {missing}")
        for k in missing:
            log(f" - expected: {files[k]}")
        sys.exit(1)
    if not os.path.exists(schema_path):
        log(f"ERROR schema.sql not found: {schema_path}")
        sys.exit(1)

    # 连接数据库
    dsn = f"host={args.host} port={args.port} dbname={args.dbname} user={args.user} password={args.password}"
    log(f"Connected to {args.dbname}@{args.host}")
    with psycopg2.connect(dsn) as conn:
        conn.autocommit = False
        try:
            # 建表
            log("Creating schema...")
            run_sql_file(conn, schema_path)
            conn.commit()
            log("Tables created: lines, stops, line_stops, trips, stop_events\n")

            # 依赖顺序加载
            counts = {}

            log(f"Loading {files['lines']}...")
            counts['lines'] = load_lines(conn, files['lines']); conn.commit()
            log(f" -> {counts['lines']} rows")

            log(f"Loading {files['stops']}...")
            counts['stops'] = load_stops(conn, files['stops']); conn.commit()
            log(f" -> {counts['stops']} rows")

            log(f"Loading {files['line_stops']}...")
            counts['line_stops'] = load_line_stops(conn, files['line_stops']); conn.commit()
            log(f" -> {counts['line_stops']} rows")

            log(f"Loading {files['trips']}...")
            counts['trips'] = load_trips(conn, files['trips']); conn.commit()
            log(f" -> {counts['trips']} rows")

            log(f"Loading {files['stop_events']}...")
            counts['stop_events'] = load_stop_events(conn, files['stop_events']); conn.commit()
            log(f" -> {counts['stop_events']} rows")

            total = sum(counts.values())
            log("\nSummary")
            for k in ["lines", "stops", "line_stops", "trips", "stop_events"]:
                log(f" {k:12s}: {counts[k]}")
            log(f"\nTotal: {total} rows loaded")

        except Exception as e:
            conn.rollback()
            log(f"ERROR: {e}")
            sys.exit(2)

if __name__ == "__main__":
    main()
