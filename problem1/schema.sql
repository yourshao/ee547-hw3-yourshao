-- ============================
-- Metro Transit Schema (Part A)
-- ============================

-- Recreate cleanly for local dev
DROP TABLE IF EXISTS stop_events     CASCADE;
DROP TABLE IF EXISTS trips           CASCADE;
DROP TABLE IF EXISTS line_stops      CASCADE;
DROP TABLE IF EXISTS stops           CASCADE;
DROP TABLE IF EXISTS lines           CASCADE;

-- ----------------------------
-- Lines: e.g., "Route 2"
-- ----------------------------
CREATE TABLE lines (
                       line_id      SERIAL PRIMARY KEY,
                       line_name    VARCHAR(50) NOT NULL UNIQUE,
                       vehicle_type VARCHAR(10) NOT NULL,
                       CONSTRAINT vehicle_type_chk
                           CHECK (vehicle_type IN ('rail', 'bus'))
);

-- ----------------------------
-- Stops: distinct physical stops
-- ----------------------------
CREATE TABLE stops (
                       stop_id    SERIAL PRIMARY KEY,
                       stop_name  VARCHAR(100) NOT NULL UNIQUE,
                       latitude   DOUBLE PRECISION NOT NULL,
                       longitude  DOUBLE PRECISION NOT NULL,
                       CONSTRAINT lat_range_chk  CHECK (latitude  BETWEEN -90  AND 90),
                       CONSTRAINT lon_range_chk  CHECK (longitude BETWEEN -180 AND 180)
);

-- -----------------------------------------------------
-- Line Stops: ordered list of stops for each line
-- sequence starts at 1; time_offset_minutes >= 0
-- -----------------------------------------------------
CREATE TABLE line_stops (
                            line_id              INTEGER NOT NULL REFERENCES lines(line_id) ON UPDATE CASCADE ON DELETE RESTRICT,
                            stop_id              INTEGER NOT NULL REFERENCES stops(stop_id) ON UPDATE CASCADE ON DELETE RESTRICT,
                            sequence_number      INTEGER NOT NULL,
                            time_offset_minutes  INTEGER NOT NULL DEFAULT 0,
    -- A line cannot have the same stop twice, and each sequence index per line is unique
                            CONSTRAINT line_stops_pk PRIMARY KEY (line_id, sequence_number),
                            CONSTRAINT line_stop_unique UNIQUE (line_id, stop_id),
                            CONSTRAINT seq_positive_chk CHECK (sequence_number >= 1),
                            CONSTRAINT offset_nonneg_chk CHECK (time_offset_minutes >= 0)
);

-- Helpful index for listing stops of a given line in order
CREATE INDEX idx_line_stops_line_seq ON line_stops(line_id, sequence_number);
-- Helpful index for reverse lookup: which lines serve a stop?
CREATE INDEX idx_line_stops_stop ON line_stops(stop_id);

-- -----------------------------------------
-- Trips: scheduled vehicle runs on a line
-- Keep CSV's trip_id (e.g., "T0001") as PK
-- -----------------------------------------
CREATE TABLE trips (
                       trip_id              VARCHAR(20) PRIMARY KEY,
                       line_id              INTEGER NOT NULL REFERENCES lines(line_id) ON UPDATE CASCADE ON DELETE RESTRICT,
                       scheduled_departure  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                       vehicle_id           VARCHAR(30) NOT NULL,
    -- Avoid duplicate trip definitions
                       CONSTRAINT trips_unique_sched UNIQUE (line_id, scheduled_departure, vehicle_id)
);

-- Helpful index for typical filters/join patterns
CREATE INDEX idx_trips_line ON trips(line_id);
CREATE INDEX idx_trips_departure ON trips(scheduled_departure);

-- -----------------------------------------------------------------
-- Stop Events: actual/scheduled times and ridership during a trip
-- -----------------------------------------------------------------
CREATE TABLE stop_events (
                             trip_id         VARCHAR(20) NOT NULL REFERENCES trips(trip_id) ON UPDATE CASCADE ON DELETE CASCADE,
                             stop_id         INTEGER NOT NULL REFERENCES stops(stop_id) ON UPDATE CASCADE ON DELETE RESTRICT,
                             scheduled_time  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                             actual_time     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                             passengers_on   INTEGER NOT NULL DEFAULT 0,
                             passengers_off  INTEGER NOT NULL DEFAULT 0,
                             CONSTRAINT passengers_on_nonneg_chk  CHECK (passengers_on  >= 0),
                             CONSTRAINT passengers_off_nonneg_chk CHECK (passengers_off >= 0),
    -- Per trip, each stop should appear at most once at a given scheduled timestamp
                             CONSTRAINT stop_events_pk PRIMARY KEY (trip_id, stop_id, scheduled_time)
);

-- Commonly used indexes for analytics
CREATE INDEX idx_stop_events_stop ON stop_events(stop_id);
CREATE INDEX idx_stop_events_sched ON stop_events(scheduled_time);
CREATE INDEX idx_stop_events_actual ON stop_events(actual_time);
