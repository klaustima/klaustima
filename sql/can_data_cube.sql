-- SQL script to load CAN time series data from a CSV file and create a data cube
-- with 5 equal-width bins for each feature.
-- This example assumes a dataset with the columns: timestamp, speed, rpm, throttle

-- 1. Create table to hold raw data
CREATE TABLE can_raw(
    ts TIMESTAMP,
    speed NUMERIC,
    rpm NUMERIC,
    throttle NUMERIC
);

-- 2. Load the CSV file into the table (adjust path as needed)
COPY can_raw FROM '/path/to/can_data.csv' DELIMITER ',' CSV HEADER;

-- 3. Compute statistics and assign each value to one of five bins per feature
WITH stats AS (
    SELECT
        MIN(speed)    AS speed_min,
        MAX(speed)    AS speed_max,
        MIN(rpm)      AS rpm_min,
        MAX(rpm)      AS rpm_max,
        MIN(throttle) AS throttle_min,
        MAX(throttle) AS throttle_max
    FROM can_raw
), durations AS (
    SELECT
        cr.*, LEAD(ts) OVER (ORDER BY ts) AS next_ts
    FROM can_raw cr
), binned AS (
    SELECT
        ts,
        COALESCE(EXTRACT(EPOCH FROM next_ts - ts), 0) AS dwell_seconds,
        -- width_bucket divides the range [min, max] into equal bins
        width_bucket(speed, speed_min, speed_max, 5) AS speed_bin,
        width_bucket(rpm, rpm_min, rpm_max, 5) AS rpm_bin,
        width_bucket(throttle, throttle_min, throttle_max, 5) AS throttle_bin
    FROM durations, stats
)
-- 4. Build the dwell time cube by summing dwell seconds for each bin combination
SELECT speed_bin, rpm_bin, throttle_bin, SUM(dwell_seconds) AS dwell_time
FROM binned
GROUP BY speed_bin, rpm_bin, throttle_bin
ORDER BY speed_bin, rpm_bin, throttle_bin;
