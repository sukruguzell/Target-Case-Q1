WITH valid_segments AS (
    SELECT
        route_id,
        distance,
        recorded_at,
        LAG(distance) OVER (PARTITION BY route_id ORDER BY recorded_at) AS prev_distance,
        LAG(recorded_at) OVER (PARTITION BY route_id ORDER BY recorded_at) AS prev_time
    FROM events.navigation_records
),
distance_diff AS (
    SELECT
        route_id,
        recorded_at,
        IFNULL(TIMESTAMP_DIFF(recorded_at, prev_time, SECOND), 1) AS time_diff_sec,
        CASE
            WHEN prev_distance IS NULL OR distance >= prev_distance THEN distance - prev_distance
            ELSE 0
        END AS distance_travelled,
        CASE
            WHEN prev_distance IS NOT NULL AND distance >= prev_distance 
                 THEN (distance - prev_distance) / IFNULL(TIMESTAMP_DIFF(recorded_at, prev_time, SECOND), 1)
            ELSE 0
        END AS speed,
        prev_time
    FROM valid_segments
    WHERE prev_time IS NULL 
          OR TIMESTAMP_DIFF(recorded_at, prev_time, SECOND) > 0
),
filtered_distance AS (
    SELECT
        route_id,
        recorded_at,
        CASE 
            WHEN speed > 100 THEN 0 
            ELSE distance_travelled 
        END AS valid_distance_travelled,
        prev_time,
        time_diff_sec
    FROM distance_diff
),
zero_streaks AS (
    SELECT
        route_id,
        recorded_at,
        valid_distance_travelled,
        SUM(CASE WHEN valid_distance_travelled = 0 THEN time_diff_sec ELSE 0 END) 
        OVER (PARTITION BY route_id ORDER BY recorded_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS zero_duration
    FROM filtered_distance
),
adjusted_times AS (
    -- Identify the first change after a 10-hour (36000 seconds) streak of zeros
    SELECT
        route_id,
        MIN(CASE WHEN zero_duration >= 36000 AND valid_distance_travelled > 0 THEN recorded_at ELSE NULL END) AS adjusted_start_time
    FROM zero_streaks
    GROUP BY route_id
),
route_summary AS (
    SELECT
        fd.route_id,
        SUM(fd.valid_distance_travelled) AS total_distance,
        -- Use the adjusted start time if there's a gap, otherwise the original min time
        COALESCE(ast.adjusted_start_time, MIN(fd.recorded_at)) AS start_time,
        MAX(fd.recorded_at) AS end_time
    FROM filtered_distance fd
    LEFT JOIN adjusted_times ast ON fd.route_id = ast.route_id
    GROUP BY fd.route_id, ast.adjusted_start_time
)
SELECT
    route_id,
    total_distance,
    TIMESTAMP_DIFF(end_time, start_time, MINUTE) AS total_duration -- Duration in minutes
FROM route_summary;
