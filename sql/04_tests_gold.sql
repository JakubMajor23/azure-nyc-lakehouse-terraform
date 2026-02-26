-- daily_revenue_summary

SELECT 'G01: daily_revenue not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS details
FROM gold.daily_revenue_summary

UNION ALL

SELECT 'G02: total_revenue >= 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.daily_revenue_summary
WHERE total_revenue < 0

UNION ALL

SELECT 'G03: total_trips > 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.daily_revenue_summary
WHERE total_trips <= 0

UNION ALL

SELECT 'G04: avg_trip_cost 1-200' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.daily_revenue_summary
WHERE avg_trip_cost < 1 OR avg_trip_cost > 200

UNION ALL

SELECT 'G05: dates 2021-2025' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.daily_revenue_summary
WHERE trip_date < '2021-01-01' OR trip_date >= '2026-01-01'

UNION ALL

SELECT 'G06: no duplicate dates' AS test,
    CASE WHEN MAX(cnt) = 1 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(MAX(cnt) AS VARCHAR) AS max_dupes
FROM (SELECT trip_date, COUNT(*) cnt FROM gold.daily_revenue_summary GROUP BY trip_date) t

UNION ALL

-- popular_zones

SELECT 'G07: popular_zones not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS total_rows
FROM gold.popular_zones

UNION ALL

SELECT 'G08: zone locations 1-265' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.popular_zones
WHERE pickup_location_id < 1 OR pickup_location_id > 265
   OR dropoff_location_id < 1 OR dropoff_location_id > 265

UNION ALL

SELECT 'G09: zone trip_count > 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.popular_zones
WHERE trip_count <= 0

UNION ALL

-- hourly_patterns

SELECT 'G10: hourly_patterns not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS total_rows
FROM gold.hourly_patterns

UNION ALL

SELECT 'G11: pickup_hour 0-23' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.hourly_patterns
WHERE pickup_hour < 0 OR pickup_hour > 23

UNION ALL

SELECT 'G12: hourly trip_count > 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.hourly_patterns
WHERE trip_count <= 0

UNION ALL

-- corrections_summary

SELECT 'G13: corrections not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS total_rows
FROM gold.corrections_summary

UNION ALL

SELECT 'G14: corrections avg fare < 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS non_negative
FROM gold.corrections_summary
WHERE avg_refunded_fare > 0

UNION ALL

-- Cross-layer consistency

SELECT 'G15: Gold = Silver valid count' AS test,
    CASE WHEN ABS(g.cnt - s.cnt) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CONCAT('Gold=', g.cnt, ' Silver=', s.cnt) AS info
FROM (SELECT SUM(total_trips) cnt FROM gold.daily_revenue_summary) g,
     (SELECT COUNT(*) cnt FROM silver.yellow_taxi_cleaned WHERE trip_status = 'valid') s

UNION ALL

SELECT 'G16: Gold = Silver correction count' AS test,
    CASE WHEN ABS(g.cnt - s.cnt) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CONCAT('Gold=', g.cnt, ' Silver=', s.cnt) AS info
FROM (SELECT SUM(correction_count) cnt FROM gold.corrections_summary) g,
     (SELECT COUNT(*) cnt FROM silver.yellow_taxi_cleaned WHERE trip_status = 'correction') s

UNION ALL

SELECT 'G17: Gold revenue = Silver revenue' AS test,
    CASE WHEN ABS(g.rev - s.rev) < 1 THEN 'PASS' ELSE 'FAIL' END AS result,
    CONCAT('Gold=', CAST(g.rev AS VARCHAR), ' Silver=', CAST(s.rev AS VARCHAR)) AS info
FROM (SELECT SUM(total_revenue) rev FROM gold.daily_revenue_summary) g,
     (SELECT SUM(total_amount) rev FROM silver.yellow_taxi_cleaned WHERE trip_status = 'valid') s;