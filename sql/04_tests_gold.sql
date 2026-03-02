-- ===================
-- 1. dim_date tests
-- ===================

SELECT 'G01: dim_date not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS details
FROM gold.dim_date

UNION ALL

SELECT 'G02: dates 2021-2025' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.dim_date
WHERE full_date < '2021-01-01' OR full_date >= '2026-01-01'

UNION ALL

SELECT 'G03: no duplicate date_key',
    CASE WHEN MAX(cnt) = 1 THEN 'PASS' ELSE 'FAIL' END,
    CAST(MAX(cnt) AS VARCHAR)
FROM (SELECT date_key, COUNT(*) cnt FROM gold.dim_date GROUP BY date_key) t

UNION ALL

-- ===================
-- 2. dim_payment_type
-- ===================

SELECT 'G04: dim_payment_type count = 7' AS test,
    CASE WHEN COUNT(*) = 7 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS total_rows
FROM gold.dim_payment_type

UNION ALL

SELECT 'G05: payment_category mapped' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.dim_payment_type
WHERE payment_category NOT IN ('Credit Card', 'Cash', 'Others')

UNION ALL

-- ===================
-- 3. fact_trips
-- ===================

SELECT 'G06: fact_trips not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS total_rows
FROM gold.fact_trips

UNION ALL

SELECT 'G07: total_revenue >= 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.fact_trips
WHERE total_revenue < 0

UNION ALL

SELECT 'G08: trip_count > 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.fact_trips
WHERE trip_count <= 0

UNION ALL

SELECT 'G09: avg_trip_cost <= 1000' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.fact_trips
WHERE avg_trip_cost <= 0 OR avg_trip_cost > 1000

UNION ALL

SELECT 'G10: valid payment types' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.fact_trips
WHERE payment_key NOT IN (SELECT payment_key FROM gold.dim_payment_type)

UNION ALL

-- ===================
-- 4. fact_corrections
-- ===================

SELECT 'G11: fact_corrections not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS total_rows
FROM gold.fact_corrections

UNION ALL

SELECT 'G12: correction_count is positive' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.fact_corrections
WHERE correction_count <= 0

UNION ALL

SELECT 'G13: valid payment types (corrections)' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS violations
FROM gold.fact_corrections
WHERE payment_key NOT IN (SELECT payment_key FROM gold.dim_payment_type)

UNION ALL

-- ===================
-- Cross-layer tests
-- ===================

SELECT 'G14: Gold = Silver valid count' AS test,
    CASE WHEN ABS(g.cnt - s.cnt) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CONCAT('Gold=', CAST(g.cnt AS VARCHAR), ' Silver=', CAST(s.cnt AS VARCHAR)) AS info
FROM (SELECT ISNULL(SUM(CAST(trip_count AS BIGINT)), 0) cnt FROM gold.fact_trips) g,
     (SELECT COUNT(*) cnt FROM silver.yellow_taxi_cleaned WHERE trip_status = 'valid') s

UNION ALL

SELECT 'G15: Gold = Silver correction count' AS test,
    CASE WHEN ABS(g.cnt - s.cnt) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CONCAT('Gold=', CAST(g.cnt AS VARCHAR), ' Silver=', CAST(s.cnt AS VARCHAR)) AS info
FROM (SELECT ISNULL(SUM(CAST(correction_count AS BIGINT)), 0) cnt FROM gold.fact_corrections) g,
     (SELECT COUNT(*) cnt FROM silver.yellow_taxi_cleaned WHERE trip_status = 'correction') s

UNION ALL

SELECT 'G16: Gold revenue = Silver valid revenue' AS test,
    CASE WHEN ABS(g.rev - s.rev) < 1 THEN 'PASS' ELSE 'FAIL' END AS result,
    CONCAT('Gold=', CAST(g.rev AS VARCHAR), ' Silver=', CAST(s.rev AS VARCHAR)) AS info
FROM (SELECT ISNULL(SUM(total_revenue), 0) rev FROM gold.fact_trips) g,
     (SELECT ISNULL(SUM(total_amount), 0) rev FROM silver.yellow_taxi_cleaned WHERE trip_status = 'valid') s;