SELECT 'S01: Silver not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    CAST(COUNT(*) AS VARCHAR) AS details
FROM silver.yellow_taxi_cleaned

UNION ALL

SELECT 'S02: vendor_id in {1,2}',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE vendor_id NOT IN (1, 2)

UNION ALL

SELECT 'S03: pickup < dropoff',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE pickup_datetime >= dropoff_datetime

UNION ALL

SELECT 'S04: duration 1-1440 min',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE trip_duration_minutes > 1440 OR trip_duration_minutes < 1

UNION ALL

SELECT 'S05: passenger_count >= 0',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE passenger_count < 0

UNION ALL

SELECT 'S06: trip_distance > 0',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE trip_distance_miles <= 0

UNION ALL

SELECT 'S07: store_and_fwd Y/N',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE store_and_fwd_flag NOT IN ('Y', 'N')

UNION ALL

SELECT 'S08: PU location 1-265',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE pickup_location_id < 1 OR pickup_location_id > 265

UNION ALL

SELECT 'S09: DO location 1-265',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE dropoff_location_id < 1 OR dropoff_location_id > 265

UNION ALL

SELECT 'S10: payment_type 0-6',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE payment_type NOT IN (0, 1, 2, 3, 4, 5, 6)

UNION ALL

SELECT 'S11: trip_status valid/correction',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE trip_status NOT IN ('valid', 'correction')

UNION ALL

SELECT 'S12: valid fare > 0',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'valid' AND fare_amount <= 0

UNION ALL

SELECT 'S13: valid total > 0',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'valid' AND total_amount <= 0

UNION ALL

SELECT 'S14: corrections negative',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'correction' AND fare_amount > 0 AND total_amount > 0

UNION ALL

SELECT 'S15: no NULLs in fixed cols',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE passenger_count IS NULL
   OR rate_code_id IS NULL
   OR store_and_fwd_flag IS NULL
   OR congestion_surcharge IS NULL
   OR airport_fee IS NULL
   OR cbd_congestion_fee IS NULL

UNION ALL

SELECT 'S16: dates in 2021-2025',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE pickup_datetime < '2021-01-01' OR pickup_datetime >= '2026-01-01'

UNION ALL

SELECT 'S17: derived cols not NULL',
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(COUNT(*) AS VARCHAR)
FROM silver.yellow_taxi_cleaned
WHERE trip_year IS NULL
   OR trip_month IS NULL
   OR trip_day IS NULL
   OR trip_weekday IS NULL
   OR pickup_hour IS NULL
   OR trip_duration_minutes IS NULL

UNION ALL

SELECT 'S18: data loss < 10%',
    CASE WHEN CAST(s.cnt AS FLOAT) / b.cnt > 0.90 THEN 'PASS' ELSE 'FAIL' END,
    CONCAT(CAST(ROUND((1.0 - CAST(s.cnt AS FLOAT) / b.cnt) * 100, 2) AS VARCHAR), '% lost')
FROM (SELECT COUNT(*) cnt FROM bronze.vw_yellow_taxi_raw) b,
     (SELECT COUNT(*) cnt FROM silver.yellow_taxi_cleaned) s;