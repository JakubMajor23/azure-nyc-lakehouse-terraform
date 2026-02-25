SELECT 'S01: Silver not empty' AS test,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS total_rows
FROM silver.yellow_taxi_cleaned;

SELECT 'S02: vendor_id in {1,2}' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE vendor_id NOT IN (1, 2);

SELECT 'S03: pickup < dropoff' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE pickup_datetime >= dropoff_datetime;

SELECT 'S04: duration 1-1440 min' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE trip_duration_minutes > 1440 OR trip_duration_minutes < 1;

SELECT 'S05: passenger_count >= 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE passenger_count < 0;


SELECT 'S06: trip_distance > 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE trip_distance_miles <= 0;

SELECT 'S07: store_and_fwd Y/N' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE store_and_fwd_flag NOT IN ('Y', 'N');

SELECT 'S08: PU location 1-265' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE pickup_location_id < 1 OR pickup_location_id > 265;

SELECT 'S09: DO location 1-265' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE dropoff_location_id < 1 OR dropoff_location_id > 265;

SELECT 'S10: payment_type 0-6' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE payment_type NOT IN (0, 1, 2, 3, 4, 5, 6);

SELECT 'S11: trip_status valid/correction' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE trip_status NOT IN ('valid', 'correction');

SELECT 'S12: valid fare > 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'valid' AND fare_amount <= 0;

SELECT 'S13: valid total > 0' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'valid' AND total_amount <= 0;

SELECT 'S14: corrections negative' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'correction' AND fare_amount > 0 AND total_amount > 0;

SELECT 'S15: no NULLs in fixed cols' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE passenger_count IS NULL
   OR rate_code_id IS NULL
   OR store_and_fwd_flag IS NULL
   OR congestion_surcharge IS NULL
   OR airport_fee IS NULL
   OR cbd_congestion_fee IS NULL;

SELECT 'S16: dates in 2021-2025' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE pickup_datetime < '2021-01-01' OR pickup_datetime >= '2026-01-01';

SELECT 'S17: derived cols not NULL' AS test,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result,
    COUNT(*) AS violations
FROM silver.yellow_taxi_cleaned
WHERE trip_year IS NULL
   OR trip_month IS NULL
   OR trip_day IS NULL
   OR trip_weekday IS NULL
   OR pickup_hour IS NULL
   OR trip_duration_minutes IS NULL;

SELECT 'S18: data loss < 10%' AS test,
    CASE WHEN CAST(s.cnt AS FLOAT) / b.cnt > 0.90 THEN 'PASS' ELSE 'FAIL' END AS result,
    CONCAT(CAST(ROUND((1.0 - CAST(s.cnt AS FLOAT) / b.cnt) * 100, 2) AS VARCHAR), '% lost') AS info
FROM (SELECT COUNT(*) cnt FROM bronze.vw_yellow_taxi_raw) b,
     (SELECT COUNT(*) cnt FROM silver.yellow_taxi_cleaned) s;
