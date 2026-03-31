-- Bronze profiling.
-- Run this after 00_setup.sql to inspect raw data quality before cleaning.
-- The outputs from this script explain the Silver filtering and COALESCE rules.

USE nyc_taxi_dwh;
GO

-- Keep the raw Bronze view here too so profiling can run on its own.
CREATE OR ALTER VIEW bronze.vw_yellow_taxi_raw
AS
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    COALESCE(airport_fee_lower, airport_fee_upper) AS airport_fee,
    cbd_congestion_fee,
    result.filename()  AS source_file,
    result.filepath(1) AS source_year
FROM
    OPENROWSET(
        BULK 'yellow_tripdata/*/*.parquet',
        DATA_SOURCE = 'datalake_bronze',
        FORMAT = 'PARQUET'
    )
    WITH (
        VendorID                INT,
        tpep_pickup_datetime    DATETIME2,
        tpep_dropoff_datetime   DATETIME2,
        passenger_count         VARCHAR(20),
        trip_distance           FLOAT,
        RatecodeID              VARCHAR(20),
        store_and_fwd_flag      VARCHAR(1),
        PULocationID            INT,
        DOLocationID            INT,
        payment_type            VARCHAR(20),
        fare_amount             FLOAT,
        extra                   FLOAT,
        mta_tax                 FLOAT,
        tip_amount              FLOAT,
        tolls_amount            FLOAT,
        improvement_surcharge   FLOAT,
        total_amount            FLOAT,
        congestion_surcharge    FLOAT,
        airport_fee_lower       FLOAT '$.airport_fee',
        airport_fee_upper       FLOAT '$.Airport_fee',
        cbd_congestion_fee      FLOAT
    ) AS result;
GO

CREATE OR ALTER PROCEDURE bronze.sp_run_profiling
AS
BEGIN

-- 1. Quick overview: volume, date range, files and years.
SELECT
    '1. OVERVIEW' AS section,
    COUNT(*)                                        AS total_records,
    MIN(tpep_pickup_datetime)                       AS earliest_pickup,
    MAX(tpep_pickup_datetime)                       AS latest_pickup,
    COUNT(DISTINCT source_year)                     AS year_count,
    COUNT(DISTINCT source_file)                     AS file_count
FROM bronze.vw_yellow_taxi_raw;

-- 2. Volume split by year.
SELECT
    '2. RECORDS PER YEAR' AS section,
    source_year,
    COUNT(*)              AS record_count,
    CAST(ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS DECIMAL(5,2)) AS pct_of_total
FROM bronze.vw_yellow_taxi_raw
GROUP BY source_year
ORDER BY source_year;

-- 3. NULL check for columns used later in Silver.
SELECT
    '3. NULL ANALYSIS' AS section,
    COUNT(*) AS total_rows,

    -- Columns with noticeable NULL rates
    SUM(CASE WHEN passenger_count IS NULL    THEN 1 ELSE 0 END) AS null_passenger_count,
    CAST(ROUND(SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_passenger,

    SUM(CASE WHEN RatecodeID IS NULL         THEN 1 ELSE 0 END) AS null_ratecode_id,
    CAST(ROUND(SUM(CASE WHEN RatecodeID IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_ratecode,

    SUM(CASE WHEN store_and_fwd_flag IS NULL THEN 1 ELSE 0 END) AS null_store_fwd,
    CAST(ROUND(SUM(CASE WHEN store_and_fwd_flag IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_store_fwd,

    SUM(CASE WHEN congestion_surcharge IS NULL THEN 1 ELSE 0 END) AS null_congestion,
    CAST(ROUND(SUM(CASE WHEN congestion_surcharge IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_congestion,

    SUM(CASE WHEN airport_fee IS NULL        THEN 1 ELSE 0 END) AS null_airport_fee,
    CAST(ROUND(SUM(CASE WHEN airport_fee IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_airport_fee,

    SUM(CASE WHEN cbd_congestion_fee IS NULL THEN 1 ELSE 0 END) AS null_cbd_fee,
    CAST(ROUND(SUM(CASE WHEN cbd_congestion_fee IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_cbd_fee,

    -- Core fields that should not be NULL
    SUM(CASE WHEN VendorID IS NULL              THEN 1 ELSE 0 END) AS null_vendor_id,
    SUM(CASE WHEN tpep_pickup_datetime IS NULL  THEN 1 ELSE 0 END) AS null_pickup_dt,
    SUM(CASE WHEN tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS null_dropoff_dt,
    SUM(CASE WHEN fare_amount IS NULL           THEN 1 ELSE 0 END) AS null_fare,
    SUM(CASE WHEN total_amount IS NULL          THEN 1 ELSE 0 END) AS null_total
FROM bronze.vw_yellow_taxi_raw;

-- 4. Same NULL story, now split by year.
SELECT
    '4. NULL RATE PER YEAR' AS section,
    source_year,
    COUNT(*) AS total_rows,
    CAST(ROUND(SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_passenger,
    CAST(ROUND(SUM(CASE WHEN congestion_surcharge IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_congestion,
    CAST(ROUND(SUM(CASE WHEN airport_fee IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_airport_fee,
    CAST(ROUND(SUM(CASE WHEN cbd_congestion_fee IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_null_cbd_fee
FROM bronze.vw_yellow_taxi_raw
GROUP BY source_year
ORDER BY source_year;

-- 5. Vendor quality check.
SELECT
    '5. VENDOR ANALYSIS' AS section,
    VendorID,
    COUNT(*) AS record_count,
    CAST(ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS DECIMAL(5,2)) AS pct_of_total,

    -- A few sanity signals per vendor
    SUM(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 1 ELSE 0 END) AS reversed_timestamps,
    SUM(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END) AS zero_distance,
    SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS negative_fare,
    MIN(tpep_pickup_datetime) AS earliest_trip,
    MAX(tpep_pickup_datetime) AS latest_trip
FROM bronze.vw_yellow_taxi_raw
GROUP BY VendorID
ORDER BY VendorID;

-- 6. Distance outliers and basic distribution.
SELECT
    '6. TRIP DISTANCE' AS section,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END) AS zero_distance,
    CAST(ROUND(SUM(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_zero,
    SUM(CASE WHEN trip_distance < 0 THEN 1 ELSE 0 END) AS negative_distance,
    SUM(CASE WHEN trip_distance >= 500 THEN 1 ELSE 0 END) AS over_500_miles,
    CAST(ROUND(MIN(trip_distance), 2) AS DECIMAL(10,2)) AS min_distance,
    CAST(ROUND(AVG(trip_distance), 2) AS DECIMAL(10,2)) AS avg_distance,
    CAST(ROUND(MAX(trip_distance), 2) AS DECIMAL(10,2)) AS max_distance
FROM bronze.vw_yellow_taxi_raw;

-- 7. Duration issues: reversed timestamps, too short, too long.
SELECT
    '7. TRIP DURATION' AS section,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 1 ELSE 0 END) AS reversed_or_zero_duration,
    CAST(ROUND(SUM(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_reversed,
    SUM(CASE WHEN DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) < 1
             AND tpep_dropoff_datetime > tpep_pickup_datetime THEN 1 ELSE 0 END) AS under_1_min,
    SUM(CASE WHEN DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) > 1440
             AND tpep_dropoff_datetime > tpep_pickup_datetime THEN 1 ELSE 0 END) AS over_24_hours,
    CAST(ROUND(AVG(CASE WHEN tpep_dropoff_datetime > tpep_pickup_datetime
        THEN CAST(DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS FLOAT)
        END), 2) AS DECIMAL(10,2)) AS avg_duration_min
FROM bronze.vw_yellow_taxi_raw;

-- 8. Pickup and dropoff IDs outside the official TLC range.
SELECT
    '8. LOCATION IDs' AS section,
    SUM(CASE WHEN PULocationID < 1 OR PULocationID > 265 THEN 1 ELSE 0 END) AS invalid_pickup_loc,
    SUM(CASE WHEN DOLocationID < 1 OR DOLocationID > 265 THEN 1 ELSE 0 END) AS invalid_dropoff_loc,
    MIN(PULocationID) AS min_pu, MAX(PULocationID) AS max_pu,
    MIN(DOLocationID) AS min_do, MAX(DOLocationID) AS max_do
FROM bronze.vw_yellow_taxi_raw;

-- 9. Financial anomalies used later for correction logic.
SELECT
    '9. FINANCIAL ANOMALIES' AS section,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END) AS non_positive_fare,
    CAST(ROUND(SUM(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_non_positive_fare,
    SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) AS non_positive_total,
    CAST(ROUND(SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_non_positive_total,
    SUM(CASE WHEN total_amount > 1000 THEN 1 ELSE 0 END) AS total_over_1000,
    SUM(CASE WHEN fare_amount <= 0 OR total_amount <= 0 OR total_amount > 1000 THEN 1 ELSE 0 END) AS correction_candidates,
    CAST(ROUND(SUM(CASE WHEN fare_amount <= 0 OR total_amount <= 0 OR total_amount > 1000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS DECIMAL(5,2)) AS pct_corrections
FROM bronze.vw_yellow_taxi_raw;

-- 10. Payment type distribution.
SELECT
    '10. PAYMENT TYPE' AS section,
    CAST(CAST(payment_type AS FLOAT) AS INT) AS payment_type_id,
    CASE CAST(CAST(payment_type AS FLOAT) AS INT)
        WHEN 0 THEN 'Unknown'
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
        ELSE 'Other (' + CAST(payment_type AS VARCHAR) + ')'
    END AS payment_name,
    COUNT(*) AS record_count,
    CAST(ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS DECIMAL(5,2)) AS pct_of_total
FROM bronze.vw_yellow_taxi_raw
GROUP BY CAST(CAST(payment_type AS FLOAT) AS INT), payment_type
ORDER BY record_count DESC;

-- 11. Records outside the expected project window.
SELECT
    '11. DATE RANGE' AS section,
    SUM(CASE WHEN tpep_pickup_datetime < '2021-01-01' THEN 1 ELSE 0 END)  AS before_2021,
    SUM(CASE WHEN tpep_pickup_datetime >= '2026-01-01' THEN 1 ELSE 0 END) AS after_2025,
    SUM(CASE WHEN tpep_pickup_datetime >= '2021-01-01'
             AND tpep_pickup_datetime < '2026-01-01' THEN 1 ELSE 0 END)   AS in_range,
    CAST(ROUND(
        SUM(CASE WHEN tpep_pickup_datetime < '2021-01-01'
                  OR tpep_pickup_datetime >= '2026-01-01' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        4
    ) AS DECIMAL(7,4)) AS pct_out_of_range
FROM bronze.vw_yellow_taxi_raw;

-- 12. Compare two approaches:
--     drop rows with NULLs vs keep fixable rows and filter only invalid ones.
SELECT
    '12. DROP vs COALESCE' AS section,
    COUNT(*) AS total_rows,

    -- What we lose if we simply drop rows with NULLs
    SUM(CASE
        WHEN passenger_count IS NULL
          OR RatecodeID IS NULL
          OR store_and_fwd_flag IS NULL
          OR congestion_surcharge IS NULL
          OR airport_fee IS NULL
        THEN 1 ELSE 0
    END) AS rows_lost_if_drop_nulls,

    CAST(ROUND(
        SUM(CASE
            WHEN passenger_count IS NULL
              OR RatecodeID IS NULL
              OR store_and_fwd_flag IS NULL
              OR congestion_surcharge IS NULL
              OR airport_fee IS NULL
            THEN 1 ELSE 0
        END) * 100.0 / COUNT(*), 2
    ) AS DECIMAL(5,2)) AS pct_lost_if_drop,

    -- What we lose if we only filter clearly invalid records
    SUM(CASE
        WHEN VendorID NOT IN (1, 2)
          OR trip_distance <= 0
          OR trip_distance >= 500
          OR tpep_dropoff_datetime <= tpep_pickup_datetime
          OR DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) < 1
          OR DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) > 1440
          OR PULocationID < 1 OR PULocationID > 265
          OR DOLocationID < 1 OR DOLocationID > 265
          OR tpep_pickup_datetime < '2021-01-01'
          OR tpep_pickup_datetime >= '2026-01-01'
        THEN 1 ELSE 0
    END) AS rows_lost_if_filter_only,

    CAST(ROUND(
        SUM(CASE
            WHEN VendorID NOT IN (1, 2)
              OR trip_distance <= 0
              OR trip_distance >= 500
              OR tpep_dropoff_datetime <= tpep_pickup_datetime
              OR DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) < 1
              OR DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) > 1440
              OR PULocationID < 1 OR PULocationID > 265
              OR DOLocationID < 1 OR DOLocationID > 265
              OR tpep_pickup_datetime < '2021-01-01'
              OR tpep_pickup_datetime >= '2026-01-01'
            THEN 1 ELSE 0
        END) * 100.0 / COUNT(*), 2
    ) AS DECIMAL(5,2)) AS pct_lost_if_filter

FROM bronze.vw_yellow_taxi_raw;

-- 13. Per-filter impact summary.
SELECT
    '13. FILTER IMPACT' AS section,
    filter_name,
    affected_rows,
    CAST(ROUND(affected_rows * 100.0 / total_rows, 2) AS DECIMAL(5,2)) AS pct_affected
FROM (
    SELECT
        (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw) AS total_rows,
        filter_name,
        affected_rows
    FROM (
        VALUES
            ('VendorID NOT IN (1,2)',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE VendorID NOT IN (1, 2))),
            ('trip_distance <= 0',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE trip_distance <= 0)),
            ('trip_distance >= 500',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE trip_distance >= 500)),
            ('dropoff <= pickup (reversed)',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE tpep_dropoff_datetime <= tpep_pickup_datetime)),
            ('duration < 1 min',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE tpep_dropoff_datetime > tpep_pickup_datetime AND DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) < 1)),
            ('duration > 1440 min (24h)',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE tpep_dropoff_datetime > tpep_pickup_datetime AND DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) > 1440)),
            ('PULocationID out of 1-265',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE PULocationID < 1 OR PULocationID > 265)),
            ('DOLocationID out of 1-265',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE DOLocationID < 1 OR DOLocationID > 265)),
            ('pickup before 2021',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE tpep_pickup_datetime < '2021-01-01')),
            ('pickup after 2025',
                (SELECT COUNT(*) FROM bronze.vw_yellow_taxi_raw WHERE tpep_pickup_datetime >= '2026-01-01'))
    ) AS filters(filter_name, affected_rows)
) t
ORDER BY pct_affected DESC;

END;
GO
