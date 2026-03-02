CREATE EXTERNAL TABLE gold.dim_date
WITH (
    LOCATION     = 'dim_date/',
    DATA_SOURCE  = datalake_gold,
    FILE_FORMAT  = parquet_format
)
AS
WITH E00(N) AS (SELECT 1 UNION ALL SELECT 1),
     E02(N) AS (SELECT 1 FROM E00 a, E00 b),
     E04(N) AS (SELECT 1 FROM E02 a, E02 b),
     E08(N) AS (SELECT 1 FROM E04 a, E04 b),
     E16(N) AS (SELECT 1 FROM E08 a, E08 b),
     Tally(N) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM E16),
     DateRange AS (
         SELECT CAST(DATEADD(day, N - 1, '2021-01-01') AS DATE) AS full_date
         FROM Tally
         WHERE N <= DATEDIFF(day, '2021-01-01', '2025-12-31') + 1
     )
SELECT 
    CAST(FORMAT(full_date, 'yyyyMMdd') AS INT)                       AS date_key,
    full_date,
    YEAR(full_date)                                                  AS year,
    MONTH(full_date)                                                 AS month,
    DAY(full_date)                                                   AS day,
    DATENAME(WEEKDAY, full_date)                                     AS day_name,
    DATENAME(MONTH, full_date)                                       AS month_name,
    CONCAT(
        CAST(YEAR(full_date) AS VARCHAR),
        '-',
        RIGHT('0' + CAST(MONTH(full_date) AS VARCHAR), 2)
    )                                                                AS year_month,
    DATEPART(WEEKDAY, full_date)                                     AS weekday_num,
    DATEPART(QUARTER, full_date)                                     AS quarter
FROM DateRange
;
GO


-- ===================
-- 2. dim_payment_type
-- ===================
-- Static dimension with payment type labels and categories.
-- payment_category groups into 3: Credit Card, Cash, Others.

CREATE EXTERNAL TABLE gold.dim_payment_type
WITH (
    LOCATION     = 'dim_payment_type/',
    DATA_SOURCE  = datalake_gold,
    FILE_FORMAT  = parquet_format
)
AS
SELECT 0 AS payment_key, 'Unknown'      AS payment_name, 'Others'      AS payment_category
UNION ALL
SELECT 1, 'Credit Card',  'Credit Card'
UNION ALL
SELECT 2, 'Cash',         'Cash'
UNION ALL
SELECT 3, 'No Charge',    'Others'
UNION ALL
SELECT 4, 'Dispute',      'Others'
UNION ALL
SELECT 5, 'Unknown',      'Others'
UNION ALL
SELECT 6, 'Voided Trip',  'Others'
;
GO


-- ===================
-- 3. fact_trips
-- ===================
-- Fact table for valid trips, aggregated by:
--   date + hour + payment + pickup_location + dropoff_location
-- Contains all revenue components, distance, duration metrics.

CREATE EXTERNAL TABLE gold.fact_trips
WITH (
    LOCATION     = 'fact_trips/',
    DATA_SOURCE  = datalake_gold,
    FILE_FORMAT  = parquet_format
)
AS
SELECT
    CAST(FORMAT(CAST(pickup_datetime AS DATE), 'yyyyMMdd') AS INT)   AS date_key,
    payment_type                                                      AS payment_key,
    pickup_location_id,
    dropoff_location_id,
    pickup_hour,

    COUNT(*)                        AS trip_count,
    SUM(passenger_count)            AS total_passengers,

    SUM(total_amount)               AS total_revenue,
    SUM(fare_amount)                AS total_fare,
    SUM(tip_amount)                 AS total_tips,
    SUM(tolls_amount)               AS total_tolls,
    SUM(congestion_surcharge)       AS total_congestion,
    SUM(extra_amount)               AS total_extra,
    SUM(mta_tax)                    AS total_mta_tax,
    SUM(improvement_surcharge)      AS total_improvement,
    SUM(airport_fee)                AS total_airport_fee,

    SUM(trip_distance_miles)        AS total_distance_miles,
    SUM(trip_duration_minutes)      AS total_duration_minutes,

    AVG(total_amount)               AS avg_trip_cost,
    AVG(fare_amount)                AS avg_fare,
    AVG(tip_amount)                 AS avg_tip,
    AVG(trip_distance_miles)        AS avg_distance_miles,
    AVG(trip_duration_minutes)      AS avg_duration_minutes,

    CASE
        WHEN SUM(trip_distance_miles) > 0
        THEN SUM(total_amount) / SUM(trip_distance_miles)
        ELSE 0
    END                             AS revenue_per_mile

FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'valid'
GROUP BY
    CAST(FORMAT(CAST(pickup_datetime AS DATE), 'yyyyMMdd') AS INT),
    payment_type,
    pickup_location_id,
    dropoff_location_id,
    pickup_hour
;
GO


-- ===================
-- 4. fact_corrections
-- ===================
-- Fact table for corrections/refunds (trip_status = 'correction').
-- Aggregated by: date + payment + pickup_location.

CREATE EXTERNAL TABLE gold.fact_corrections
WITH (
    LOCATION     = 'fact_corrections/',
    DATA_SOURCE  = datalake_gold,
    FILE_FORMAT  = parquet_format
)
AS
SELECT
    CAST(FORMAT(CAST(pickup_datetime AS DATE), 'yyyyMMdd') AS INT)   AS date_key,
    payment_type                                                      AS payment_key,
    pickup_location_id,

    COUNT(*)                        AS correction_count,
    SUM(fare_amount)                AS total_refunded_fare,
    SUM(total_amount)               AS total_refunded_amount,
    AVG(fare_amount)                AS avg_refunded_fare

FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'correction'
GROUP BY
    CAST(FORMAT(CAST(pickup_datetime AS DATE), 'yyyyMMdd') AS INT),
    payment_type,
    pickup_location_id
;
GO
