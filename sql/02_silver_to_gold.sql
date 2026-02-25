-- Daily Revenue Summary

CREATE EXTERNAL TABLE gold.daily_revenue_summary
WITH (
    LOCATION     = 'daily_revenue_summary/',
    DATA_SOURCE  = datalake_gold,
    FILE_FORMAT  = parquet_format
)
AS
SELECT
    CAST(pickup_datetime AS DATE)   AS trip_date,
    trip_year,
    trip_month,
    trip_day,
    trip_weekday,


    COUNT(*)                        AS total_trips,
    SUM(passenger_count)            AS total_passengers,

    SUM(total_amount)               AS total_revenue,
    SUM(fare_amount)                AS total_fare_revenue,
    SUM(tip_amount)                 AS total_tips,
    SUM(tolls_amount)               AS total_tolls,
    SUM(congestion_surcharge)       AS total_congestion_surcharge,
    AVG(total_amount)               AS avg_trip_cost,
    AVG(fare_amount)                AS avg_fare,
    AVG(tip_amount)                 AS avg_tip,

    SUM(trip_distance_miles)        AS total_distance_miles,
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
    CAST(pickup_datetime AS DATE),
    trip_year,
    trip_month,
    trip_day,
    trip_weekday
;
GO

-- Popular Pickup Zones

CREATE EXTERNAL TABLE gold.popular_zones
WITH (
    LOCATION     = 'popular_zones/',
    DATA_SOURCE  = datalake_gold,
    FILE_FORMAT  = parquet_format
)
AS
SELECT
    pickup_location_id,
    dropoff_location_id,
    trip_year,
    trip_month,

    COUNT(*)                    AS trip_count,
    SUM(total_amount)           AS total_revenue,
    AVG(total_amount)           AS avg_revenue,
    AVG(trip_distance_miles)    AS avg_distance,
    AVG(trip_duration_minutes)  AS avg_duration_minutes,
    AVG(tip_amount)             AS avg_tip

FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'valid'
GROUP BY
    pickup_location_id,
    dropoff_location_id,
    trip_year,
    trip_month
;
GO

-- Hourly Patterns

CREATE EXTERNAL TABLE gold.hourly_patterns
WITH (
    LOCATION     = 'hourly_patterns/',
    DATA_SOURCE  = datalake_gold,
    FILE_FORMAT  = parquet_format
)
AS
SELECT
    trip_year,
    trip_month,
    trip_weekday,
    pickup_hour,

    COUNT(*)                    AS trip_count,
    AVG(total_amount)           AS avg_total_amount,
    AVG(trip_distance_miles)    AS avg_distance,
    AVG(trip_duration_minutes)  AS avg_duration_minutes,
    AVG(tip_amount)             AS avg_tip

FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'valid'
GROUP BY
    trip_year,
    trip_month,
    trip_weekday,
    pickup_hour
;
GO

-- Payment Type Breakdown

CREATE OR ALTER VIEW gold.vw_payment_breakdown
AS
SELECT
    trip_year,
    trip_month,
    CASE payment_type
        WHEN 0 THEN 'Unknown'
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
        ELSE 'Other'
    END                         AS payment_method,

    COUNT(*)                    AS trip_count,
    SUM(total_amount)           AS total_revenue,
    AVG(total_amount)           AS avg_revenue,
    AVG(tip_amount)             AS avg_tip,

    CASE
        WHEN SUM(fare_amount) > 0
        THEN SUM(tip_amount) / SUM(fare_amount) * 100
        ELSE 0
    END                         AS tip_percentage

FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'valid'
GROUP BY
    trip_year,
    trip_month,
    payment_type
;
GO

-- Corrections Summary

CREATE EXTERNAL TABLE gold.corrections_summary
WITH (
    LOCATION     = 'corrections_summary/',
    DATA_SOURCE  = datalake_gold,
    FILE_FORMAT  = parquet_format
)
AS
SELECT
    trip_year,
    trip_month,
    CASE payment_type
        WHEN 0 THEN 'Unknown'
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Other'
    END                         AS payment_method,
    pickup_location_id,

    COUNT(*)                    AS correction_count,
    SUM(fare_amount)            AS total_refunded_fare,
    SUM(total_amount)           AS total_refunded_amount,
    AVG(fare_amount)            AS avg_refunded_fare

FROM silver.yellow_taxi_cleaned
WHERE trip_status = 'correction'
GROUP BY
    trip_year,
    trip_month,
    payment_type,
    pickup_location_id
;
GO

