USE nyc_taxi_dwh;
GO

-- Raw Bronze view over all ingested parquet files.
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

-- Monthly Silver load.
-- The procedure writes one requested month to its own partition in the Silver layer.
CREATE OR ALTER PROCEDURE silver.sp_load_yellow_taxi_incremental
    @TargetYear VARCHAR(4),
    @TargetMonth VARCHAR(2)
AS
BEGIN
    DECLARE @DynamicSQL NVARCHAR(MAX);
    DECLARE @LocationPath VARCHAR(200) = 'yellow_taxi_cleaned/year=' + @TargetYear + '/month=' + @TargetMonth + '/';

    -- Temporary external table used only to materialize parquet output.
    IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'temp_incremental_write' AND schema_id = SCHEMA_ID('silver'))
        DROP EXTERNAL TABLE silver.temp_incremental_write;

    SET @DynamicSQL = N'
    CREATE EXTERNAL TABLE silver.temp_incremental_write
    WITH (
        LOCATION     = ''' + @LocationPath + ''',
        DATA_SOURCE  = datalake_silver,
        FILE_FORMAT  = parquet_format
    )
    AS
    SELECT
        VendorID                                          AS vendor_id,
        PULocationID                                      AS pickup_location_id,
        DOLocationID                                      AS dropoff_location_id,
        tpep_pickup_datetime                              AS pickup_datetime,
        tpep_dropoff_datetime                             AS dropoff_datetime,
        COALESCE(CAST(CAST(passenger_count AS FLOAT) AS INT), 1)    AS passenger_count,
        CAST(trip_distance AS DECIMAL(10,2))                        AS trip_distance_miles,
        COALESCE(CAST(CAST(RatecodeID AS FLOAT) AS INT), 1)         AS rate_code_id,
        COALESCE(store_and_fwd_flag, ''N'')                         AS store_and_fwd_flag,
        CAST(CAST(payment_type AS FLOAT) AS INT)                    AS payment_type,
        CAST(fare_amount AS DECIMAL(10,2))                          AS fare_amount,
        CAST(extra AS DECIMAL(10,2))                                AS extra_amount,
        CAST(mta_tax AS DECIMAL(10,2))                              AS mta_tax,
        CAST(tip_amount AS DECIMAL(10,2))                           AS tip_amount,
        CAST(tolls_amount AS DECIMAL(10,2))                         AS tolls_amount,
        CAST(improvement_surcharge AS DECIMAL(10,2))                AS improvement_surcharge,
        CAST(total_amount AS DECIMAL(10,2))                         AS total_amount,
        COALESCE(CAST(congestion_surcharge AS DECIMAL(10,2)), 0.00) AS congestion_surcharge,
        COALESCE(CAST(airport_fee AS DECIMAL(10,2)), 0.00)          AS airport_fee,
        COALESCE(CAST(cbd_congestion_fee AS DECIMAL(10,2)), 0.00)   AS cbd_congestion_fee,
        DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
        YEAR(tpep_pickup_datetime)                                  AS trip_year,
        MONTH(tpep_pickup_datetime)                                 AS trip_month,
        DAY(tpep_pickup_datetime)                                   AS trip_day,
        DATENAME(WEEKDAY, tpep_pickup_datetime)                     AS trip_weekday,
        DATEPART(HOUR, tpep_pickup_datetime)                        AS pickup_hour,
        CASE
            WHEN fare_amount <= 0 OR total_amount <= 0 OR total_amount > 1000 THEN ''correction''
            ELSE ''valid''
        END                                                         AS trip_status

    FROM bronze.vw_yellow_taxi_raw

    WHERE source_file LIKE ''%'' + @TargetYear + ''-'' + @TargetMonth + ''%''
        AND VendorID IN (1, 2)
        AND trip_distance > 0
        AND trip_distance < 500
        AND tpep_dropoff_datetime > tpep_pickup_datetime
        AND DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 1 AND 1440
        AND PULocationID BETWEEN 1 AND 265
        AND DOLocationID BETWEEN 1 AND 265
        AND tpep_pickup_datetime >= ''2021-01-01''
        AND tpep_pickup_datetime <  ''2026-01-01''
    ';

    -- Execute the month-specific load.
    EXEC sp_executesql @DynamicSQL;

    -- Drop the temporary metadata object. Written parquet files stay in storage.
    DROP EXTERNAL TABLE silver.temp_incremental_write;
END;
GO

-- Logical Silver view over all generated monthly partitions.
-- Clean up the old full-refresh object if it still exists.
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'yellow_taxi_cleaned' AND schema_id = SCHEMA_ID('silver'))
    DROP EXTERNAL TABLE silver.yellow_taxi_cleaned;
GO

CREATE OR ALTER VIEW silver.yellow_taxi_cleaned
AS
SELECT *
FROM
    OPENROWSET(
        BULK 'yellow_taxi_cleaned/year=*/month=*/*.parquet',
        DATA_SOURCE = 'datalake_silver',
        FORMAT = 'PARQUET'
    ) AS result;
GO
