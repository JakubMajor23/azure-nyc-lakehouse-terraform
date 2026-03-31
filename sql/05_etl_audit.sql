-- ETL audit snapshot.
-- Run this after the Silver and Gold steps to persist one health snapshot in Gold.

USE nyc_taxi_dwh;
GO

CREATE OR ALTER PROCEDURE gold.sp_run_etl_audit
AS
BEGIN
    -- Recreate the snapshot on every run.
    IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'etl_audit' AND schema_id = SCHEMA_ID('gold'))
        DROP EXTERNAL TABLE gold.etl_audit;

    CREATE EXTERNAL TABLE gold.etl_audit
    WITH (
        LOCATION     = 'etl_audit/',
        DATA_SOURCE  = datalake_gold,
        FILE_FORMAT  = parquet_format
    )
    AS
    SELECT
        -- Run metadata
        CAST(GETUTCDATE() AS DATETIME2) AS audit_timestamp,

        -- Bronze metrics
        b.bronze_row_count,
        b.bronze_earliest_pickup,
        b.bronze_latest_pickup,
        b.bronze_file_count,

        -- Silver metrics
        s.silver_row_count,
        s.silver_valid_count,
        s.silver_correction_count,

        -- Data loss
        CAST(ROUND(
            (1.0 - CAST(s.silver_row_count AS FLOAT) / NULLIF(b.bronze_row_count, 0)) * 100, 2
        ) AS DECIMAL(5,2)) AS data_loss_pct,

        -- Gold metrics
        g.gold_trip_rows,
        g.gold_trip_count_sum,
        g.gold_correction_rows,
        g.gold_correction_count_sum,
        g.gold_dim_date_rows,
        g.gold_dim_payment_rows,
        g.gold_dim_location_rows,

        -- Revenue reconciliation
        g.gold_total_revenue,
        s.silver_valid_revenue,
        CAST(ABS(g.gold_total_revenue - s.silver_valid_revenue) AS DECIMAL(15,2)) AS revenue_diff,
        CASE
            WHEN ABS(g.gold_total_revenue - s.silver_valid_revenue) < 1 THEN 'MATCH'
            ELSE 'MISMATCH'
        END AS revenue_reconciliation,

        -- Row-count reconciliation
        CASE
            WHEN g.gold_trip_count_sum = s.silver_valid_count THEN 'MATCH'
            ELSE 'MISMATCH'
        END AS trip_count_reconciliation,

        CASE
            WHEN g.gold_correction_count_sum = s.silver_correction_count THEN 'MATCH'
            ELSE 'MISMATCH'
        END AS correction_count_reconciliation,

        -- Freshness
        DATEDIFF(DAY, b.bronze_latest_pickup, GETUTCDATE()) AS days_since_latest_record,

        -- Final health flag
        CASE
            WHEN CAST(ROUND(
                    (1.0 - CAST(s.silver_row_count AS FLOAT) / NULLIF(b.bronze_row_count, 0)) * 100, 2
                 ) AS DECIMAL(5,2)) <= 10
             AND ABS(g.gold_total_revenue - s.silver_valid_revenue) < 1
             AND g.gold_trip_count_sum = s.silver_valid_count
             AND g.gold_correction_count_sum = s.silver_correction_count
             AND g.gold_dim_date_rows > 0
             AND g.gold_dim_payment_rows = 7
             AND g.gold_dim_location_rows = 265
            THEN 'HEALTHY'
            ELSE 'NEEDS ATTENTION'
        END AS pipeline_status

    FROM
        -- Bronze aggregate
        (SELECT
            COUNT(*)                    AS bronze_row_count,
            MIN(tpep_pickup_datetime)   AS bronze_earliest_pickup,
            MAX(tpep_pickup_datetime)   AS bronze_latest_pickup,
            COUNT(DISTINCT source_file) AS bronze_file_count
         FROM bronze.vw_yellow_taxi_raw
        ) b,

        -- Silver aggregate
        (SELECT
            COUNT(*) AS silver_row_count,
            SUM(CASE WHEN trip_status = 'valid' THEN 1 ELSE 0 END) AS silver_valid_count,
            SUM(CASE WHEN trip_status = 'correction' THEN 1 ELSE 0 END) AS silver_correction_count,
            ISNULL(SUM(CASE WHEN trip_status = 'valid' THEN total_amount ELSE 0 END), 0) AS silver_valid_revenue
         FROM silver.yellow_taxi_cleaned
        ) s,

        -- Gold aggregate
        (SELECT
            (SELECT COUNT(*) FROM gold.fact_trips) AS gold_trip_rows,
            (SELECT ISNULL(SUM(CAST(trip_count AS BIGINT)), 0) FROM gold.fact_trips) AS gold_trip_count_sum,
            (SELECT COUNT(*) FROM gold.fact_corrections) AS gold_correction_rows,
            (SELECT ISNULL(SUM(CAST(correction_count AS BIGINT)), 0) FROM gold.fact_corrections) AS gold_correction_count_sum,
            (SELECT COUNT(*) FROM gold.dim_date) AS gold_dim_date_rows,
            (SELECT COUNT(*) FROM gold.dim_payment_type) AS gold_dim_payment_rows,
            (SELECT COUNT(*) FROM gold.dim_location) AS gold_dim_location_rows,
            (SELECT ISNULL(SUM(total_revenue), 0) FROM gold.fact_trips) AS gold_total_revenue
        ) g;
END;
GO
