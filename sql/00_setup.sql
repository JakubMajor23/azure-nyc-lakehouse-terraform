CREATE DATABASE nyc_taxi_dwh;
GO

USE nyc_taxi_dwh;
GO

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<your_master_key_password>';
GO

CREATE DATABASE SCOPED CREDENTIAL synapse_identity
WITH IDENTITY = 'Managed Identity';
GO

CREATE EXTERNAL DATA SOURCE datalake_bronze
WITH (
    LOCATION   = 'abfss://bronze@<storage_account_name>.dfs.core.windows.net',
    CREDENTIAL = synapse_identity
);
GO

CREATE EXTERNAL DATA SOURCE datalake_silver
WITH (
    LOCATION   = 'abfss://silver@<storage_account_name>.dfs.core.windows.net',
    CREDENTIAL = synapse_identity
);
GO

CREATE EXTERNAL DATA SOURCE datalake_gold
WITH (
    LOCATION   = 'abfss://gold@<storage_account_name>.dfs.core.windows.net',
    CREDENTIAL = synapse_identity
);
GO

CREATE EXTERNAL FILE FORMAT parquet_format
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

