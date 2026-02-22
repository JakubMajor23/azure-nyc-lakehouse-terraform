resource "azurerm_data_factory_linked_custom_service" "nyc_tlc_http" {
  name            = "ls_http_nyc_tlc"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "HttpServer"
  type_properties_json = jsonencode({
    url                               = "https://d37ci6vzurychx.cloudfront.net"
    enableServerCertificateValidation = true
    authenticationType                = "Anonymous"
  })
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "datalake" {
  name                 = "ls_adls_datalake"
  data_factory_id      = azurerm_data_factory.adf.id
  url                  = "https://${azurerm_storage_account.datalake.name}.dfs.core.windows.net"
  use_managed_identity = true
}

resource "azurerm_data_factory_custom_dataset" "source_nyc_parquet" {
  name            = "ds_source_nyc_parquet"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "Parquet"

  linked_service {
    name = azurerm_data_factory_linked_custom_service.nyc_tlc_http.name
  }

  type_properties_json = <<JSON
{
  "location": {
    "type": "HttpServerLocation",
    "relativeUrl": {
      "value": "@concat('/trip-data/yellow_tripdata_', dataset().Year, '-', dataset().Month, '.parquet')",
      "type": "Expression"
    },
    "fileName": {
      "value": "@concat('yellow_tripdata_', dataset().Year, '-', dataset().Month, '.parquet')",
      "type": "Expression"
    }
  }
}
JSON

  parameters = {
    Year  = ""
    Month = ""
  }

  schema_json = "[]"
}

resource "azurerm_data_factory_custom_dataset" "sink_bronze_parquet" {
  name            = "ds_sink_bronze_parquet"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "Parquet"

  linked_service {
    name = azurerm_data_factory_linked_service_data_lake_storage_gen2.datalake.name
  }

  type_properties_json = <<JSON
{
  "location": {
    "type": "AzureBlobFSLocation",
    "fileSystem": "${azurerm_storage_data_lake_gen2_filesystem.bronze.name}",
    "folderPath": {
      "value": "@concat('yellow_tripdata/', dataset().Year, '/')",
      "type": "Expression"
    },
    "fileName": {
      "value": "@concat('yellow_tripdata_', dataset().Year, '-', dataset().Month, '.parquet')",
      "type": "Expression"
    }
  },
  "compressionCodec": "snappy"
}
JSON

  parameters = {
    Year  = ""
    Month = ""
  }

  schema_json = "[]"
}

resource "azurerm_data_factory_pipeline" "ingest_single_month" {
  name            = "pl_ingest_single_month"
  data_factory_id = azurerm_data_factory.adf.id

  depends_on = [
    azurerm_data_factory_custom_dataset.source_nyc_parquet,
    azurerm_data_factory_custom_dataset.sink_bronze_parquet
  ]

  parameters = {
    Year  = ""
    Month = ""
  }

  activities_json = <<JSON
[
  {
    "name": "Copy_NYC_Taxi_Data",
    "type": "Copy",
    "dependsOn": [],
    "policy": {
      "timeout": "0.01:00:00",
      "retry": 2,
      "retryIntervalInSeconds": 30
    },
    "typeProperties": {
      "source": {
        "type": "ParquetSource",
        "storeSettings": {
          "type": "HttpReadSettings",
          "requestMethod": "GET"
        }
      },
      "sink": {
        "type": "ParquetSink",
        "storeSettings": {
          "type": "AzureBlobFSWriteSettings"
        },
        "formatSettings": {
          "type": "ParquetWriteSettings"
        }
      },
      "enableStaging": false
    },
    "inputs": [
      {
        "referenceName": "ds_source_nyc_parquet",
        "type": "DatasetReference",
        "parameters": {
          "Year": "@pipeline().parameters.Year",
          "Month": "@pipeline().parameters.Month"
        }
      }
    ],
    "outputs": [
      {
        "referenceName": "ds_sink_bronze_parquet",
        "type": "DatasetReference",
        "parameters": {
          "Year": "@pipeline().parameters.Year",
          "Month": "@pipeline().parameters.Month"
        }
      }
    ]
  }
]
JSON
}

resource "azurerm_data_factory_pipeline" "ingest_year" {
  name            = "pl_ingest_year"
  data_factory_id = azurerm_data_factory.adf.id

  depends_on = [
    azurerm_data_factory_pipeline.ingest_single_month
  ]

  parameters = {
    Year = ""
  }

  activities_json = <<JSON
[
  {
    "name": "ForEach_Month",
    "type": "ForEach",
    "dependsOn": [],
    "typeProperties": {
      "isSequential": false,
      "batchCount": 4,
      "items": {
        "value": "@createArray('01','02','03','04','05','06','07','08','09','10','11','12')",
        "type": "Expression"
      },
      "activities": [
        {
          "name": "Execute_Ingest_Month",
          "type": "ExecutePipeline",
          "dependsOn": [],
          "typeProperties": {
            "pipeline": {
              "referenceName": "pl_ingest_single_month",
              "type": "PipelineReference"
            },
            "waitOnCompletion": true,
            "parameters": {
              "Year": "@pipeline().parameters.Year",
              "Month": "@item()"
            }
          }
        }
      ]
    }
  }
]
JSON
}

resource "azurerm_data_factory_pipeline" "ingest_all_data" {
  name            = "pl_ingest_all_data"
  data_factory_id = azurerm_data_factory.adf.id

  depends_on = [
    azurerm_data_factory_pipeline.ingest_year
  ]

  parameters = {
    StartYear = "2021"
    EndYear   = "2025"
  }

  activities_json = <<JSON
[
  {
    "name": "ForEach_Year",
    "type": "ForEach",
    "dependsOn": [],
    "typeProperties": {
      "isSequential": true,
      "items": {
        "value": "@range(int(pipeline().parameters.StartYear), add(sub(int(pipeline().parameters.EndYear), int(pipeline().parameters.StartYear)), 1))",
        "type": "Expression"
      },
      "activities": [
        {
          "name": "Execute_Ingest_Year",
          "type": "ExecutePipeline",
          "dependsOn": [],
          "typeProperties": {
            "pipeline": {
              "referenceName": "pl_ingest_year",
              "type": "PipelineReference"
            },
            "waitOnCompletion": true,
            "parameters": {
              "Year": "@string(item())"
            }
          }
        }
      ]
    }
  }
]
JSON
}
