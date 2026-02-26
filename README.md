# Azure NYC Taxi — Data Warehouse

Hurtownia danych dla NYC Yellow Taxi zbudowana na platformie Azure w architekturze Medallion (Bronze → Silver → Gold).

> **Źródło danych:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
> **Zakres:** Yellow Taxi, styczeń 2021 – listopad 2025 (~200M rekordów)

---

## Spis treści

1. [Architektura](#architektura)
2. [Infrastruktura (Terraform)](#infrastruktura-terraform)
3. [Ingestion — Bronze Layer](#ingestion--bronze-layer)
4. [Transformacja — Bronze → Silver](#transformacja--bronze--silver)
5. [Transformacja — Silver → Gold](#transformacja--silver--gold)
6. [Testy jakości danych](#testy-jakości-danych)
7. [Uruchomienie projektu](#uruchomienie-projektu)
8. [Dashboardy Power BI](#dashboardy-power-bi)


---

## Architektura

![Architektura](photos/t.png)

> **Storage:** Wszystkie warstwy (Bronze/Silver/Gold) → Azure Data Lake Storage Gen2

| Warstwa | Opis | Format | Lokalizacja |
|---------|------|--------|-------------|
| **Bronze** | Surowe dane bez zmian | Parquet (Snappy) | `bronze/yellow_tripdata/` |
| **Silver** | Wyczyszczone, ustandaryzowane | Parquet (Snappy) | `silver/yellow_taxi_cleaned/` |
| **Gold** | Zagregowane KPI i metryki | Parquet + Views | `gold/*/` |

### Użyte technologie

| Komponent | Technologia |
|-----------|-------------|
| IaC | Terraform |
| Ingestion | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Synapse Analytics |
| Wizualizacja | Power BI (DirectQuery) |
| Autoryzacja | Managed Identity|

![Azure Resource Group — wszystkie zasoby projektu](photos/1.png)

---

## Infrastruktura (Terraform)

Cała infrastruktura zdefiniowana jako kod (IaC) w plikach `.tf`:

| Plik | Opis |
|------|------|
| `main.tf` | Provider, Resource Group |
| `storage.tf` | Storage Account, ADLS Gen2 filesystems (bronze, silver, gold) |
| `data_factory.tf` | Azure Data Factory |
| `pipeline.tf` | ADF Linked Services, Datasets, Pipelines (ingestion) |
| `synapse.tf` | Synapse Workspace (Serverless SQL Pool) |
| `security.tf` | Role assignments, Managed Identity |
| `variables.tf` | Zmienne|
| `outputs.tf` | Outputy (nazwy zasobów, URLs) |

### Deployment

```bash
terraform init
terraform plan
terraform apply
```

---

## Ingestion — Bronze Layer

Azure Data Factory pobiera pliki Parquet z NYC TLC API i zapisuje je w ADLS Gen2 (Bronze).

### Pipeline

```
pl_ingest_year (ForEach month 01-12)
  └── pl_ingest_single_month (Copy Activity)
        Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet
        Sink:   bronze/yellow_tripdata/{year}/yellow_tripdata_{year}-{month}.parquet
```

| Parametr | Wartość |
|----------|---------|
| Równoległość | 4 miesiące jednocześnie |
| Retry | 2 próby, 30s przerwa |
| Timeout | 1h na plik |
| Kompresja | Snappy |

> ** Błedy w pipline wynikaja z tego ze za grudzien 2026 nie ma jszcze dostepnych plików a pipline próbował je pobrać** 

![Azure Data Factory → Pipeline "pl_ingest_year" → widok edytora z ForEach](photos/adf_1.png)
![Azure Data Factory → Monitor → zakończone pipeline runy](photos/adf_2.png)
![Azure Portal → Storage Account → Containers → bronze → yellow_tripdata → lista folderów z latami](photos/adf_3.png)



