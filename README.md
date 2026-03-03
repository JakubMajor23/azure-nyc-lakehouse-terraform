[![en](https://img.shields.io/badge/lang-English-blue.svg)](README.md)
[![pl](https://img.shields.io/badge/lang-Polski-red.svg)](README_PL.md)

# Azure NYC Taxi — Data Lakehouse

A data warehouse for NYC Yellow Taxi built on Azure using the Medallion architecture (Bronze → Silver → Gold).

> **Data Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
> **Scope:** Yellow Taxi, January 2021 – November 2025 (~200M records)

---

## Table of Contents

1. [Architecture](#architecture)
2. [Infrastructure (Terraform)](#infrastructure-terraform)
3. [Ingestion — Bronze Layer](#ingestion--bronze-layer)
4. [Transformation — Bronze → Silver](#transformation--bronze--silver)
5. [Transformation — Silver → Gold](#transformation--silver--gold)
6. [Data Quality Tests](#data-quality-tests)
7. [Getting Started](#getting-started)
8. [Power BI Dashboards](#power-bi-dashboards)


---

## Architecture

![Architecture](photos/t.png)

> **Storage:** All layers (Bronze/Silver/Gold) → Azure Data Lake Storage Gen2

| Layer | Description | Format | Location |
|-------|-------------|--------|----------|
| **Bronze** | Raw data, no modifications | Parquet (Snappy) | `bronze/yellow_tripdata/` |
| **Silver** | Cleaned & standardized | Parquet (Snappy) | `silver/yellow_taxi_cleaned/` |
| **Gold** | Star Schema (KPIs) | Parquet | `gold/*/` |

### Tech Stack

| Component | Technology |
|-----------|------------|
| IaC | Terraform |
| Ingestion | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Synapse Analytics |
| Visualization | Power BI (DirectQuery) |
| Authorization | Managed Identity |

![Azure Resource Group — all project resources](photos/1.png)

---

## Infrastructure (Terraform)

All infrastructure is defined as code (IaC) in `.tf` files:

| File | Description |
|------|-------------|
| `main.tf` | Provider, Resource Group |
| `storage.tf` | Storage Account, ADLS Gen2 filesystems (bronze, silver, gold) |
| `data_factory.tf` | Azure Data Factory |
| `pipeline.tf` | ADF Linked Services, Datasets, Pipelines (ingestion) |
| `synapse.tf` | Synapse Workspace (Serverless SQL Pool) |
| `security.tf` | Role assignments, Managed Identity |
| `variables.tf` | Variables |
| `outputs.tf` | Outputs (resource names, URLs) |

---

## Ingestion — Bronze Layer

Azure Data Factory downloads Parquet files from the NYC TLC API and stores them in ADLS Gen2 (Bronze).

### Pipeline

```
pl_ingest_year (ForEach month 01-12)
  └── pl_ingest_single_month (Copy Activity)
        Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet
        Sink:   bronze/yellow_tripdata/{year}/yellow_tripdata_{year}-{month}.parquet
```

| Parameter | Value |
|-----------|-------|
| Parallelism | 4 months concurrently |
| Retry | 2 attempts, 30s interval |
| Timeout | 1h per file |
| Compression | Snappy |

> **Pipeline errors are caused by December 2025 files not yet being available at the time the pipeline attempted to download them.**

![Azure Data Factory → Pipeline "pl_ingest_year" → editor view with ForEach](photos/adf_1.png)
![Azure Data Factory → Monitor → completed pipeline runs](photos/adf_2.png)
![Azure Portal → Storage Account → Containers → bronze → yellow_tripdata → year folder list](photos/adf_3.png)


## Transformation — Bronze → Silver

**Script:** `sql/01_bronze_to_silver.sql`

Silver is the cleaned version of Bronze data. Strategy: **fix what you can, only remove impossible records.**

### Step 1: Bronze View (OPENROWSET)

The view `bronze.vw_yellow_taxi_raw` reads raw Parquet files directly from the Data Lake.

> **Note:** The `airport_fee` column has inconsistent casing across years (`airport_fee` in 2021, `Airport_fee` in 2025). Solution: read both versions and merge with `COALESCE`.

### Step 2: Fixing NULLs (COALESCE)

Instead of dropping rows with NULLs (~24% of data!), we fill them with sensible defaults:

| Column | Issue | Fix |
|--------|-------|-----|
| `passenger_count` | 24% NULL | → `1` (default 1 passenger) |
| `RatecodeID` | 24% NULL | → `1` (standard rate) |
| `store_and_fwd_flag` | 24% NULL | → `'N'` (not stored) |
| `congestion_surcharge` | 24% NULL | → `0.00` |
| `airport_fee` | 24-91% NULL | → `0.00` |
| `cbd_congestion_fee` | doesn't exist before 2024 | → `0.00` |

### Step 3: Filtering (WHERE)

We remove **only physically impossible records** (~4.5% of data):

| Filter | Removed | Reason |
|--------|---------|--------|
| `VendorID IN (1,2)` | 1.54% | Vendor 7 has 100% broken dates, Vendor 6 unofficial |
| `trip_distance > 0 AND < 500` | 2.62% | Zero distance = cancellation/GPS error |
| `pickup < dropoff` | 1.49% | 97% from Vendor 7 (reversed timestamps) |
| `duration 1-1440 min` | 2.56% | < 1 min = meter test, > 24h = forgotten |
| `LocationID 1-265` | 0.00% | Locations outside NYC |
| `Date 2021-2025` | 0.00% | Data outside ingestion range |

> **Total removed: ~4.5% | Retained: ~95.5%**

### Step 4: `trip_status` Flag

Negative amounts (refunds, complaints, disputes) **are not deleted** — they are flagged:

| `trip_status` | Description | Share |
|---------------|-------------|-------|
| `valid` | Normal trip | ~87% |
| `correction` | Refund/complaint (negative fare, negative total, or total > 1000) | ~8.5% |

This allows the Gold Layer to filter by `trip_status = 'valid'` for clean KPIs, while corrections remain available for separate analysis.

### Step 5: Column Standardization

- Names → `snake_case` (e.g. `VendorID` → `vendor_id`)
- Types → `DECIMAL(10,2)` for monetary amounts, `INT` for identifiers
- Derived columns: `trip_duration_minutes`, `trip_year`, `trip_month`, `trip_day`, `trip_weekday`, `pickup_hour`

![Synapse Studio → SQL Script → running 01_bronze_to_silver.sql](photos/bronze_silver_1.png)
![Azure Portal → Storage → silver container → yellow_taxi_cleaned → Parquet files](photos/bronze_silver_2.png)


---

## Transformation — Silver → Gold

**Script:** `sql/02_silver_to_gold.sql`

Gold is the business layer ready for BI tools (e.g. Power BI).
It is built as a **Star Schema** which provides native performance, easy DAX measure creation, and a unified time dimension.

### Star Schema (Entity Relationship)

```mermaid
erDiagram
    dim_date ||--o{ fact_trips : date_key
    dim_payment_type ||--o{ fact_trips : payment_key
    dim_date ||--o{ fact_corrections : date_key
    dim_payment_type ||--o{ fact_corrections : payment_key

    dim_date {
        int date_key PK
        date full_date
        int year
        int month
        int day
        varchar day_name
        varchar month_name
        varchar year_month
        int weekday_num
        int quarter
    }

    dim_payment_type {
        int payment_key PK
        varchar payment_name
        varchar payment_category
    }

    fact_trips {
        int date_key FK
        int payment_key FK
        int pickup_location_id
        int dropoff_location_id
        int pickup_hour
        int trip_count
        int total_passengers
        decimal total_revenue
        decimal total_fare
        decimal total_tips
        decimal total_tolls
        decimal total_congestion
        decimal total_extra
        decimal total_mta_tax
        decimal total_improvement
        decimal total_airport_fee
        decimal total_distance_miles
        decimal total_duration_minutes
        decimal avg_trip_cost
        decimal avg_fare
        decimal avg_tip
        decimal avg_distance_miles
        decimal avg_duration_minutes
        decimal revenue_per_mile
    }

    fact_corrections {
        int date_key FK
        int payment_key FK
        int pickup_location_id
        int correction_count
        decimal total_refunded_fare
        decimal total_refunded_amount
        decimal avg_refunded_fare
    }
```

> **Important:** The `trip_status` flag is used when splitting into fact tables: `fact_trips` takes only valid trips, while `fact_corrections` separately aggregates refunds and complaints to avoid skewing the main financial KPIs.

### Tables (Dimensions & Facts)

#### `gold.dim_date`
Calendar dimension with attributes such as day names, month names, and composite keys (`year_month` for Power BI). Enables Time Intelligence in aggregations.

#### `gold.dim_payment_type`
Payment method lookup table with mapped categories (Credit Card, Cash, Others).

#### `gold.fact_trips`
Central fact table with data aggregated at the *date + hour + payment + pu_location + do_location* level. Contains all metrics (fares, tips, distance, duration).

#### `gold.fact_corrections`
Separate table for analyzing cancellations and refunds, grouping negative trips.

![Synapse Studio → SQL Script → running 02_silver_to_gold.sql](photos/silver_gold_1.png)
![Azure Portal → Storage → gold container → folder list](photos/silver_gold_2.png)
![Synapse Studio → SELECT FROM gold.fact_trips → tabular results](photos/silver_gold_3.png)

---

## Data Quality Tests

### Silver Tests (`sql/03_tests_silver.sql`) — 18 tests

![Synapse Studio → running 03_tests_silver.sql → results](photos/silver_test.png)

### Gold Tests (`sql/04_tests_gold.sql`) — 16 tests

![Synapse Studio → running 04_tests_gold.sql → results](photos/gold_test.png)

---

## Getting Started

### Prerequisites

- Azure CLI (`az login`)
- Terraform >= 1.5
- Python 3.x + pandas (for local testing)

### Step by Step

```bash
# 1. Infrastructure
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars
terraform init
terraform plan
terraform apply
```

```bash
# 2. Ingestion — run the pipeline in ADF
# Azure Portal → Data Factory → pl_ingest_all → Trigger
```

```sql
-- 3. Synapse — run SQL scripts in order:
-- sql/00_setup.sql             ← database, credentials, data sources
-- sql/01_bronze_to_silver.sql  ← Bronze → Silver transformation
-- sql/03_tests_silver.sql      ← Silver validation
-- sql/02_silver_to_gold.sql    ← Silver → Gold transformation
-- sql/04_tests_gold.sql        ← Gold validation
```

> **Note:** In `sql/00_setup.sql`, replace `<storage_account_name>` with the value from `terraform output datalake_name` and `<your_master_key_password>` with your own password.

### Destroying Resources

> **⚠️ Warning:** This will delete **all** Azure resources and data (Bronze/Silver/Gold). This action cannot be undone.

```bash
terraform destroy
```

## Power BI Dashboards

The Power BI report connects to the Gold Layer via **DirectQuery** to Azure Synapse Serverless SQL Pool. The report consists of 4 pages, accessible through a navigation bar at the top.

> **Note:** The included `raport.pbix` file contains only 2 sample months of data loaded for demonstration purposes. After deploying your own infrastructure, connect the report to your Synapse endpoint to work with the full dataset (~200M records).

### Page 1: Executive Overview

![Power BI — Executive Overview dashboard](photos/bi_1.png)

The main landing page providing a high-level summary of the entire NYC Yellow Taxi dataset.

| Element | Description |
|---------|-------------|
| **KPI Cards** | Total Revenue ($4.78bn), Total Trips (186M), Avg Trip Cost ($25.73), Avg Tip ($3.06) — each with MoM % change |
| **Revenue Trend (Daily)** | Dual-axis line chart showing daily Total Revenue (yellow) and Total Trips (blue) from 2021–2025. Clearly shows the COVID recovery trend and seasonal patterns |
| **Revenue by Year** | Bar chart comparing total revenue across years — steady growth from $0.6bn (2021) to ~$1.1bn (2023–2025) |
| **Payment Methods** | Donut chart showing payment distribution: Credit Card 77.55%, Cash 13.18%, Others 9.27% |
| **Filters** | Year and Month slicers for interactive filtering |

### Page 2: Revenue Deep Dive

![Power BI — Revenue Deep Dive dashboard](photos/bi_2.png)

A detailed breakdown of revenue components and cost efficiency metrics.

| Element | Description |
|---------|-------------|
| **KPI Cards** | Total Revenue ($907.92M), Fare Revenue ($622.75M), Total Tips ($110.35M), Total Toll ($19.62M) — filtered view |
| **Revenue Breakdown by Component** | Stacked area chart showing monthly Fare Revenue, Total Tips, Total Congestion, and Total Toll over time |
| **Tip % of Fare** | Line chart showing the ratio of tips to fare over time (trending around 17–18%) |
| **Avg Revenue per Mile** | Line chart showing revenue efficiency per mile — growth from ~$8 (2021) to ~$20 (2025) |
| **Revenue Composition** | Donut chart — Fare Revenue 75.24%, Tips 13.33%, Congestion 9.05%, Toll 2.37% |
| **Avg Trip Cost by Weekday** | Horizontal bar chart — Thursday is the most expensive ($26.73), Saturday the cheapest ($24.47) |

### Page 3: Zone Analysis

![Power BI — Zone Analysis dashboard](photos/bi_3.png)

An in-depth look at trip patterns by NYC taxi zone — pickup and dropoff locations.

| Element | Description |
|---------|-------------|
| **KPI Cards** | Top Pickup Zone (Upper East Side South), Top Dropoff Zone (Upper East Side South), Unique Zones (263), Avg Distance (4.18 mi) |
| **Top 20 Pickup Zones** | Vertical bar chart — Upper East Side South leads with ~9M trips, followed by JFK Airport and Midtown Center |
| **Top 15 Dropoff Zones** | Vertical bar chart showing the most popular dropoff zones — Upper East Side South also leads |
| **Revenue by Zone — Treemap** | Treemap of top revenue-generating pickup zones: JFK Airport dominates (long-distance, high fare), followed by Midtown Center and Penn Station/Madison Sq |
| **Distance vs Duration (Scatter)** | Scatter plot revealing zone characteristics — airport zones (JFK, LaGuardia) cluster at high distance/duration; Manhattan zones cluster at low distance/short duration |
| **Filters** | Year and Month slicers for interactive filtering |

### Page 4: Temporal Patterns

![Power BI — Temporal Patterns dashboard](photos/bi_4.png)

Time-based analysis of trip patterns — hourly, daily, and seasonal trends.

| Element | Description |
|---------|-------------|
| **KPI Cards** | Peak Hour (18:00), Peak Day (Thursday), Weekend Ratio (0.27 = 27% of trips on weekends), Peak Month (October) |
| **Trips by Hour of Day** | Column chart (0–23h) — clear bimodal pattern with morning ramp-up from 6 AM, afternoon peak at 18:00, and a quiet period from 0–5 AM |
| **Hourly Heatmap (Day × Hour)** | Matrix with color scale (dark blue → yellow) — shows trip intensity for every day-hour combination. Busiest: Thursday/Friday 17–19h. Quietest: Sunday 3–5 AM |
| **Trips by Day of Week** | Bar chart — Thursday is the busiest day, Sunday the quietest. Weekdays consistently outperform weekends |
| **Monthly Seasonality** | Multi-line chart (one line per year 2021–2025) — clear seasonal pattern: dip in January/February, peak in October. 2021 line is visibly lower (COVID recovery) |
| **Filters** | Year and Month slicers for interactive filtering |

> 📖 **Build instructions for Pages 3 and 4:** [POWERBI_GUIDE.md](POWERBI_GUIDE.md)
