[![en](https://img.shields.io/badge/lang-English-blue.svg)](README.md)
[![pl](https://img.shields.io/badge/lang-Polski-red.svg)](README_PL.md)

# Azure NYC Taxi — Data Lakehouse

Hurtownia danych dla NYC Yellow Taxi zbudowana na platformie Azure w architekturze Medallion (Bronze → Silver → Gold).

> **Źródło danych:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
> **Skonfigurowany zakres ingestion:** Yellow Taxi, styczeń 2021 – grudzień 2025 (~200M rekordów dla pełnego zakresu 2021-2025)

---

## Spis treści

1. [Architektura](#architektura)
2. [Infrastruktura (Terraform)](#infrastruktura-terraform)
3. [Ingestion — Bronze Layer](#ingestion--bronze-layer)
4. [Profilowanie danych — Bronze Layer](#profilowanie-danych--bronze-layer)
5. [Transformacja — Bronze → Silver](#transformacja--bronze--silver)
6. [Transformacja — Silver → Gold](#transformacja--silver--gold)
7. [Testy jakości danych](#testy-jakości-danych)
8. [Uruchomienie projektu](#uruchomienie-projektu)
9. [Dashboardy Power BI](#dashboardy-power-bi)


---

## Architektura

![Architektura](photos/t.png)

> **Storage:** Wszystkie warstwy (Bronze/Silver/Gold) → Azure Data Lake Storage Gen2

| Warstwa | Opis | Format | Lokalizacja |
|---------|------|--------|-------------|
| **Bronze** | Surowe dane bez zmian | Parquet (Snappy) | `bronze/yellow_tripdata/` |
| **Silver** | Wyczyszczone, ustandaryzowane | Parquet (Snappy) | `silver/yellow_taxi_cleaned/` |
| **Gold** | Schemat Gwiazdy (KPI) | Parquet | `gold/*/` |

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

---

## Ingestion — Bronze Layer

Azure Data Factory pobiera pliki Parquet z NYC TLC API i zapisuje je w ADLS Gen2 (Bronze).

> **Aktualny stan repo:** Terraform definiuje pipeline'y ADF dla ingestu do Bronze oraz kopiowania metadanych. Transformacje SQL dla Silver/Gold są w obecnej wersji projektu uruchamiane ręcznie w Synapse Studio.

### Pipeline

```
pl_ingest_all_data (ForEach year 2021-2025)
  └── pl_ingest_year (ForEach month 01-12)
        └── pl_ingest_single_month (Copy Activity)
              Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet
              Sink:   bronze/yellow_tripdata/{year}/yellow_tripdata_{year}-{month}.parquet

pl_ingest_metadata
  └── Copy taxi_zone_lookup.csv do bronze/metadata/
```

| Parametr | Wartość |
|----------|---------|
| Równoległość | 4 miesiące jednocześnie |
| Retry | 2 próby, 30s przerwa |
| Timeout | 1h na plik |
| Kompresja | Snappy |

> **Uwaga:** Główny pipeline ingestion iteruje po wszystkich miesiącach 2025. Jeżeli późny plik źródłowy nie jest jeszcze dostępny w NYC TLC w momencie uruchomienia, dany miesiąc może zakończyć się błędem i wymagać ponownej próby później.

![Azure Data Factory → Pipeline "pl_ingest_year" → widok edytora z ForEach](photos/adf_1.png)
![Azure Data Factory → Monitor → zakończone pipeline runy](photos/adf_2.png)
![Azure Portal → Storage Account → Containers → bronze → yellow_tripdata → lista folderów z latami](photos/adf_3.png)


## Profilowanie danych — Bronze Layer

**Skrypt:** `sql/01a_bronze_profiling.sql`

Przed jakimkolwiek czyszczeniem profilujemy surowe dane Bronze, aby zrozumieć problemy z jakością i uzasadnić każdą decyzję o transformacji. Skrypt wykonuje 13 analiz:

| # | Analiza | Kluczowy wynik |
|---|---------|----------------|
| 1 | Przegląd ogólny | Łączna liczba rekordów, zakres dat, liczba plików |
| 2 | Rekordy per rok | Rozkład wolumenu w latach 2021–2025 |
| 3 | Analiza NULLi | `passenger_count`, `RatecodeID`, `store_and_fwd_flag` — **~24% NULL** |
| 4 | NULLe per rok | Które roczniki wprowadzają problem NULLi |
| 5 | Analiza vendorów | Vendor 7 ma ~100% zepsutych dat |
| 6 | Dystans przejazdu | Zerowy/ujemny dystans (~2.6%) |
| 7 | Czas przejazdu | Odwrócone daty, <1 min, >24h |
| 8 | Location ID | Wartości poza zakresem NYC TLC (1–265) |
| 9 | Anomalie finansowe | Ujemne opłaty, korekty (~8.5%) |
| 10 | Typ płatności | Rozkład metod płatności |
| 11 | Zakres dat | Rekordy poza oczekiwanym oknem 2021–2025 |
| 12 | **DROP vs COALESCE** | **~24% strata przy naiwnym DROP vs ~4.5% przy smart filtering** |
| 13 | Wpływ filtrów | Ile rekordów usuwa każdy poszczególny filtr |

> **Kluczowy wniosek (Analiza #12):** Usunięcie wierszy z NULLami spowodowałoby utratę ~24% danych. Strategia COALESCE w Silver zachowuje te rekordy, wypełniając NULLe sensownymi wartościami domyślnymi, redukując stratę do ~4.5%.

---

## Transformacja — Bronze → Silver

**Skrypt:** `sql/01_bronze_to_silver.sql`

Silver to wyczyszczona wersja danych Bronze. Strategia: **napraw co się da, usuń tylko błedne rekordy.**

### Krok 1: Widok Bronze (OPENROWSET)

Widok `bronze.vw_yellow_taxi_raw` czyta surowe pliki Parquet bezpośrednio z Data Lake.

> **Uwaga:** Kolumna `airport_fee` ma różną wielkość liter między latami (`airport_fee` w 2021, `Airport_fee` w 2025). Rozwiązanie: czytamy obie wersje i łączymy `COALESCE`.

### Krok 2: Naprawianie NULLi (COALESCE)

Zamiast usuwać wiersze z NULLami (~24% danych!), naprawiamy je sensownymi wartościami domyślnymi:

| Kolumna | Problem | Rozwiązanie |
|---------|---------|-------------|
| `passenger_count` | 24% NULL | → `1` (domyślnie 1 pasażer) |
| `RatecodeID` | 24% NULL | → `1` (taryfa standardowa) |
| `store_and_fwd_flag` | 24% NULL | → `'N'` (nie przechowywano) |
| `congestion_surcharge` | 24% NULL | → `0.00` |
| `airport_fee` | 24-91% NULL | → `0.00` |
| `cbd_congestion_fee` | nie istnieje do 2024 | → `0.00` |

### Krok 3: Filtrowanie (WHERE)

Usuwamy **tylko fizycznie niemożliwe rekordy** (~4.5% danych):

| Filtr | Usunięte | Dlaczego |
|-------|----------|----------|
| `VendorID IN (1,2)` | 1.54% | Vendor 7 ma 100% zepsutych dat, Vendor 6 nieoficjalny |
| `trip_distance > 0 AND < 500` | 2.62% | Zerowy dystans = anulacja/błąd GPS |
| `pickup < dropoff` | 1.49% | 97% to Vendor 7 (odwrócone daty) |
| `duration 1-1440 min` | 2.56% | < 1 min = test taksometru, > 24h = zapomniany |
| `LocationID 1-265` | 0.00% | Lokalizacje poza NYC |
| `Date 2021-2025` | 0.00% | Dane spoza zakresu ingestion |

> **Łącznie usunięto: ~4.5% | Zachowano: ~95.5%**

### Krok 4: Flaga `trip_status`

Ujemne kwoty (zwroty, reklamacje, spory) **nie są usuwane** — są oznaczone flagą:

| `trip_status` | Opis | Udział |
|---------------|------|--------|
| `valid` | Normalny kurs | ~87% |
| `correction` | Zwrot/reklamacja (ujemny fare, ujemny total lub total > 1000) | ~8.5% |

Dzięki temu Gold Layer może filtrować po `trip_status = 'valid'` dla czystych KPI, a korekty są dalej dostępne do osobnej analizy.

### Krok 5: Standaryzacja kolumn

- Nazwy → `snake_case` (np. `VendorID` → `vendor_id`)
- Typy → `DECIMAL(10,2)` dla kwot, `INT` dla identyfikatorów
- Kolumny pochodne: `trip_duration_minutes`, `trip_year`, `trip_month`, `trip_day`, `trip_weekday`, `pickup_hour`

![Synapse Studio → SQL Script → uruchomiony 01_bronze_to_silver.sql](photos/bronze_silver_1.png)
![Azure Portal → Storage → silver container → yellow_taxi_cleaned → pliki Parquet](photos/bronze_silver_2.png)


---

## Transformacja — Silver → Gold

**Skrypt:** `sql/02_silver_to_gold.sql`

Gold to warstwa biznesowa gotowa do podłączenia pod systemy klasy BI (np. Power BI).
Została zbudowana w **Schemacie Gwiazdy (Star Schema)** co daje natywną wydajność, łatwość budowania miar (DAX) i ujednolicony wymiar czasu.

### Schemat Gwiazdy (Entity Relationship)

```mermaid
erDiagram
    dim_date ||--o{ fact_trips : date_key
    dim_payment_type ||--o{ fact_trips : payment_key
    dim_location ||--o{ fact_trips : pickup_location_id
    dim_location ||--o{ fact_trips : dropoff_location_id
    dim_date ||--o{ fact_corrections : date_key
    dim_payment_type ||--o{ fact_corrections : payment_key
    dim_location ||--o{ fact_corrections : pickup_location_id

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

    dim_location {
        int location_id PK
        varchar borough
        varchar zone_name
        varchar service_zone
    }

    fact_trips {
        int date_key FK
        int payment_key FK
        int pickup_location_id FK
        int dropoff_location_id FK
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
        int pickup_location_id FK
        int correction_count
        decimal total_refunded_fare
        decimal total_refunded_amount
        decimal avg_refunded_fare
    }
```

> **Ważne:** Zastosowano flagę `trip_status` w trakcie rozdzielania na fakty: `fact_trips` bierze wyłącznie poprawne kursy, a `fact_corrections` oddzielnie agreguje zwroty i reklamacje by nie zaburzać głównych KPI finansowych.

### Tabele (Dimensions & Facts)

#### `gold.dim_date`
Wymiar kalendarzowy z atrybutami, np. nazwy dni, miesięcy i złączone klucze (`year_month` dla Power BI). Pozwala na używanie Time Intelligence w agregacjach.

#### `gold.dim_payment_type`
Słownik sposobów płatności ze zmapowanymi kategoriami (Credit Card, Cash, Others).

#### `gold.dim_location`
Wymiar lokalizacji ze wszystkimi 265 oficjalnymi strefami NYC TLC. Zawiera nazwę dzielnicy (borough), nazwę strefy i kategorię serwisową (Yellow Zone, Boro Zone, Airports, EWR). Dane ze źródła [NYC TLC Taxi Zone Lookup](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

#### `gold.fact_trips`
Centralna tabela faktów z danymi agregowanymi na poziomie *date + hour + payment + pu_location + do_location*. Posiada wszystkie metryki (opłaty, napiwki, dystans, czas trwania).

#### `gold.fact_corrections`
Wydzielona tabela do analizy anulacji i zwrotów, grupująca negatywne przejazdy.

![Synapse Studio → SQL Script → uruchomiony 02_silver_to_gold.sql](photos/silver_gold_1.png)
![Azure Portal → Storage → gold container → lista folderów](photos/silver_gold_2.png)
![Synapse Studio → SELECT FROM gold.fact_trips → wynik tabelaryczny](photos/silver_gold_3.png)

---

## Testy jakości danych

### Silver Tests (`sql/03_tests_silver.sql`) — 18 testów

![Synapse Studio → uruchomiony 03_tests_silver.sql → wyniki](photos/silver_test.png)

### Gold Tests (`sql/04_tests_gold.sql`) — 21 testów

![Synapse Studio → uruchomiony 04_tests_gold.sql → wyniki](photos/gold_test.png)

### Audyt ETL (`sql/05_etl_audit.sql`)

Snapshot audytowy po zakończeniu pipeline’u, zapisuje metryki zdrowia pipeline’u w tabeli `gold.etl_audit`:

| Metryka | Opis |
|---------|------|
| Liczba wierszy | Bronze, Silver (valid/correction), Gold per tabela |
| Strata danych % | `(1 - Silver/Bronze) × 100` — musi być ≤ 10% |
| Rekoncyliacja przychodów | Gold revenue vs Silver valid revenue (różnica < $1) |
| Rekoncyliacja wierszy | `SUM(trip_count)` w Gold = `COUNT(*)` w Silver |
| Świeżość danych | Dni od ostatniego rekordu w Bronze |
| Status pipeline | `HEALTHY` jeśli wszystko OK, `NEEDS ATTENTION` w przeciwnym razie |

---

## Uruchomienie projektu

### Wymagania

- Azure CLI (`az login`)
- Terraform >= 1.5
- Dostęp do Synapse Studio lub innego klienta SQL, który potrafi uruchomić dołączone skrypty

### Krok po kroku

```bash
# 1. Infrastruktura
cp terraform.tfvars.example terraform.tfvars
# Edytuj terraform.tfvars
terraform init
terraform plan
terraform apply
```

```bash
# 2. Ingestion — uruchom pipeline'y w ADF
# Azure Portal → Data Factory → pl_ingest_all_data → Trigger
# Azure Portal → Data Factory → pl_ingest_metadata  → Trigger
```

```sql
-- 3. Synapse — uruchom skrypty SQL, żeby utworzyć tabele, widoki i procedury:
-- sql/00_setup.sql              ← baza danych, credentials, data sources, file format

-- Aktualny stan repo: najpierw utwórz schematy ręcznie
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- Następnie uruchom:
-- sql/01a_bronze_profiling.sql  ← tworzy procedurę bronze.sp_run_profiling
-- sql/01_bronze_to_silver.sql   ← tworzy widok i procedurę silver.sp_load_yellow_taxi_incremental
-- sql/03_tests_silver.sql       ← tworzy procedurę silver.sp_run_tests
-- sql/02_silver_to_gold.sql     ← tworzy procedurę gold.sp_load_gold_layer
-- sql/04_tests_gold.sql         ← tworzy procedurę gold.sp_run_tests
-- sql/05_etl_audit.sql          ← tworzy procedurę gold.sp_run_etl_audit

-- 4. W obecnej wersji repo kroki SQL uruchamiane są ręcznie:
EXEC bronze.sp_run_profiling;

-- Ta procedura przyjmuje rok i miesiąc.
-- Jest to incremental load na poziomie miesiąca, ale repo nie zawiera jeszcze
-- trwałej tabeli Watermark ani pipeline'u ADF orkiestrującego Silver/Gold.
EXEC silver.sp_load_yellow_taxi_incremental '2021', '01';
EXEC silver.sp_run_tests;
EXEC gold.sp_load_gold_layer;
EXEC gold.sp_run_tests;
EXEC gold.sp_run_etl_audit;
```

> **Uwaga:** W `sql/00_setup.sql` zamień `<storage_account_name>` na wartość z `terraform output datalake_name` oraz `<your_master_key_password>` na własne hasło.

### Usunięcie zasobów

> **⚠️ Uwaga:** To usunie **wszystkie** zasoby Azure i dane (Bronze/Silver/Gold). Nie da się tego cofnąć.

```bash
terraform destroy
```

## Dashboardy Power BI

Raport Power BI łączy się z warstwą Gold przez **DirectQuery** do Azure Synapse Serverless SQL Pool. Raport składa się z 4 stron, dostępnych przez pasek nawigacji u góry.

> **Uwaga:** Załączony plik `raport.pbix` zawiera jedynie 2 przykładowe miesiące danych wgrane w celach demonstracyjnych. Po wdrożeniu własnej infrastruktury podłącz raport do swojego endpointu Synapse, aby pracować z pełnym zbiorem danych (~200M rekordów).

### Strona 1: Executive Overview

![Power BI — Executive Overview dashboard](photos/bi_1.png)


### Strona 2: Revenue Deep Dive

![Power BI — Revenue Deep Dive dashboard](photos/bi_2.png)



### Strona 3: Zone Analysis

![Power BI — Zone Analysis dashboard](photos/bi_3.png)



### Strona 4: Temporal Patterns

![Power BI — Temporal Patterns dashboard](photos/bi_4.png)




