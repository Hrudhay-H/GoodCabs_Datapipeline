# GoodCabs Transportation Data Pipeline

A Databricks **Lakeflow Declarative Pipelines** (formerly Delta Live Tables)
project that builds a medallion-architecture (Bronze → Silver → Gold)
lakehouse for a fictional cab company, **GoodCabs**, operating across
several Indian cities.

## Business scenario

GoodCabs' trip and ride data originates in an operational relational
database behind an application server (see `docs/architecture.png`). That
data is periodically exported to cloud object storage (simulated here as an
S3 bucket, `s3://goodcabs/data-store/...`) as daily CSV files, which this
pipeline then ingests, cleans, and models for analytics (dashboards / Genie)
in Databricks.

```
Trip app servers → Relational DB → Data Fetch Service → S3 → Databricks (Bronze → Silver → Gold) → Dashboards / Genie
```

## Repository layout

```
goodcabs-transportation-pipeline/
├── README.md
├── docs/
│   └── architecture.png        # End-to-end system/data flow diagram
├── notebooks/
│   └── project_setup.ipynb     # One-time setup: creates the catalog & bronze/silver/gold schemas
├── pipelines/                  # Lakeflow Declarative Pipeline source (Python + SQL)
│   ├── bronze/
│   │   ├── city.py             # Batch load of city.csv into transportation.bronze.city
│   │   └── trips.py            # Auto Loader streaming ingestion into transportation.bronze.trips
│   ├── silver/
│   │   ├── calendar.py         # Generated date dimension with Indian public holidays
│   │   ├── city.py             # Cleaned city dimension
│   │   └── trips.py            # Cleaned trips + CDC upsert (SCD Type 1) into transportation.silver.trips
│   └── gold/
│       ├── fact_trips.sql           # Master fact view joining trips + city + calendar
│       └── fact_trips_<city>.sql    # Per-city filtered views (10 cities)
└── data/                        # Sample/source data, as it would land in object storage
    ├── city/
    │   └── city.csv              # City lookup (city_id, city_name) for 10 cities
    └── trips/
        ├── full_load/             # 148 daily trip export files: 2025-08-01 → 2025-12-26
        └── incremental_load/      # 5 daily trip export files: 2025-12-27 → 2025-12-31 (simulates new arrivals)
```

## Data model

**Source files**
- `data/city/city.csv` — `city_id, city_name` (e.g. `RJ01, Jaipur`)
- `data/trips/**/trip_export_*.csv` — `trip_id, date, city_id, passenger_type,
  distance_travelled(km), fare_amount, passenger_rating, driver_rating`

**Bronze** (`transportation.bronze`) — raw ingestion, schema-on-read, adds
`file_name` and `ingest_datetime` lineage columns.
- `city` — materialized view, batch CSV read of the full city directory.
- `trips` — streaming table using **Auto Loader** (`cloudFiles`) with schema
  evolution so new daily files are picked up incrementally.

**Silver** (`transportation.silver`) — cleaned, typed, business-friendly
column names.
- `city` — materialized view selecting/aliasing bronze columns.
- `calendar` — generated date spine (`start_date`/`end_date` pipeline
  parameters) with weekday/weekend/holiday flags.
- `trips` — a streaming view (`trips_silver_staging`) feeding a
  **CDC (`create_auto_cdc_flow`) upsert** into `silver.trips`, keyed by
  trip `id`, SCD Type 1 (latest value wins).

**Gold** (`transportation.gold`) — star-schema-style consumption layer.
- `fact_trips` — joins silver `trips` + `city` + `calendar` into one wide
  analytical view.
- `fact_trips_<city>` — one filtered view per city (Chandigarh, Coimbatore,
  Indore, Jaipur, Kochi, Lucknow, Mysore, Surat, Vadodara, Visakhapatnam) for
  city-scoped dashboards/access.

## Running it

1. Open `notebooks/project_setup.ipynb` in a Databricks workspace and run it
   once to create the `transportation` catalog and its `bronze`/`silver`/`gold`
   schemas in Unity Catalog.
2. Upload the contents of `data/` to the configured source locations
   (`s3://goodcabs/data-store/city` and `s3://goodcabs/data-store/trips`),
   or point the `SOURCE_PATH` variables in `pipelines/bronze/*.py` at your
   own storage location.
3. Create a Lakeflow Declarative Pipeline pointing at the `pipelines/`
   directory (or `pipelines/bronze` + `pipelines/silver` + `pipelines/gold`)
   and run it — set the `start_date`/`end_date` pipeline configuration
   values used by `pipelines/silver/calendar.py`.
4. Use `data/trips/full_load/` for the initial backfill and drop the files
   in `data/trips/incremental_load/` in afterwards to see the Auto
   Loader + CDC pipeline pick up new/incremental data.

## Notes on original layout

This project was reorganized from an original `1. data / 2. codes /
3. other_files` numbered-folder layout into named folders
(`data/pipelines/notebooks/docs`) for clarity, and the `gold/*.sql` files
were renamed from `trips_<city>.sql` to `fact_trips_<city>.sql` to match the
view names they actually create.
