# TEAD Cinema Project

Minimal lakehouse prototype for movie analytics using MinIO, Hive Metastore, Trino, MLflow, and Iceberg.

## What is included

- Bronze, silver, and gold Iceberg schemas
- A small sample movie dataset for the first prototype
- A Python bronze ingestion script that writes Parquet, uploads it to MinIO, and loads an Iceberg bronze table with simple batch metadata
- Trino SQL scripts for silver and gold transformations

## Project paths

- Infrastructure: `docker-compose.yml`
- Hive image: `docker/hive/Dockerfile`
- Trino Iceberg catalog: `docker/trino/etc/catalog/iceberg.properties`
- Sample input: `data/sample_movies.csv`
- Ingestion code: `ml/ingest_bronze.py`
- Run script: `scripts/run_prototype.ps1`
- Validation script: `scripts/validate_prototype.ps1`
- Bronze SQL: `sql/bronze/create_tables.sql`
- Silver SQL: `sql/silver/transform_movies.sql`
- Gold SQL: `sql/gold/movie_performance.sql`

## Prototype flow

1. Start the stack with `docker compose up -d`
2. Create the schemas and bronze table from `sql/bronze/create_tables.sql`
3. Run `ml/ingest_bronze.py` inside the ingestion image
4. Execute the silver and gold SQL scripts through Trino
5. Query `iceberg.gold.movie_performance`

## What the bronze step now records

- `source_system`
- `source_dataset`
- `source_file_name`
- `ingestion_batch_id`
- `raw_object_path`

The raw file is stored in MinIO under a simple source-style layout such as:

- `bronze/source_system=tmdb/source_dataset=sample_movies/ingest_date=YYYY-MM-DD/<batch>.parquet`

For this prototype, rerunning the bronze ingestion keeps only the latest batch for the sample source path.

## Convenience commands

- Run the full prototype flow: `powershell -ExecutionPolicy Bypass -File .\scripts\run_prototype.ps1`
- Validate the generated tables: `powershell -ExecutionPolicy Bypass -File .\scripts\validate_prototype.ps1`

## UI decision

No separate UI is required for this prototype right now.
The validation script already shows:

- service health
- layer row counts
- bronze ingestion metadata
- raw bronze object paths in MinIO
- a gold table preview
