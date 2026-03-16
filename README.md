# TEAD Cinema Project

TEAD movie analytics lakehouse prototype built with Docker, MinIO, PostgreSQL, Hive Metastore, Trino, MLflow, Iceberg, Parquet, and a small LLM analytics add-on.

## Project goal

This project demonstrates a simple local lakehouse for movie analytics with three layers:

- Bronze: raw source-style data
- Silver: cleaned and normalized data
- Gold: analytical tables for consumption

The current prototype uses a small sample movie dataset and exposes one Gold table:

- `iceberg.gold.movie_performance`

## Current architecture

Core services:

- MinIO for object storage
- PostgreSQL for the Hive Metastore database
- Hive Metastore for Iceberg metadata
- Trino for SQL querying
- MLflow for experiment tracking

Data flow:

1. A sample CSV is read by Python
2. The Bronze step writes Parquet
3. The raw file is uploaded to MinIO
4. Bronze rows are loaded into an Iceberg table
5. Silver SQL cleans and reshapes the data
6. Gold SQL creates an analytical movie performance table

LLM add-on:

- A separate Streamlit app accepts natural-language questions
- OpenAI generates SQL
- The SQL is validated
- Trino executes the query
- Results and a short explanation are shown in the UI

## Project structure

- `docker-compose.yml`: local infrastructure stack
- `docker/hive/Dockerfile`: custom Hive Metastore image
- `docker/hive/conf/hive-site.xml`: Hive Metastore configuration
- `docker/trino/etc/catalog/iceberg.properties`: Trino Iceberg catalog
- `data/sample_movies.csv`: sample movie input data
- `ml/ingest_bronze.py`: Bronze ingestion script
- `sql/bronze/create_tables.sql`: Bronze schema and table definitions
- `sql/silver/transform_movies.sql`: Silver transformation
- `sql/gold/movie_performance.sql`: Gold analytical table
- `scripts/run_prototype.ps1`: full prototype runner
- `scripts/validate_prototype.ps1`: environment and data validation
- `llm_app/`: separate LLM analytics assistant

## Bronze metadata

The Bronze table keeps a small amount of source and batch lineage:

- `source_system`
- `source_dataset`
- `source_file_name`
- `ingestion_batch_id`
- `raw_object_path`
- `ingested_at`

The raw object is stored in MinIO with a source-style layout like:

- `bronze/source_system=tmdb/source_dataset=sample_movies/ingest_date=YYYY-MM-DD/<batch>.parquet`

## Run the lakehouse

From the project root:

```powershell
docker compose up -d
powershell -ExecutionPolicy Bypass -File .\scripts\run_prototype.ps1
```

What this does:

- starts the Docker stack
- waits for Trino to become healthy
- builds the ingestion image
- creates Bronze schemas and tables
- loads Bronze data
- runs Silver SQL
- runs Gold SQL

## Validate the prototype

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\validate_prototype.ps1
```

The validation script checks:

- container health
- layer row counts
- Bronze ingestion metadata
- raw Bronze objects in MinIO
- a Gold table preview

## Access points

- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Trino: `http://localhost:8080`
- MLflow UI: `http://localhost:5000`

## Gold table currently available

Current Gold scope:

- `iceberg.gold.movie_performance`

Main columns:

- `movie_id`
- `title`
- `release_year`
- `budget`
- `revenue`
- `vote_average`
- `vote_count`
- `roi`
- `performance_bucket`

## Run the LLM analytics assistant

If Python is available on your machine:

```powershell
python -m pip install -r .\llm_app\requirements.txt
python -m streamlit run .\llm_app\app.py
```

If Python is not available on your machine, run the UI in Docker:

```powershell
docker run --rm `
  --network tead-cinema-project-master_default `
  -p 8501:8501 `
  -v "C:\Users\André Matos\desktop\work\tead-cinema-project-master:/app" `
  -w /app `
  --env-file llm_app/.env `
  python:3.10-slim `
  /bin/sh -c "pip install --no-cache-dir -r llm_app/requirements.txt && streamlit run llm_app/app.py --server.address 0.0.0.0 --server.port 8501"
```

Then open:

- `http://localhost:8501`

## LLM assistant scope

The assistant is intentionally limited to the current Gold schema.

It can answer questions about:

- revenue
- ROI
- release years
- vote metrics
- performance buckets
- language-level patterns

It cannot reliably answer questions that require missing Gold data, such as:

- directors
- actors
- genres
- studios
- crew joins

Unsupported questions are blocked or downgraded by design.

## Example questions

- Which movies have the highest revenue-to-budget ratio?
- Which movies have the highest revenue?
- What are the most profitable movies by ROI?
- Which release years have the highest average revenue?
- Which performance buckets contain the most movies?
- Which languages are associated with higher revenue?

## Notes

- SQL schema ownership for Bronze lives in SQL, not in the Python ingestion script
- The LLM add-on is separate from the Bronze, Silver, and Gold pipeline
- The prototype is intentionally simple and local-first
