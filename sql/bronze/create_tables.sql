-- Keep the first prototype explicit: one Iceberg schema per lakehouse layer.
CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3a://lakehouse/warehouse/bronze');

CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3a://lakehouse/warehouse/silver');

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3a://lakehouse/warehouse/gold');

-- Bronze stays close to the source payload so we can trace every downstream row.
CREATE TABLE IF NOT EXISTS iceberg.bronze.movies_raw (
    movie_id INTEGER,
    title VARCHAR,
    release_date DATE,
    budget BIGINT,
    revenue BIGINT,
    vote_average DOUBLE,
    vote_count INTEGER,
    imdb_id VARCHAR,
    original_language VARCHAR,
    source_system VARCHAR,
    source_dataset VARCHAR,
    source_file_name VARCHAR,
    ingestion_batch_id VARCHAR,
    raw_object_path VARCHAR,
    ingested_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET'
);

-- Keep the table forward-compatible when the prototype evolves.
ALTER TABLE iceberg.bronze.movies_raw ADD COLUMN IF NOT EXISTS source_dataset VARCHAR;
ALTER TABLE iceberg.bronze.movies_raw ADD COLUMN IF NOT EXISTS source_file_name VARCHAR;
ALTER TABLE iceberg.bronze.movies_raw ADD COLUMN IF NOT EXISTS ingestion_batch_id VARCHAR;
