-- Silver is where the first pass of cleaning and type-safe modeling happens.
CREATE TABLE IF NOT EXISTS iceberg.silver.movies_clean (
    movie_id INTEGER,
    title VARCHAR,
    release_date DATE,
    release_year INTEGER,
    budget BIGINT,
    revenue BIGINT,
    vote_average DOUBLE,
    vote_count INTEGER,
    imdb_id VARCHAR,
    original_language VARCHAR,
    source_system VARCHAR,
    raw_object_path VARCHAR,
    ingested_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET'
);

DELETE FROM iceberg.silver.movies_clean;

-- This keeps the transformation readable while still being easy to rerun.
INSERT INTO iceberg.silver.movies_clean
SELECT
    movie_id,
    trim(title) AS title,
    release_date,
    year(release_date) AS release_year,
    coalesce(budget, 0) AS budget,
    coalesce(revenue, 0) AS revenue,
    coalesce(vote_average, 0.0) AS vote_average,
    coalesce(vote_count, 0) AS vote_count,
    imdb_id,
    coalesce(original_language, 'unknown') AS original_language,
    source_system,
    raw_object_path,
    ingested_at
FROM iceberg.bronze.movies_raw
WHERE movie_id IS NOT NULL
  AND title IS NOT NULL;
