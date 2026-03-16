-- Gold exposes a small business-facing table we can use in analytics queries.
CREATE TABLE IF NOT EXISTS iceberg.gold.movie_performance (
    movie_id INTEGER,
    title VARCHAR,
    release_year INTEGER,
    budget BIGINT,
    revenue BIGINT,
    vote_average DOUBLE,
    vote_count INTEGER,
    roi DOUBLE,
    performance_bucket VARCHAR
)
WITH (
    format = 'PARQUET'
);

DELETE FROM iceberg.gold.movie_performance;

-- ROI is the first simple KPI for the movie prototype.
INSERT INTO iceberg.gold.movie_performance
SELECT
    movie_id,
    title,
    release_year,
    budget,
    revenue,
    vote_average,
    vote_count,
    CASE
        WHEN budget = 0 THEN NULL
        ELSE CAST(revenue - budget AS DOUBLE) / budget
    END AS roi,
    CASE
        WHEN budget = 0 THEN 'unknown'
        WHEN revenue >= budget * 3 THEN 'blockbuster'
        WHEN revenue >= budget THEN 'profitable'
        ELSE 'underperforming'
    END AS performance_bucket
FROM iceberg.silver.movies_clean;
