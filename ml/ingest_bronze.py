import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import trino


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "sample_movies.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "tmp" / "bronze_movies.parquet"


def build_batch_metadata(csv_path: Path) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    source_system = os.getenv("BRONZE_SOURCE_SYSTEM", "tmdb")
    source_dataset = os.getenv("BRONZE_SOURCE_DATASET", csv_path.stem)
    ingestion_batch_id = now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]

    return {
        "source_system": source_system,
        "source_dataset": source_dataset,
        "source_file_name": csv_path.name,
        "ingestion_batch_id": ingestion_batch_id,
        "ingested_at": now.isoformat(),
        "ingest_date": now.strftime("%Y-%m-%d"),
    }


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def build_parquet(rows: list[dict[str, str]], parquet_path: Path, metadata: dict[str, str]) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # Bronze keeps the payload close to the raw source, so we preserve values as delivered.
    table = pa.table(
        {
            "movie_id": [int(row["movie_id"]) for row in rows],
            "title": [row["title"] for row in rows],
            "release_date": [row["release_date"] for row in rows],
            "budget": [int(row["budget"]) for row in rows],
            "revenue": [int(row["revenue"]) for row in rows],
            "vote_average": [float(row["vote_average"]) for row in rows],
            "vote_count": [int(row["vote_count"]) for row in rows],
            "imdb_id": [row["imdb_id"] for row in rows],
            "original_language": [row["original_language"] for row in rows],
            "source_system": [metadata["source_system"]] * len(rows),
            "source_dataset": [metadata["source_dataset"]] * len(rows),
            "source_file_name": [metadata["source_file_name"]] * len(rows),
            "ingestion_batch_id": [metadata["ingestion_batch_id"]] * len(rows),
            "ingested_at": [metadata["ingested_at"]] * len(rows),
        }
    )
    pq.write_table(table, parquet_path)


def upload_to_minio(parquet_path: Path, object_name: str) -> str:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    bucket = os.getenv("MINIO_BUCKET", "lakehouse")
    s3.upload_file(str(parquet_path), bucket, object_name)
    return f"s3://{bucket}/{object_name}"


def cleanup_legacy_objects(metadata: dict[str, str]) -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    bucket = os.getenv("MINIO_BUCKET", "lakehouse")

    prefixes = [
        "bronze/raw/",
        (
            f"bronze/source_system={metadata['source_system']}/"
            f"source_dataset={metadata['source_dataset']}/"
        ),
    ]

    objects = []
    for prefix in prefixes:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects.extend(response.get("Contents", []))

    if objects:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": item["Key"]} for item in {item["Key"]: item for item in objects}.values()]},
        )


def ensure_bronze_table(
    rows: list[dict[str, str]],
    raw_object_path: str,
    metadata: dict[str, str],
) -> None:
    connection = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "tead"),
        catalog="iceberg",
        schema="bronze",
    )
    cursor = connection.cursor()

    # This table is the reproducible raw landing table for the prototype.
    cursor.execute(
        """
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
        )
        """
    )

    # Keep the table aligned with the latest prototype columns if it already exists.
    cursor.execute("ALTER TABLE iceberg.bronze.movies_raw ADD COLUMN IF NOT EXISTS source_dataset VARCHAR")
    cursor.execute("ALTER TABLE iceberg.bronze.movies_raw ADD COLUMN IF NOT EXISTS source_file_name VARCHAR")
    cursor.execute("ALTER TABLE iceberg.bronze.movies_raw ADD COLUMN IF NOT EXISTS ingestion_batch_id VARCHAR")

    cursor.execute("DELETE FROM iceberg.bronze.movies_raw")

    values_sql = []
    for row in rows:
        safe_title = row["title"].replace("'", "''")
        values_sql.append(
            "({movie_id}, '{title}', DATE '{release_date}', {budget}, {revenue}, {vote_average}, {vote_count}, '{imdb_id}', '{original_language}', '{source_system}', '{source_dataset}', '{source_file_name}', '{ingestion_batch_id}', '{raw_object_path}', from_iso8601_timestamp('{ingested_at}'))".format(
                movie_id=int(row["movie_id"]),
                title=safe_title,
                release_date=row["release_date"],
                budget=int(row["budget"]),
                revenue=int(row["revenue"]),
                vote_average=float(row["vote_average"]),
                vote_count=int(row["vote_count"]),
                imdb_id=row["imdb_id"],
                original_language=row["original_language"],
                source_system=metadata["source_system"],
                source_dataset=metadata["source_dataset"],
                source_file_name=metadata["source_file_name"],
                ingestion_batch_id=metadata["ingestion_batch_id"],
                raw_object_path=raw_object_path,
                ingested_at=metadata["ingested_at"],
            )
        )

    # We load the sample rows directly so the Iceberg table is queryable immediately.
    cursor.execute(
        """
        INSERT INTO iceberg.bronze.movies_raw (
            movie_id,
            title,
            release_date,
            budget,
            revenue,
            vote_average,
            vote_count,
            imdb_id,
            original_language,
            source_system,
            source_dataset,
            source_file_name,
            ingestion_batch_id,
            raw_object_path,
            ingested_at
        )
        VALUES
        {values}
        """.format(values=",\n        ".join(values_sql))
    )


def main() -> None:
    csv_path = Path(os.getenv("BRONZE_INPUT_CSV", str(DEFAULT_INPUT)))
    parquet_path = Path(os.getenv("BRONZE_OUTPUT_PARQUET", str(DEFAULT_OUTPUT)))
    metadata = build_batch_metadata(csv_path)

    rows = load_rows(csv_path)
    build_parquet(rows, parquet_path, metadata)

    # The raw landing path mirrors how a real source batch would be stored in object storage.
    object_name = (
        f"bronze/source_system={metadata['source_system']}/"
        f"source_dataset={metadata['source_dataset']}/"
        f"ingest_date={metadata['ingest_date']}/"
        f"{metadata['ingestion_batch_id']}.parquet"
    )
    cleanup_legacy_objects(metadata)
    raw_object_path = upload_to_minio(parquet_path, object_name)
    ensure_bronze_table(rows, raw_object_path, metadata)

    print(f"Wrote Parquet file to {parquet_path}")
    print(f"Uploaded raw Bronze object to {raw_object_path}")
    print(f"Ingestion batch id: {metadata['ingestion_batch_id']}")
    print("Loaded Iceberg table iceberg.bronze.movies_raw")


if __name__ == "__main__":
    main()
