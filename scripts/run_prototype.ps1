param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"

function Invoke-TrinoSql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlFile
    )

    $sqlPath = Join-Path $ProjectRoot $SqlFile
    $sql = Get-Content -Raw $sqlPath

    Write-Host "Running $SqlFile ..."
    docker compose exec -T trino trino --execute $sql | Out-Host
}

Push-Location $ProjectRoot
try {
    Write-Host "Starting the lakehouse stack ..."
    docker compose up -d

    Write-Host "Building the ingestion image ..."
    docker build -f ml/Dockerfile -t tead-cinema-ingest .

    Invoke-TrinoSql -SqlFile "sql/bronze/create_tables.sql"

    # The ingestion container writes a local Parquet file and uploads the same bytes to MinIO.
    Write-Host "Loading bronze data ..."
    docker run --rm `
        --network tead-cinema-project-master_default `
        -v "${ProjectRoot}\tmp:/app/tmp" `
        tead-cinema-ingest `
        python /app/ml/ingest_bronze.py | Out-Host

    Invoke-TrinoSql -SqlFile "sql/silver/transform_movies.sql"
    Invoke-TrinoSql -SqlFile "sql/gold/movie_performance.sql"

    Write-Host "Prototype pipeline completed."
}
finally {
    Pop-Location
}
