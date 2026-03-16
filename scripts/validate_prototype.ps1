param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"

function Wait-ForTrino {
    param(
        [int]$MaxAttempts = 20
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            docker compose exec -T trino trino --execute "SELECT 1" | Out-Null
            return
        }
        catch {
            if ($attempt -eq $MaxAttempts) {
                throw
            }

            Start-Sleep -Seconds 3
        }
    }
}

Push-Location $ProjectRoot
try {
    Write-Host "Checking service health ..."
    docker compose ps | Out-Host

    Write-Host ""
    Write-Host "Waiting for Trino to accept queries ..."
    Wait-ForTrino

    Write-Host ""
    Write-Host "Checking layer row counts ..."
    docker compose exec -T trino trino --execute @"
SELECT 'bronze' AS layer, count(*) AS row_count FROM iceberg.bronze.movies_raw
UNION ALL
SELECT 'silver' AS layer, count(*) AS row_count FROM iceberg.silver.movies_clean
UNION ALL
SELECT 'gold' AS layer, count(*) AS row_count FROM iceberg.gold.movie_performance
"@ | Out-Host

    Write-Host ""
    Write-Host "Checking bronze ingestion metadata ..."
    docker compose exec -T trino trino --execute @"
SELECT source_system, source_dataset, source_file_name, ingestion_batch_id, raw_object_path
FROM iceberg.bronze.movies_raw
LIMIT 3
"@ | Out-Host

    Write-Host ""
    Write-Host "Checking raw bronze objects in MinIO ..."
    docker run --rm `
        --network tead-cinema-project-master_default `
        --entrypoint /bin/sh `
        minio/mc:RELEASE.2025-02-21T16-00-46Z `
        -c "mc alias set local http://minio:9000 minio minio123 >/dev/null && mc find local/lakehouse/bronze --name '*.parquet'" | Out-Host

    Write-Host ""
    Write-Host "Previewing the gold table ..."
    docker compose exec -T trino trino --execute @"
SELECT movie_id, title, revenue, budget, round(roi, 2) AS roi, performance_bucket
FROM iceberg.gold.movie_performance
ORDER BY revenue DESC
LIMIT 5
"@ | Out-Host
}
finally {
    Pop-Location
}
