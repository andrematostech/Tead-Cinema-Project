param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"

function Get-ComposeNetworkName {
    param(
        [string]$ContainerName = "tead-trino"
    )

    $networkName = docker inspect `
        --format '{{range $name, $value := .NetworkSettings.Networks}}{{println $name}}{{end}}' `
        $ContainerName

    $networkName = ($networkName | Select-Object -First 1).Trim()

    if (-not $networkName) {
        throw "Could not resolve the Docker network for container $ContainerName."
    }

    return $networkName
}

function Wait-ForTrino {
    param(
        [int]$MaxAttempts = 20
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $status = docker inspect `
            --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' `
            tead-trino

        if ($status.Trim() -eq "healthy") {
            return
        }

        if ($attempt -eq $MaxAttempts) {
            throw "Trino did not become healthy in time."
        }

        Start-Sleep -Seconds 3
    }
}

function Invoke-TrinoSql {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlFile
    )

    $sqlPath = Join-Path $ProjectRoot $SqlFile
    $sql = Get-Content -Raw $sqlPath
    $output = docker compose exec -T trino trino --execute $sql 2>&1

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to run $SqlFile`n$output"
    }

    Write-Host "Running $SqlFile ..."
    $output | Out-Host
}

Push-Location $ProjectRoot
try {
    Write-Host "Starting the lakehouse stack ..."
    docker compose up -d

    Write-Host "Waiting for Trino to accept queries ..."
    Wait-ForTrino

    $composeNetwork = Get-ComposeNetworkName

    Write-Host "Building the ingestion image ..."
    docker build -f ml/Dockerfile -t tead-cinema-ingest .

    Invoke-TrinoSql -SqlFile "sql/bronze/create_tables.sql"

    # The ingestion container writes a local Parquet file and uploads the same bytes to MinIO.
    Write-Host "Loading bronze data ..."
    docker run --rm `
        --network $composeNetwork `
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
