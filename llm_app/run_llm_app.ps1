param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"

Push-Location $ProjectRoot
try {
    Write-Host "Starting the TEAD LLM add-on ..."
    streamlit run llm_app/app.py
}
finally {
    Pop-Location
}
