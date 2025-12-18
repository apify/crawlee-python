# ============================================================================
# Test Qdrant Cloud connection with OpenAI embeddings (PowerShell)
# Usage: .\scripts\test_qdrant_connection.ps1
# ============================================================================

# Navigate to project root
Set-Location $PSScriptRoot\..

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "QDRANT CLOUD CONNECTION TEST" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Check if virtual environment exists
if (-not (Test-Path "venv\Scripts\Activate.ps1")) {
    Write-Host "ERROR: Virtual environment not found!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please run .\scripts\setup.bat first to set up your environment."
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if .env file exists
if (-not (Test-Path ".env") -and -not (Test-Path ".env.local")) {
    Write-Host "WARNING: No .env file found!" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "You need to set the following environment variables:" -ForegroundColor Cyan
    Write-Host "  - QDRANT_URL (from https://cloud.qdrant.io)"
    Write-Host "  - QDRANT_API_KEY (from https://cloud.qdrant.io)"
    Write-Host "  - OPENAI_API_KEY (from https://platform.openai.com/api-keys)"
    Write-Host ""
    Write-Host "Copy .env.example to .env and add your credentials"
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Using Qdrant Cloud (no Docker required)" -ForegroundColor Green
Write-Host ""

# Activate virtual environment and run test
Write-Host "Activating virtual environment..." -ForegroundColor Cyan
& ".\venv\Scripts\Activate.ps1"

Write-Host "Running Qdrant connection test..." -ForegroundColor Cyan
Write-Host ""

python src\tax_rag_scraper\test_qdrant_connection.py

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Test complete!" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

Read-Host "Press Enter to exit"
