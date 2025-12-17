# ============================================================================
# Test full crawler integration with Qdrant (PowerShell)
# Usage: .\scripts\test_qdrant_integration.ps1
# ============================================================================

# Navigate to project root
Set-Location $PSScriptRoot\..

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "QDRANT INTEGRATION TEST" -ForegroundColor Cyan
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
    Write-Host "WARNING: No .env or .env.local file found!" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "You need to set OPENAI_API_KEY for this test to work."
    Write-Host ""
    Write-Host "Options:" -ForegroundColor Cyan
    Write-Host "  1. Copy .env.example to .env and add your API key"
    Write-Host "  2. Set environment variable: `$env:OPENAI_API_KEY='sk-proj-...'"
    Write-Host ""
    Write-Host "Get your API key from: https://platform.openai.com/api-keys"
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if Qdrant is running
Write-Host "Checking if Qdrant is running..." -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri "http://localhost:6333/health" -UseBasicParsing -ErrorAction Stop
    Write-Host "Qdrant is running!" -ForegroundColor Green
} catch {
    Write-Host ""
    Write-Host "WARNING: Qdrant doesn't appear to be running!" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Please start Qdrant first:" -ForegroundColor Cyan
    Write-Host "  .\scripts\setup_qdrant.ps1"
    Write-Host "  OR: docker compose up -d"
    Write-Host ""
    Write-Host "Then wait 10-15 seconds for initialization."
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""

# Activate virtual environment and run test
Write-Host "Activating virtual environment..." -ForegroundColor Cyan
& ".\venv\Scripts\Activate.ps1"

Write-Host "Running Qdrant integration test..." -ForegroundColor Cyan
Write-Host "This will crawl real CRA pages and generate embeddings." -ForegroundColor Yellow
Write-Host "Cost: approximately `$0.001-0.01 in OpenAI API usage." -ForegroundColor Yellow
Write-Host ""

python src\tax_rag_scraper\test_qdrant_integration.py

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Test complete!" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Check Qdrant dashboard: http://localhost:6333/dashboard"
Write-Host "  2. View Docker logs: docker compose logs qdrant"
Write-Host ""

Read-Host "Press Enter to exit"
