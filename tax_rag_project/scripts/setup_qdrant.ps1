# ============================================================================
# Setup and start Qdrant vector database (PowerShell)
# Usage: .\scripts\setup_qdrant.ps1
# ============================================================================

# Navigate to project root
Set-Location $PSScriptRoot\..

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "QDRANT SETUP" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is installed
try {
    $dockerVersion = docker --version 2>$null
    Write-Host "Docker is installed: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Docker is not installed or not in PATH!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Docker Desktop from:"
    Write-Host "  https://www.docker.com/products/docker-desktop"
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if Docker is running
try {
    docker info 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker not running"
    }
    Write-Host "Docker is running!" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Docker is not running!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please start Docker Desktop and try again." -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""

# Check if docker-compose.yml exists
if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "ERROR: docker-compose.yml not found!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Make sure you're in the tax_rag_project directory."
    Write-Host "Current directory: $(Get-Location)"
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

# Start Qdrant
Write-Host "Starting Qdrant vector database..." -ForegroundColor Cyan

# Try 'docker compose' (newer) first, then 'docker-compose' (older)
try {
    docker compose up -d 2>$null
    if ($LASTEXITCODE -ne 0) {
        docker-compose up -d
    }
} catch {
    Write-Host "ERROR: Failed to start Qdrant!" -ForegroundColor Red
    Write-Host $_.Exception.Message
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Write-Host "Waiting for Qdrant to initialize (10 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host ""
Write-Host "Checking Qdrant health..." -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri "http://localhost:6333/health" -UseBasicParsing -ErrorAction Stop
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host "Qdrant is running!" -ForegroundColor Green
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Dashboard: http://localhost:6333/dashboard" -ForegroundColor Cyan
    Write-Host "API Port: 6333"
    Write-Host "gRPC Port: 6334"
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "  1. Verify OPENAI_API_KEY is set in .env or .env.local"
    Write-Host "  2. Run: .\scripts\test_qdrant_connection.ps1"
    Write-Host ""
} catch {
    Write-Host ""
    Write-Host "WARNING: Qdrant health check failed!" -ForegroundColor Yellow
    Write-Host "It might need more time to start. Wait a few more seconds."
    Write-Host ""
    Write-Host "Check status with:" -ForegroundColor Cyan
    Write-Host "  docker ps"
    Write-Host "  docker compose logs qdrant"
    Write-Host ""
}

Write-Host "Container status:" -ForegroundColor Cyan
docker compose ps 2>$null
if ($LASTEXITCODE -ne 0) {
    docker-compose ps
}

Write-Host ""
Read-Host "Press Enter to continue"
