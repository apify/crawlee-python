@echo off
REM ============================================================================
REM Setup and start Qdrant vector database
REM Usage: scripts\setup_qdrant.bat
REM ============================================================================

cd /d "%~dp0\.."

echo ============================================================
echo QDRANT SETUP
echo ============================================================
echo.

REM Check if Docker is installed
where docker >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not installed or not in PATH!
    echo.
    echo Please install Docker Desktop from:
    echo   https://www.docker.com/products/docker-desktop
    echo.
    pause
    exit /b 1
)

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running!
    echo.
    echo Please start Docker Desktop and try again.
    echo.
    pause
    exit /b 1
)

echo Docker is running!
echo.

REM Check if docker-compose.yml exists
if not exist "docker-compose.yml" (
    echo ERROR: docker-compose.yml not found!
    echo.
    echo Make sure you're in the tax_rag_project directory.
    echo.
    pause
    exit /b 1
)

REM Start Qdrant
echo Starting Qdrant vector database...
docker-compose up -d

echo.
echo Waiting for Qdrant to initialize (10 seconds)...
timeout /t 10 /nobreak >nul

echo.
echo Checking Qdrant health...
curl -s http://localhost:6333/health >nul 2>&1
if errorlevel 1 (
    echo.
    echo WARNING: Qdrant health check failed!
    echo It might need more time to start. Wait a few more seconds.
    echo.
    echo Check status with:
    echo   docker-compose ps
    echo   docker-compose logs qdrant
    echo.
) else (
    echo.
    echo ============================================================
    echo Qdrant is running!
    echo ============================================================
    echo.
    echo Dashboard: http://localhost:6333/dashboard
    echo API Port: 6333
    echo gRPC Port: 6334
    echo.
    echo Next steps:
    echo   1. Set OPENAI_API_KEY in .env file
    echo   2. Run: scripts\test_qdrant_connection.bat
    echo.
)

echo Check container status:
docker-compose ps

echo.
pause
