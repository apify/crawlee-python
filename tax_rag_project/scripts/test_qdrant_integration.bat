@echo off
REM ============================================================================
REM Test full crawler integration with Qdrant
REM Usage: scripts\test_qdrant_integration.bat
REM ============================================================================

cd /d "%~dp0\.."

echo ============================================================
echo QDRANT CLOUD INTEGRATION TEST
echo ============================================================
echo.

REM Check if virtual environment exists
if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found!
    echo.
    echo Please run scripts\setup.bat first to set up your environment.
    echo.
    pause
    exit /b 1
)

REM Check if .env file exists
if not exist ".env" (
    if not exist ".env.local" (
        echo WARNING: No .env or .env.local file found!
        echo.
        echo You need to set OPENAI_API_KEY for this test to work.
        echo.
        echo Options:
        echo   1. Copy .env.example to .env and add your API key
        echo   2. Set environment variable: set OPENAI_API_KEY=sk-proj-...
        echo.
        echo Get your API key from: https://platform.openai.com/api-keys
        echo.
        pause
        exit /b 1
    )
)

REM Check if Qdrant is running
echo Checking if Qdrant is running...
curl -s http://Qdrant Cloud/health >nul 2>&1
if errorlevel 1 (
    echo.
    echo WARNING: Qdrant doesn't appear to be running!
    echo.
    echo Please start Qdrant first:
    echo   Qdrant Cloud setup at https://cloud.qdrant.io
    echo.
    echo Then wait 10-15 seconds for initialization.
    echo.
    pause
    exit /b 1
)
echo Qdrant is running!
echo.

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run the test
echo Running Qdrant integration test...
echo This will crawl real CRA pages and generate embeddings.
echo Cost: approximately $0.001-0.01 in OpenAI API usage.
echo.
python src\tax_rag_scraper\test_qdrant_integration.py

echo.
echo ============================================================
echo Test complete!
echo ============================================================
echo.
echo Next steps:
echo   1. Check Qdrant dashboard: http://Qdrant Cloud/dashboard
echo   2. View Docker logs: docker-compose logs qdrant
echo.

REM Keep window open to see results
pause
