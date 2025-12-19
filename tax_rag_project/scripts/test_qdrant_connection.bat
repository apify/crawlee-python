@echo off
REM ============================================================================
REM Test Qdrant Cloud connection with OpenAI embeddings
REM Usage: scripts\test_qdrant_connection.bat
REM ============================================================================

cd /d "%~dp0\.."

echo ============================================================
echo QDRANT CLOUD CONNECTION TEST
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
        echo WARNING: No .env file found!
        echo.
        echo You need to set the following environment variables:
        echo   - QDRANT_URL (from https://cloud.qdrant.io)
        echo   - QDRANT_API_KEY (from https://cloud.qdrant.io)
        echo   - OPENAI_API_KEY (from https://platform.openai.com/api-keys)
        echo.
        echo Copy .env.example to .env and add your credentials
        echo.
        pause
        exit /b 1
    )
)

echo Using Qdrant Cloud (no Docker required)
echo.

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run the test
echo Running Qdrant connection test...
echo.
python src\tax_rag_scraper\test_qdrant_connection.py

echo.
echo ============================================================
echo Test complete!
echo ============================================================
echo.

REM Keep window open to see results
pause
