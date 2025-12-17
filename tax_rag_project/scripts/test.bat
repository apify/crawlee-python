@echo off
REM ============================================================================
REM Run tests with the virtual environment
REM Usage: scripts\test.bat [optional-test-file]
REM Examples:
REM   scripts\test.bat                                    (runs default test)
REM   scripts\test.bat src\tax_rag_scraper\my_test.py    (runs specific test)
REM ============================================================================

cd /d "%~dp0\.."

REM Check if virtual environment exists
if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found!
    echo.
    echo Please run scripts\setup.bat first to set up your environment.
    echo.
    pause
    exit /b 1
)

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Check which test to run (default to deep crawling test)
if "%1"=="" (
    echo Running deep crawling test...
    python src\tax_rag_scraper\test_deep_crawling.py
) else (
    echo Running: %*
    python %*
)

REM Keep window open to see results
pause
