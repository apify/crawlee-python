@echo off
REM ============================================================================
REM Development Environment Setup Script
REM Run this ONCE to set up your Python virtual environment and dependencies
REM Usage: scripts\setup.bat
REM ============================================================================

cd /d "%~dp0\.."

echo.
echo ========================================
echo Tax RAG Project - Environment Setup
echo ========================================
echo.

REM Check if Python is available
py --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.11+ from https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/5] Python found:
py --version
echo.

REM Check if virtual environment already exists
if exist "venv\Scripts\activate.bat" (
    echo [2/5] Virtual environment already exists
    echo.
) else (
    echo [2/5] Creating virtual environment...
    py -m venv venv
    if %errorlevel% neq 0 (
        echo ERROR: Failed to create virtual environment
        pause
        exit /b 1
    )
    echo Virtual environment created successfully
    echo.
)

REM Activate virtual environment
echo [3/5] Activating virtual environment...
call venv\Scripts\activate.bat
echo.

REM Upgrade pip
echo [4/5] Upgrading pip...
python -m pip install --upgrade pip
echo.

REM Install dependencies
echo [5/5] Installing dependencies...
echo.
echo Installing crawlee and dependencies...
pip install "crawlee[beautifulsoup,httpx]>=1.2.0"
echo.
echo Installing other requirements...
pip install pydantic>=2.11.0 pydantic-settings>=2.12.0 httpx>=0.27.0
echo.
echo Installing development dependencies...
pip install pytest>=8.0.0 pytest-asyncio>=0.21.0 pytest-cov>=4.0.0
echo.

REM Install the package in editable mode
echo Installing tax_rag_scraper in editable mode...
pip install -e .
echo.

echo ========================================
echo Setup Complete!
echo ========================================
echo.
echo Your development environment is ready.
echo.
echo To run tests, use:
echo   scripts\test.bat
echo.
echo To activate the environment manually:
echo   scripts\activate.bat
echo.
pause
