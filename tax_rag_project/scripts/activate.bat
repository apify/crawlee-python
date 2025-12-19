@echo off
REM ============================================================================
REM Activate virtual environment for interactive use
REM Usage: scripts\activate.bat
REM ============================================================================

cd /d "%~dp0\.."

if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found!
    echo Please run scripts\setup.bat first.
    pause
    exit /b 1
)

echo Activating virtual environment...
call venv\Scripts\activate.bat
echo.
echo Virtual environment activated!
echo Working directory: %CD%
echo.
echo You can now run Python commands directly:
echo   python src\tax_rag_scraper\test_deep_crawling.py
echo   pytest tests/
echo   pip install new-package
echo.
echo Type 'deactivate' to exit the virtual environment.
echo.
cmd /k
