@echo off
REM Windows batch file to run tax_rag_scraper tests from tax_rag_project

echo Activating virtual environment...
call ..\.venv\Scripts\activate.bat

echo.
echo Running all tests...
python run_all_tests.py

pause
