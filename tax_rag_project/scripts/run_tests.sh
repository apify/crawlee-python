#!/bin/bash
# Shell script to run tax_rag_scraper tests from tax_rag_project

echo "Activating virtual environment..."
source ../.venv/bin/activate

echo ""
echo "Running all tests..."
python run_all_tests.py
