#!/bin/bash
# ============================================================================
# Run tests with the virtual environment (Git Bash / Linux / Mac)
# Usage: ./scripts/test.sh [optional-test-file]
# Examples:
#   ./scripts/test.sh                                    (runs default test)
#   ./scripts/test.sh src/tax_rag_scraper/my_test.py    (runs specific test)
# ============================================================================

cd "$(dirname "$0")/.."

# Check if virtual environment exists
if [ ! -f "venv/Scripts/activate" ] && [ ! -f "venv/bin/activate" ]; then
    echo "ERROR: Virtual environment not found!"
    echo
    echo "Please run ./scripts/setup.sh first to set up your environment."
    echo
    read -p "Press Enter to continue..."
    exit 1
fi

# Activate virtual environment
if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Check which test to run (default to deep crawling test)
if [ -z "$1" ]; then
    echo "Running deep crawling test..."
    python src/tax_rag_scraper/test_deep_crawling.py
else
    echo "Running: $@"
    python "$@"
fi

# Keep terminal open if double-clicked (optional)
echo
read -p "Press Enter to continue..."
