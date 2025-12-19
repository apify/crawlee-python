#!/bin/bash
# ============================================================================
# Test Qdrant Cloud connection with OpenAI embeddings
# Usage: ./scripts/test_qdrant_connection.sh
# ============================================================================

cd "$(dirname "$0")/.."

echo "============================================================"
echo "QDRANT CLOUD CONNECTION TEST"
echo "============================================================"
echo

# Check if virtual environment exists
if [ ! -f "venv/Scripts/activate" ] && [ ! -f "venv/bin/activate" ]; then
    echo "ERROR: Virtual environment not found!"
    echo
    echo "Please run ./scripts/setup.sh first to set up your environment."
    echo
    read -p "Press Enter to continue..."
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ] && [ ! -f ".env.local" ]; then
    echo "WARNING: No .env file found!"
    echo
    echo "You need to set the following environment variables:"
    echo "  - QDRANT_URL (from https://cloud.qdrant.io)"
    echo "  - QDRANT_API_KEY (from https://cloud.qdrant.io)"
    echo "  - OPENAI_API_KEY (from https://platform.openai.com/api-keys)"
    echo
    echo "Copy .env.example to .env and add your credentials"
    echo
    read -p "Press Enter to continue..."
    exit 1
fi

echo "Using Qdrant Cloud (no Docker required)"
echo

# Activate virtual environment
if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Run the test
echo "Running Qdrant connection test..."
echo
python src/tax_rag_scraper/test_qdrant_connection.py

echo
echo "============================================================"
echo "Test complete!"
echo "============================================================"
echo

# Keep terminal open
read -p "Press Enter to continue..."
