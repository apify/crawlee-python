#!/bin/bash
# ============================================================================
# Test full crawler integration with Qdrant
# Usage: ./scripts/test_qdrant_integration.sh
# ============================================================================

cd "$(dirname "$0")/.."

echo "============================================================"
echo "QDRANT CLOUD INTEGRATION TEST"
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
    echo "WARNING: No .env or .env.local file found!"
    echo
    echo "You need to set OPENAI_API_KEY for this test to work."
    echo
    echo "Options:"
    echo "  1. Copy .env.example to .env and add your API key"
    echo "  2. Set environment variable: export OPENAI_API_KEY=sk-proj-..."
    echo
    echo "Get your API key from: https://platform.openai.com/api-keys"
    echo
    read -p "Press Enter to continue..."
    exit 1
fi

# Check if Qdrant is running
echo "Checking if Qdrant is running..."
if ! curl -s http://Qdrant Cloud/health > /dev/null 2>&1; then
    echo
    echo "WARNING: Qdrant doesn't appear to be running!"
    echo
    echo "Please start Qdrant first:"
    echo "  Qdrant Cloud setup at https://cloud.qdrant.io"
    echo
    echo "Then wait 10-15 seconds for initialization."
    echo
    read -p "Press Enter to continue..."
    exit 1
fi
echo "Qdrant is running!"
echo

# Activate virtual environment
if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

# Run the test
echo "Running Qdrant integration test..."
echo "This will crawl real CRA pages and generate embeddings."
echo "Cost: approximately \$0.001-0.01 in OpenAI API usage."
echo
python src/tax_rag_scraper/test_qdrant_integration.py

echo
echo "============================================================"
echo "Test complete!"
echo "============================================================"
echo
echo "Next steps:"
echo "  1. Check Qdrant dashboard: http://Qdrant Cloud/dashboard"
echo "  2. View Docker logs: docker-compose logs qdrant"
echo

# Keep terminal open
read -p "Press Enter to continue..."
