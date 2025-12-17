#!/bin/bash
# ============================================================================
# Development Environment Setup Script (Git Bash / Linux / Mac)
# Run this ONCE to set up your Python virtual environment and dependencies
# Usage: ./scripts/setup.sh
# ============================================================================

cd "$(dirname "$0")/.."

echo
echo "========================================"
echo "Tax RAG Project - Environment Setup"
echo "========================================"
echo

# Check if Python is available
if ! py --version &> /dev/null; then
    echo "ERROR: Python is not installed or not in PATH"
    echo "Please install Python 3.11+ from https://www.python.org/downloads/"
    read -p "Press Enter to continue..."
    exit 1
fi

echo "[1/5] Python found:"
py --version
echo

# Check if virtual environment already exists
if [ -f "venv/Scripts/activate" ] || [ -f "venv/bin/activate" ]; then
    echo "[2/5] Virtual environment already exists"
    echo
else
    echo "[2/5] Creating virtual environment..."
    py -m venv venv
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to create virtual environment"
        read -p "Press Enter to continue..."
        exit 1
    fi
    echo "Virtual environment created successfully"
    echo
fi

# Activate virtual environment (works for both Windows Git Bash and Linux/Mac)
echo "[3/5] Activating virtual environment..."
if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi
echo

# Upgrade pip
echo "[4/5] Upgrading pip..."
python -m pip install --upgrade pip
echo

# Install dependencies
echo "[5/5] Installing dependencies..."
echo
echo "Installing crawlee and dependencies..."
pip install "crawlee[beautifulsoup,httpx]>=1.2.0"
echo
echo "Installing other requirements..."
pip install pydantic>=2.11.0 pydantic-settings>=2.12.0 httpx>=0.27.0
echo
echo "Installing development dependencies..."
pip install pytest>=8.0.0 pytest-asyncio>=0.21.0 pytest-cov>=4.0.0
echo

# Install the package in editable mode
echo "Installing tax_rag_scraper in editable mode..."
pip install -e .
echo

echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo
echo "Your development environment is ready."
echo
echo "To run tests, use:"
echo "  ./scripts/test.sh"
echo
echo "To activate the environment manually:"
echo "  source venv/Scripts/activate  (Windows Git Bash)"
echo "  source venv/bin/activate      (Linux/Mac)"
echo
read -p "Press Enter to continue..."
