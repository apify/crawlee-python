#!/bin/bash
# ============================================================================
# Setup and start Qdrant vector database
# Usage: ./scripts/setup_qdrant.sh
# ============================================================================

cd "$(dirname "$0")/.."

echo "============================================================"
echo "QDRANT SETUP"
echo "============================================================"
echo

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed or not in PATH!"
    echo
    echo "Please install Docker from:"
    echo "  https://www.docker.com/products/docker-desktop"
    echo
    read -p "Press Enter to continue..."
    exit 1
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running!"
    echo
    echo "Please start Docker and try again."
    echo
    read -p "Press Enter to continue..."
    exit 1
fi

echo "Docker is running!"
echo

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "ERROR: docker-compose.yml not found!"
    echo
    echo "Make sure you're in the tax_rag_project directory."
    echo
    read -p "Press Enter to continue..."
    exit 1
fi

# Start Qdrant
echo "Starting Qdrant vector database..."
docker-compose up -d

echo
echo "Waiting for Qdrant to initialize (10 seconds)..."
sleep 10

echo
echo "Checking Qdrant health..."
if ! curl -s http://localhost:6333/health > /dev/null 2>&1; then
    echo
    echo "WARNING: Qdrant health check failed!"
    echo "It might need more time to start. Wait a few more seconds."
    echo
    echo "Check status with:"
    echo "  docker-compose ps"
    echo "  docker-compose logs qdrant"
    echo
else
    echo
    echo "============================================================"
    echo "Qdrant is running!"
    echo "============================================================"
    echo
    echo "Dashboard: http://localhost:6333/dashboard"
    echo "API Port: 6333"
    echo "gRPC Port: 6334"
    echo
    echo "Next steps:"
    echo "  1. Set OPENAI_API_KEY in .env file"
    echo "  2. Run: ./scripts/test_qdrant_connection.sh"
    echo
fi

echo "Check container status:"
docker-compose ps

echo
read -p "Press Enter to continue..."
