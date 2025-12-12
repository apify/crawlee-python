# Canadian Tax Documentation RAG Pipeline

A production-ready web scraping pipeline for collecting Canadian Revenue Agency (CRA) tax documentation to power a Retrieval-Augmented Generation (RAG) chatbot.

## Purpose

This project crawls, processes, and stores Canadian tax documentation in a vector database to enable intelligent question-answering about Canadian tax regulations, forms, and guidelines. The data pipeline supports a RAG-based chatbot that provides accurate, citation-backed responses to tax-related queries.

## Tech Stack

- **Web Scraping**: [Crawlee for Python](https://crawlee.dev/python/) - Robust, production-grade crawler
- **Vector Database**: [Qdrant](https://qdrant.tech/) - High-performance vector search
- **Embeddings**: [sentence-transformers](https://www.sbert.net/) - State-of-the-art semantic embeddings
- **Containerization**: Docker & Docker Compose - Consistent deployment environment
- **Storage**: File-based persistence with structured metadata

## Project Structure

```
tax_rag_project/
├── crawlers/           # Web scraper implementations (TODO)
├── processors/         # Document processing pipeline (TODO)
├── config/            # Configuration files (TODO)
├── storage/           # Local data storage (created at runtime)
├── .env.example       # Environment variable template
├── .env.local         # Local development overrides (not committed)
├── CHANGELOG.md       # Project version history
└── README.md          # This file
```

## Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose (for Qdrant)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <your-fork-url>
   cd crawlee-python-taxrag
   ```

2. **Install Crawlee from local source**
   ```bash
   pip install -e .
   ```

3. **Install project dependencies**
   ```bash
   cd tax_rag_project
   pip install -r requirements.txt  # TODO: Create requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   cp .env.example .env.local
   # Edit .env.local with your configuration
   ```

5. **Start Qdrant vector database**
   ```bash
   docker-compose up -d  # TODO: Create docker-compose.yml
   ```

6. **Run the crawler** (TODO)
   ```bash
   python crawlers/cra_crawler.py
   ```

## Development Setup

### Environment Variables

See [.env.example](.env.example) for all configuration options including:
- Crawler settings (max requests, concurrency, rate limits)
- Qdrant connection details
- Storage paths
- Retry and timeout configurations

### Project Configuration

Configuration follows a staged implementation approach:

**Stage 1: Basic Crawler** (Current)
- Simple CRA website crawler
- File-based storage
- Basic document extraction

**Stage 2: Enhanced Processing** (Planned)
- PDF parsing and text extraction
- Metadata enrichment
- Chunking strategy implementation

**Stage 3: Vector Database** (Planned)
- Qdrant integration
- Embedding generation
- Vector indexing

**Stage 4: Production Hardening** (Planned)
- Error handling and retry logic
- Rate limiting and politeness delays
- Monitoring and logging
- Docker deployment configuration

See [claude.md](claude.md) for detailed development notes and architecture decisions.

## Deployment

### DigitalOcean VPS Deployment

This project is designed for self-hosted deployment on a DigitalOcean VPS:

1. **Provision VPS** with Ubuntu 22.04+
2. **Install dependencies**: Docker, Python 3.10+
3. **Clone repository** to VPS
4. **Configure production environment** (`.env.production`)
5. **Run via Docker Compose** for production reliability

Detailed deployment guide: TODO

## Contributing

See the main [Crawlee CHANGELOG](../CHANGELOG.md) for framework updates.
See [CHANGELOG.md](CHANGELOG.md) for project-specific changes.

## License

This project uses the Crawlee framework (Apache License 2.0).
Project-specific code: TODO - Define license

## Links

- [Crawlee Documentation](https://crawlee.dev/python/)
- [Qdrant Documentation](https://qdrant.tech/documentation/)
- [Project Changelog](CHANGELOG.md)
