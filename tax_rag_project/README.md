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
├── src/
│   ├── tax_rag_scraper/        # Main scraper package
│   │   ├── crawlers/           # Crawler implementations
│   │   │   ├── base_crawler.py     # Base crawler with deep crawling
│   │   │   └── site_router.py      # Site-specific routing logic
│   │   ├── handlers/           # Request handlers
│   │   │   ├── base_handler.py     # Base request handler
│   │   │   └── cra_handler.py      # CRA-specific handler
│   │   ├── models/             # Data models
│   │   ├── config/             # Configuration
│   │   ├── utils/              # Utility modules
│   │   ├── storage/            # Storage utilities
│   │   ├── main.py             # Main entry point
│   │   └── test_deep_crawling.py   # Deep crawling tests
│   └── tax_scraper/            # Alternative scraper (WIP)
├── tests/                      # Test suite
│   ├── test_error_handling.py
│   └── test_rate_limiting.py
├── scripts/                    # Runner and setup scripts
├── docs/                       # Documentation (planned)
├── storage/                    # Local data storage (runtime)
├── processors/                 # Document processing (planned)
├── config/                     # Additional config (planned)
├── crawlers/                   # Crawler configs (planned)
├── venv/                       # Virtual environment (local)
├── .env.example                # Environment template
├── .env.local                  # Local overrides
├── .gitignore                  # Git ignore rules
├── requirements.txt            # Dependencies
├── pyproject.toml              # Project configuration
├── CHANGELOG.md                # Version history
├── claude.md                   # Development notes
└── README.md                   # This file
```

## Quick Start

### Manual Setup (All Platforms)

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

3. **Configure environment variables**
   ```bash
   cd tax_rag_project
   cp .env.example .env.local
   # Edit .env.local with your configuration
   ```

4. **Run the crawler**
   ```bash
   # Windows
   cd scripts
   ..\.venv\Scripts\python.exe run_crawler.py

   # Or use batch file
   run_tests.bat

   # Linux/Mac
   cd scripts
   ../​.venv/bin/python run_crawler.py
   ```

5. **Run all tests**
   ```bash
   cd scripts
   ..\.venv\Scripts\python.exe run_all_tests.py
   ```

### Super Simple Setup (Windows)

**First time only:**
```bash
# Double-click or run:
setup_dev_env.bat
```

**Every time you want to run tests:**
```bash
# Double-click or run:
run_test.bat
```

## Development Setup

### Environment Variables

See [.env.example](.env.example) for all configuration options including:
- Crawler settings (max requests, concurrency, rate limits)
- Qdrant connection details
- Storage paths
- Retry and timeout configurations

### Project Configuration

Configuration follows a staged implementation approach. This will not be specified in the README.md file here.

See [claude.md](claude.md) for detailed development notes and architecture decisions.

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
