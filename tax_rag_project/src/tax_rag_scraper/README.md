# Tax RAG Scraper

**A production-ready web scraping application for Canadian Revenue Agency (CRA) tax documentation**

## Project Context

This application is part of a larger RAG (Retrieval-Augmented Generation) pipeline for answering questions about Canadian tax regulations. It lives within a fork of the [apify/crawlee-python](https://github.com/apify/crawlee-python) repository.

**Repository Structure:**
- **Root:** Forked Crawlee framework (preserved for updates and local development)
- **`src/crawlee/`:** Original Crawlee framework code (unchanged)
- **`src/tax_rag_scraper/`:** This application (our custom scraper)
- **`tax_rag_project/`:** Project documentation and configuration templates

The Crawlee framework is installed locally from the fork, while this scraper is a standalone application that uses it.

## Role & Objective

### Role
A specialized web crawler that collects Canadian tax documentation from official government sources (primarily CRA websites) and prepares it for vector database ingestion.

### Objective
Build a reliable, maintainable data pipeline that:
1. **Scrapes** Canadian tax forms, guides, bulletins, and technical interpretations
2. **Processes** HTML and PDF content into structured, clean text
3. **Chunks** documents semantically for optimal RAG retrieval
4. **Enriches** content with metadata (document type, tax year, source URL)
5. **Stores** processed data in Qdrant vector database for similarity search

### Target Data Sources
- CRA forms and publications
- Tax guides and bulletins
- Technical interpretations
- News releases related to tax policy
- Educational resources

## Current Stage: Stage 1 - Foundation

**Goal:** Establish basic infrastructure and validate the Crawlee integration

**What's Implemented:**
- ✅ Project structure with modular design
- ✅ Configuration management (pydantic-settings)
- ✅ Base data model (TaxDocument)
- ✅ Simple BeautifulSoup crawler
- ✅ Test script for CRA website

**What's Next (Stage 2+):**
- PDF parsing and text extraction
- Advanced metadata extraction
- Document chunking strategies
- Qdrant vector database integration
- Production deployment configuration

## Installation

### Prerequisites

- **Python 3.10+** (required by Crawlee)
- Git
- pip or uv package manager

### Quick Start

1. **Clone the repository:**
   ```bash
   git clone <your-fork-url>
   cd crawlee-python-taxrag
   ```

2. **Install Crawlee framework** (from local fork):
   ```bash
   pip install -e .
   ```

3. **Install scraper dependencies:**
   ```bash
   cd src/tax_rag_scraper
   pip install -r requirements.txt
   ```

4. **Verify installation:**
   ```bash
   python -c "import crawlee; print(crawlee.__version__)"
   ```

## Usage

### Running the Scraper

From the repository root:

```bash
# Set Python path to include src directory
export PYTHONPATH="${PWD}/src"  # Linux/Mac
set PYTHONPATH=%CD%\src         # Windows

# Run the crawler
python src/tax_rag_scraper/main.py
```

Or as a Python module:

```bash
cd src
python -m tax_rag_scraper.main
```

### Configuration

Create a `.env` file in your working directory or set environment variables:

```env
# Crawler settings
MAX_REQUESTS_PER_CRAWL=100
MAX_CONCURRENCY=5
REQUEST_TIMEOUT=30

# Storage
STORAGE_DIR=storage
```

See `config/settings.py` for all available options.

### Output

Scraped data is saved to `storage/datasets/default/` as JSON files:

```json
{
  "url": "https://www.canada.ca/en/revenue-agency/...",
  "title": "Forms and publications - Canada.ca",
  "content": "Extracted text content...",
  "document_type": null,
  "tax_year": null,
  "scraped_at": "2025-12-12T12:34:56.789Z",
  "metadata": {}
}
```

## Project Structure

```
src/tax_rag_scraper/
├── __init__.py              # Package initialization
├── main.py                  # Entry point
├── requirements.txt         # Dependencies
├── pyproject.toml          # Package config
├── README.md               # This file
├── crawlers/               # Crawler implementations
│   ├── __init__.py
│   └── base_crawler.py     # TaxDataCrawler
├── models/                 # Data models
│   ├── __init__.py
│   └── tax_document.py     # TaxDocument model
├── config/                 # Configuration
│   ├── __init__.py
│   └── settings.py         # Settings management
├── handlers/               # Request handlers (future)
│   └── __init__.py
├── storage/                # Storage utilities (future)
│   └── __init__.py
└── utils/                  # Utilities (future)
    └── __init__.py
```

## Architecture

### Data Flow

```
CRA Website
    ↓
BeautifulSoupCrawler (current) / PlaywrightCrawler (future)
    ↓
TaxDocument Model (validation)
    ↓
Crawlee Dataset Storage (JSON)
    ↓
[Future: Document Processor → Embedding Generator → Qdrant]
    ↓
RAG Chatbot
```

### Key Components

1. **TaxDataCrawler** (`crawlers/base_crawler.py`)
   - Wraps Crawlee's BeautifulSoupCrawler
   - Handles configuration and setup
   - Defines request handlers
   - Manages data extraction and storage

2. **TaxDocument** (`models/tax_document.py`)
   - Pydantic v2 model for validation
   - Ensures data quality and type safety
   - Provides serialization to JSON

3. **Settings** (`config/settings.py`)
   - Environment-based configuration
   - Type-safe settings management
   - Supports .env files

## Development

### Adding New Crawlers

Create a new crawler in `crawlers/`:

```python
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler

class PDFCrawler(TaxDataCrawler):
    # Custom implementation
    pass
```

### Adding Custom Models

Create new models in `models/`:

```python
from pydantic import BaseModel

class TaxForm(BaseModel):
    form_number: str
    title: str
    # ...
```

### Environment-Specific Configuration

Use different .env files:
- `.env` - Default configuration
- `.env.local` - Local development overrides
- `.env.production` - Production settings

## Deployment

### Cloud-First Architecture

This project uses cloud-based services for production deployment:

1. **Set up Qdrant Cloud**
   - Visit https://cloud.qdrant.io
   - Create a cluster (free tier: 1GB storage)
   - Copy cluster URL and API key

2. **Configure credentials**
   - Set `QDRANT_URL` environment variable
   - Set `QDRANT_API_KEY` environment variable
   - Set `OPENAI_API_KEY` environment variable

3. **Deploy via GitHub Actions** (Stage 6)
   - Automated deployment pipeline
   - Scheduled crawling
   - No server maintenance required

See `tax_rag_project/` directory for detailed deployment guides.

## Staged Implementation

### ✅ Stage 1: Foundation (Current)
- Basic crawler infrastructure
- Simple text extraction
- Local JSON storage

### 🔄 Stage 2: Enhanced Processing (Next)
- PDF parsing (PyPDF2/pdfplumber)
- Metadata extraction (document type, tax year)
- Text cleaning and normalization
- Semantic chunking

### ✅ Stage 3: Production Hardening
- Comprehensive error handling
- Rate limiting and politeness
- Monitoring and alerting
- Security best practices

### ✅ Stage 4: Multi-Site Support
- Deep crawling with link extraction
- Site-specific handlers
- Advanced routing logic

### ✅ Stage 5: Qdrant Cloud Integration
- Qdrant Cloud setup (cloud-hosted vector database)
- OpenAI embedding generation
- Batch upload pipeline
- Vector search capabilities

### 🔄 Stage 5.5: Cloud Architecture (Current)
- Remove Docker dependencies
- Cloud-only configuration
- URL-based Qdrant connection
- Production-ready setup

### 📋 Stage 6: GitHub Actions Deployment
- Automated CI/CD pipeline
- Scheduled crawling
- Environment management
- Monitoring and alerts

## Troubleshooting

### ModuleNotFoundError: tax_rag_scraper

Add `src` to Python path:
```bash
export PYTHONPATH="${PWD}/src"
```

### Crawlee Import Error

Install Crawlee from repository root:
```bash
pip install -e .
```

### BeautifulSoup Not Found

Install with the beautifulsoup extra:
```bash
pip install "crawlee[beautifulsoup]"
```

## Contributing

This is a personal project within a Crawlee fork. For Crawlee framework issues, see the [upstream repository](https://github.com/apify/crawlee-python).

## License

- **Crawlee Framework:** Apache License 2.0 (see main repository LICENSE)
- **This Application:** TODO - Define license for custom code

## Links

- [Crawlee Documentation](https://crawlee.dev/python/)
- [Project Documentation](../../tax_rag_project/README.md)
- [Development Notes](../../tax_rag_project/claude.md)
- [Project Changelog](../../tax_rag_project/CHANGELOG.md)
