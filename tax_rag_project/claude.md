# Claude Code Development Notes - Tax RAG Project

## Project Overview

This document provides context for Claude Code when working on the Canadian Tax Documentation RAG pipeline.

### High-Level Architecture

```
CRA Website → Crawlee Crawler → Document Processor → Embedding Generator → Qdrant Vector DB
                           ↓
                     RAG Chatbot
```

### Core Components

1. **Crawler** ✓
   - `base_crawler.py` - BeautifulSoupCrawler with deep crawling support
     - Built-in rate limiting and retry logic
     - Robots.txt respect
     - User-agent rotation for polite crawling
     - Statistics tracking
     - Target: CRA tax guides, forms, bulletins
   - `site_router.py` - Site-specific routing logic for different domains

2. **Handlers** ✓
   - `base_handler.py` - Request handler base class with error management
   - `cra_handler.py` - CRA-specific handler with custom extraction logic
   - Data extraction from HTML
   - Extensible for different page types

3. **Utilities** ✓
   - `link_extractor.py` - Smart link extraction and filtering
   - `robots.py` - Robots.txt checker
   - `stats_tracker.py` - Statistics tracking
   - `user_agents.py` - User-agent rotation

4. **Processor** (`processors/document_processor.py`) - Planned
   - Text extraction from HTML and PDF
   - Chunking strategy (semantic chunking preferred)
   - Metadata enrichment (document type, date, source URL)

5. **Embeddings** (`processors/embedding_generator.py`) - Planned
   - Model: sentence-transformers (all-MiniLM-L6-v2 or similar)
   - Batch processing for efficiency
   - Dimension: 384 or 768 depending on model

6. **Storage** (Qdrant) - Planned
   - Collection: `cra_tax_documents`
   - Payload: text chunks + metadata
   - Vector dimension: matches embedding model

## Key Design Decisions

### Why Crawlee?

- Production-ready with built-in retry logic
- Respectful crawling (rate limiting, robots.txt)
- Persistent queue for long-running crawls
- Already forked from apify/crawlee-python

### Why Qdrant Cloud?

- Cloud-first architecture (no Docker setup required)
- Free tier with 1GB storage
- Excellent performance for semantic search
- Production-ready from day one
- Rich filtering capabilities (by document type, date, etc.)
- Automatic scaling and high availability

### Why sentence-transformers?

- Strong semantic understanding
- Pre-trained models available
- CPU-friendly for small-to-medium scale
- Easy to swap models if needed

### Chunking Strategy

**Planned Approach**: Semantic chunking with overlap
- Chunk size: ~500-1000 tokens
- Overlap: 100-200 tokens
- Preserve document structure (headers, sections)
- Maintain citation metadata per chunk

## Staged Implementation

### Stage 1: Basic Crawler ✓ (COMPLETED)

**Goal**: Get data flowing end-to-end

- [x] Project structure
- [x] BeautifulSoupCrawler implementation (base_crawler.py)
- [x] Request handler with error handling
- [x] Rate limiting and security features
- [x] Robots.txt respect
- [x] User-agent rotation
- [x] Statistics tracking
- [x] Test suite (error handling, rate limiting)
- [x] Documentation and runner scripts

**Success Criteria**: ✓ Can crawl CRA pages with proper error handling and rate limiting.

### Continued Development
Implementation stages will be provided and completed periodically with Claude Code. The user will control the development process and control Claude Code in development. We will not speculate on continued development but keep the overall project objectives in mind when assisting in development.

## Development Workflow

### Local Development

1. Work in `tax_rag_project/` directory
2. Install from repo root: `pip install -e ".[tax-rag]"` (includes Crawlee + all tax-rag dependencies)
3. For development: `pip install -e ".[tax-rag-dev]"` (adds pytest and testing tools)
4. Use `.env` for configuration (copy from `.env.example`)
5. Run Qdrant via Qdrant Cloud (cloud-first architecture, no local Docker required)

### Testing Strategy

- Unit tests for processors and utilities
- Integration tests for crawler → storage flow
- Manual verification of chunk quality
- Search relevance testing with sample queries

### Code Organization

```
tax_rag_project/
├── src/
│   ├── tax_rag_scraper/            # Main scraper package
│   │   ├── crawlers/
│   │   │   ├── __init__.py
│   │   │   ├── base_crawler.py     # ✓ Base crawler with deep crawling
│   │   │   └── site_router.py      # ✓ Site-specific routing logic
│   │   ├── handlers/
│   │   │   ├── __init__.py
│   │   │   ├── base_handler.py     # ✓ Request handler base class
│   │   │   └── cra_handler.py      # ✓ CRA-specific handler
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   └── tax_document.py     # ✓ Data models
│   │   ├── config/
│   │   │   ├── __init__.py
│   │   │   └── settings.py         # ✓ Centralized settings
│   │   ├── utils/
│   │   │   ├── __init__.py
│   │   │   ├── link_extractor.py   # ✓ Link extraction utilities
│   │   │   ├── stats_tracker.py    # ✓ Statistics tracking
│   │   │   ├── user_agents.py      # ✓ User-agent rotation
│   │   │   └── robots.py           # ✓ Robots.txt checker
│   │   ├── storage/
│   │   │   └── __init__.py
│   │   ├── main.py                 # ✓ Main entry point
│   │   └── test_deep_crawling.py   # ✓ Deep crawling 
tests
│   └── test_rate_limiting.py       # ✓ Rate limiting 
tests
├── scripts/                        # ✓ Runner and setup scripts
│   ├── run_crawler.py              # ✓ Run base crawler
│   ├── run_all_tests.py            # ✓ Run all tests
│   ├── setup.bat                   # ✓ Windows setup automation
│   ├── setup.sh                    # ✓ Linux/Mac setup automation
│   ├── test.bat                    # ✓ Windows test runner
│   ├── test.sh                     # ✓ Linux/Mac test runner
│   ├── activate.bat                # ✓ Quick venv activation
│   ├── run_tests.bat               # ✓ Windows batch file
│   ├── run_tests.sh                # ✓ Linux/Mac shell script
├── docs/                           # Documentation directory
├── processors/                     # (Planned) Document processing
├── config/                         # (Planned) Additional configs
├── crawlers/                       # (Planned) Crawler configs
├── storage/                        # Runtime storage (not committed)
├── tests/                          # ✓ Test suite
│   ├── __init__.py
│   ├── test_error_handling.py      # ✓ Error handling 
├── venv/                           # ✓ Virtual environment (local)
├── .env.example                    # ✓ Environment template
├── .env.local                      # ✓ Local settings (optional override)
├── .gitignore                      # ✓ Git ignore rules
├── CHANGELOG.md                    # ✓ Version history
├── claude.md                       # ✓ This file
└── README.md                       # ✓ Main readme

Note: Project dependencies are managed in the root pyproject.toml file (../../pyproject.toml).
Use `pip install -e ".[tax-rag]"` from repository root to install.
```

## Commands Reference

### Installation

Install from repository root (single source of truth):
```bash
# Production installation (Crawlee + all tax-rag dependencies)
pip install -e ".[tax-rag]"

# Development installation (adds pytest and testing tools)
pip install -e ".[tax-rag-dev]"

# Or both at once
pip install -e ".[tax-rag,tax-rag-dev]"
```

### Quick Setup Scripts

**Windows - Automated Setup:**
```bash
cd tax_rag_project/scripts
./setup.bat        # Complete environment setup
./test.bat         # Run tests with venv activation
./activate.bat     # Quick venv activation
```

**Linux/Mac - Automated Setup:**
```bash
cd tax_rag_project/scripts
./setup.sh         # Complete environment setup
./test.sh          # Run tests with venv activation
```

### Project Commands

Run base crawler:
```bash
cd tax_rag_project/scripts
../../.venv/Scripts/python.exe run_crawler.py     # Windows
../../.venv/bin/python run_crawler.py             # Linux/Mac
```

Run all tests:
```bash
cd tax_rag_project/scripts
../../.venv/Scripts/python.exe run_all_tests.py   # Windows
./run_tests.bat                                   # Windows (batch)
./test.bat                                        # Windows (with venv)
```

Run individual tests:
```bash
cd tax_rag_project/scripts
../../.venv/Scripts/python.exe ../tests/test_error_handling.py
../../.venv/Scripts/python.exe ../tests/test_rate_limiting.py
```

Run deep crawling tests:
```bash
cd tax_rag_project/src/tax_rag_scraper
../../.venv/Scripts/python.exe test_deep_crawling.py    # Windows
../../.venv/bin/python test_deep_crawling.py            # Linux/Mac
```

Access Qdrant Cloud:
```bash
# Set up credentials in .env file
# QDRANT_URL=https://your-cluster.cloud.qdrant.io
# QDRANT_API_KEY=your-api-key
# OPENAI_API_KEY=sk-proj-...

# Test connection
python src/tax_rag_scraper/test_qdrant_connection.py
```

### Useful Development Commands

Check Crawlee installation:
```bash
python -c 'import crawlee; print(crawlee.__version__)'
```

Monitor storage directory:
```bash
ls -lah tax_rag_project/storage/
```

Qdrant Cloud health check:
```bash
# Visit your cluster dashboard at https://cloud.qdrant.io
# Or check via API
curl https://your-cluster.cloud.qdrant.io/collections \
  -H "api-key: your-api-key"
```

## Important Notes

### Respectful Crawling

- **ALWAYS** check robots.txt (example `https://www.canada.ca/robots.txt`)
- Use conservative rate limits (1-2 req/sec max)
- Set proper User-Agent identifying the bot
- Implement politeness delays between requests

### Data Privacy

- CRA content is public, but verify licensing
- Don't store personal information
- Consider GDPR/privacy implications for deployment region

### Vector Search Optimization

- Index only meaningful content (skip navigation, footers)
- Use metadata filters for better precision
- Consider hybrid search (vector + keyword) for production

### Deployment Considerations

- GitHub Actions for automated scheduling (daily and weekly crawls)
- Qdrant Cloud handles vector storage (no VPS management needed)
- Monitor Qdrant Cloud storage usage (free tier: 1GB)
- Monitor local disk space for crawler storage directory
- GitHub Actions artifacts for crawl logs and statistics

## Current Status

**Last Updated**: 2025-12-22

**Completed Stages**:
- ✓ Stage 1: Basic Crawler
- ✓ Stage 2: Error Handling & Retry Logic
- ✓ Stage 3: Rate Limiting & Security

## Troubleshooting

### Dependency Management

**Single Source of Truth**: All dependencies are managed in the root `pyproject.toml` file:
- Core Crawlee dependencies: `[project.dependencies]`
- Tax RAG dependencies: `[project.optional-dependencies.tax-rag]`
- Development dependencies: `[project.optional-dependencies.tax-rag-dev]`

**Installation from root**:
```bash
# Always run from repository root
cd crawlee-python-taxrag
pip install -e ".[tax-rag]"          # Production
pip install -e ".[tax-rag-dev]"      # Development tools
```

### Project Organization

**Current Structure**: All tax scraper code is now in `tax_rag_project/` directory:
- Source code: `tax_rag_project/src/tax_rag_scraper/`
- Tests: `tax_rag_project/tests/`
- Scripts: `tax_rag_project/scripts/`
- Docs: `tax_rag_project/docs/`

The main repository `src/` folder only contains the crawlee library source.

### Running Tests and Scripts

**Always use the virtual environment Python**:

```bash
# From tax_rag_project/scripts/
../../.venv/Scripts/python.exe run_crawler.py      # Windows
../../.venv/bin/python run_crawler.py              # Linux/Mac

# Or use batch files
./run_tests.bat                                    # Windows
./run_tests.sh                                     # Linux/Mac
```

**Don't use**:
- Global `python` command (may not have dependencies)
- `pytest` directly (may not be in PATH)

**Module Import Paths**:
- Crawlee crawlers: `from crawlee.crawlers import BeautifulSoupCrawler`
- NOT: `from crawlee import BeautifulSoupCrawler` (will fail)
- Tax scraper: `from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler`

### Python Path Issues

The runner scripts in `tax_rag_project/scripts/` automatically handle Python path setup. If you need to import manually:

```python
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
```

### Quick Verification

```bash
# Check Python installation
.venv/Scripts/python.exe --version

# Check Crawlee installation (from repo root)
.venv/Scripts/python.exe -c "import crawlee; print(crawlee.__version__)"

# Test the scraper works
cd tax_rag_project/scripts
../../.venv/Scripts/python.exe run_crawler.py
```

## Resources

- [Crawlee Python Docs](https://crawlee.dev/python/)
- [Qdrant Quick Start](https://qdrant.tech/documentation/quick-start/)
- [sentence-transformers Models](https://www.sbert.net/docs/pretrained_models.html)
- [CRA Website](https://www.canada.ca/en/revenue-agency.html)
