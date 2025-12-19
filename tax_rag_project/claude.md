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

### Why Qdrant?

- Self-hostable (important for VPS deployment)
- Excellent performance for semantic search
- Docker-friendly deployment
- Rich filtering capabilities (by document type, date, etc.)

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
2. Install Crawlee: `pip install -e .` from repo root
3. Use `.env.local` for local configuration
4. Run Qdrant locally via Docker: `docker-compose up`

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
│   │   ├── test_deep_crawling.py   # ✓ Deep crawling tests
│   │   ├── README.md               # ✓ Scraper documentation
│   │   ├── pyproject.toml          # ✓ Scraper config
│   │   └── requirements.txt        # ✓ Scraper dependencies
│   └── tax_scraper/                # Alternative scraper (WIP)
│       ├── crawlers/               # Empty placeholder
│       ├── handlers/               # Empty placeholder
│       └── utils/                  # Empty placeholder
├── tests/                          # ✓ Test suite
│   ├── __init__.py
│   ├── test_error_handling.py      # ✓ Error handling tests
│   └── test_rate_limiting.py       # ✓ Rate limiting tests
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
├── venv/                           # ✓ Virtual environment (local)
├── .env.example                    # ✓ Environment template
├── .env.local                      # ✓ Local settings
├── .gitignore                      # ✓ Git ignore rules
├── requirements.txt                # ✓ Dependencies
├── pyproject.toml                  # ✓ Project configuration
├── CHANGELOG.md                    # ✓ Version history
├── claude.md                       # ✓ This file
└── README.md                       # ✓ Main readme
```

## Commands Reference

### Crawlee Framework

Install Crawlee from fork (from repository root):
```bash
pip install -e .
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

Start Qdrant (TODO):
```bash
docker-compose up -d
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

Qdrant health check:
```bash
curl http://localhost:6333/health
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

- VPS requirements: 2+ CPU cores, 4+ GB RAM
- Qdrant memory usage scales with collection size
- Monitor disk space for storage directory
- Use systemd or supervisor for process management

## Current Status

**Last Updated**: 2025-12-17

**Completed Stages**:
- ✓ Stage 1: Basic Crawler
- ✓ Stage 2: Error Handling & Retry Logic
- ✓ Stage 3: Rate Limiting & Security

## Troubleshooting

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
