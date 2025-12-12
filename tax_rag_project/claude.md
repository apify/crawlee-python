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

1. **Crawler** (`crawlers/cra_crawler.py`)
   - BeautifulSoupCrawler for HTML pages
   - PlaywrightCrawler for JavaScript-heavy content (if needed)
   - Target: CRA tax guides, forms, bulletins

2. **Processor** (`processors/document_processor.py`)
   - Text extraction from HTML and PDF
   - Chunking strategy (semantic chunking preferred)
   - Metadata enrichment (document type, date, source URL)

3. **Embeddings** (`processors/embedding_generator.py`)
   - Model: sentence-transformers (all-MiniLM-L6-v2 or similar)
   - Batch processing for efficiency
   - Dimension: 384 or 768 depending on model

4. **Storage** (Qdrant)
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

### Stage 1: Basic Crawler ✓ (Current)

**Goal**: Get data flowing end-to-end

- [x] Project structure
- [ ] Simple BeautifulSoupCrawler
- [ ] Crawl 5-10 CRA pages
- [ ] Extract text and save to JSON
- [ ] Basic logging

**Success Criteria**: Can crawl CRA pages and save raw content

### Stage 2: Enhanced Processing

**Goal**: Clean, structured data

- [ ] PDF parsing (PyPDF2 or pdfplumber)
- [ ] HTML text extraction (clean formatting)
- [ ] Metadata extraction (title, date, document type)
- [ ] Chunking implementation
- [ ] Data validation

**Success Criteria**: Structured, chunked documents with metadata

### Stage 3: Vector Database

**Goal**: Searchable embeddings

- [ ] Qdrant Docker setup
- [ ] Collection schema design
- [ ] Embedding generation
- [ ] Batch upload to Qdrant
- [ ] Search API testing

**Success Criteria**: Can query Qdrant and retrieve relevant chunks

### Stage 4: Production Hardening

**Goal**: Reliable, maintainable system

- [ ] Comprehensive error handling
- [ ] Rate limiting (respect CRA servers)
- [ ] Retry logic with exponential backoff
- [ ] Monitoring and alerts
- [ ] Docker Compose production config
- [ ] Deployment documentation

**Success Criteria**: Runs reliably on DigitalOcean VPS

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
├── crawlers/
│   ├── __init__.py
│   ├── cra_crawler.py      # Main crawler implementation
│   └── config.py           # Crawler-specific config
├── processors/
│   ├── __init__.py
│   ├── document_processor.py   # Text extraction and cleaning
│   ├── chunking.py            # Chunking logic
│   └── embedding_generator.py  # Embedding creation
├── config/
│   ├── __init__.py
│   ├── settings.py         # Centralized settings (from .env)
│   └── qdrant_schema.py    # Qdrant collection config
├── storage/                # Runtime storage (not committed)
└── tests/                  # Test suite (TODO)
```

## Commands Reference

### Crawlee Framework

Install Crawlee from fork:
```bash
pip install -e .
```

### Project Commands

Run crawler (TODO):
```bash
cd tax_rag_project
python crawlers/cra_crawler.py
```

Start Qdrant (TODO):
```bash
docker-compose up -d
```

Run tests (TODO):
```bash
pytest tests/
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

- **ALWAYS** check CRA's robots.txt: `https://www.canada.ca/robots.txt`
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

**Last Updated**: 2025-12-12

**Current Stage**: Stage 1 - Basic Crawler (Setup Phase)

**Next Steps**:
1. Implement basic CRA crawler
2. Test crawling 5-10 pages
3. Verify storage and logging
4. Move to Stage 2 (processing)

## Resources

- [Crawlee Python Docs](https://crawlee.dev/python/)
- [Qdrant Quick Start](https://qdrant.tech/documentation/quick-start/)
- [sentence-transformers Models](https://www.sbert.net/docs/pretrained_models.html)
- [CRA Website](https://www.canada.ca/en/revenue-agency.html)
