# Canadian Tax Documentation RAG Pipeline

A production-ready web scraping pipeline for collecting Canadian Revenue Agency (CRA) tax documentation to power a Retrieval-Augmented Generation (RAG) chatbot.

## Purpose

This project crawls, processes, and stores Canadian tax documentation in a vector database to enable intelligent question-answering about Canadian tax regulations, forms, and guidelines. The data pipeline supports a RAG-based chatbot that provides accurate, citation-backed responses to tax-related queries.

## Tech Stack

- **Web Scraping**: [Crawlee for Python](https://crawlee.dev/python/) - Robust, production-grade crawler
- **Vector Database**: [Qdrant Cloud](https://cloud.qdrant.io) - Cloud-hosted vector search (1GB free tier)
- **Embeddings**: [OpenAI Embeddings API](https://platform.openai.com/docs/guides/embeddings) - High-quality semantic embeddings
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
- Git
- Qdrant Cloud account (free tier: https://cloud.qdrant.io)
- OpenAI API key (https://platform.openai.com/api-keys)

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

3. **Set up Qdrant Cloud**
   - Visit https://cloud.qdrant.io
   - Create a free account (1GB storage included)
   - Create a new cluster
   - Copy your cluster URL and API key

4. **Configure environment variables**
   ```bash
   cd tax_rag_project
   cp .env.example .env
   # Edit .env with your Qdrant Cloud credentials:
   # QDRANT_URL=https://your-cluster.cloud.qdrant.io
   # QDRANT_API_KEY=your-api-key
   # OPENAI_API_KEY=sk-proj-...
   ```

5. **Test Qdrant Cloud connection**
   ```bash
   # Test connection to Qdrant Cloud
   python src/tax_rag_scraper/test_qdrant_connection.py
   ```

6. **Run the crawler**
   ```bash
   # Run main crawler with Qdrant Cloud integration
   python src/tax_rag_scraper/main.py

   # Or run integration test
   python src/tax_rag_scraper/test_qdrant_integration.py
   ```

7. **Verify in Qdrant Cloud**
   - Visit https://cloud.qdrant.io
   - Check your cluster dashboard
   - View the `tax_documents` collection
   - Monitor vector count and storage usage

## Development Setup

### Environment Variables

See [.env.example](.env.example) for all configuration options including:
- **Qdrant Cloud**: URL and API key (required)
- **OpenAI API**: API key for embeddings (required)
- **Crawler settings**: max requests, concurrency, rate limits
- **Storage paths**: local file storage configuration
- **Retry and timeout configurations**: error handling settings

### Qdrant Cloud Setup

1. **Create Account**
   - Visit https://cloud.qdrant.io
   - Sign up for free (1GB storage included)

2. **Create Cluster**
   - Click "Create Cluster"
   - Choose free tier
   - Select region closest to you
   - Wait for cluster provisioning (~1-2 minutes)

3. **Get Credentials**
   - Copy your cluster URL: `https://xyz-example.cloud.qdrant.io`
   - Copy your API key from the dashboard
   - Add both to your `.env` file

4. **Collection Creation**
   - Collections are created automatically when you run the crawler
   - The `tax_documents` collection will be created on first run
   - Vector dimension: 1536 (OpenAI text-embedding-3-small)

### Project Configuration

Configuration follows a staged implementation approach. This will not be specified in the README.md file here.

See [claude.md](claude.md) for detailed development notes and architecture decisions.

## Contributing

See the main [Crawlee CHANGELOG](../CHANGELOG.md) for framework updates.
See [CHANGELOG.md](CHANGELOG.md) for project-specific changes.

## License

This project uses the Crawlee framework (Apache License 2.0).
Project-specific code: TODO - Define license

## Architecture

This project uses a **cloud-first architecture** with:
- **Qdrant Cloud** for vector storage (no local Docker required)
- **OpenAI API** for generating embeddings
- **GitHub Actions** for automated deployment (Stage 6)

**Benefits:**
- No Docker setup or maintenance required
- Automatic scaling and high availability
- Free tier suitable for development and testing
- Production-ready from day one

## Links

- [Crawlee Documentation](https://crawlee.dev/python/)
- [Qdrant Cloud](https://cloud.qdrant.io)
- [Qdrant Documentation](https://qdrant.tech/documentation/)
- [OpenAI Embeddings](https://platform.openai.com/docs/guides/embeddings)
- [Project Changelog](CHANGELOG.md)
