# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

This project uses `make` for development tasks and `uv` for Python package management:

- **Setup**: `make install-dev` - Install dependencies and pre-commit hooks, install Playwright
- **Build**: `make build` - Build the package
- **Linting**: `make lint` - Run ruff format check and lint
- **Formatting**: `make format` - Auto-format code with ruff
- **Type checking**: `make type-check` - Run mypy type checker
- **Unit tests**: `make unit-tests` - Run pytest unit tests
- **Unit tests with coverage**: `make unit-tests-cov` - Run unit tests with HTML coverage report
- **End-to-end tests**: `make e2e-templates-tests` - Run E2E tests for project templates (requires apify-cli and APIFY_TEST_USER_API_TOKEN)
- **Complete code check**: `make check-code` - Run lint, type-check, and unit-tests
- **Documentation**: `make run-docs` - Build API reference and serve docs locally
- **Clean**: `make clean` - Remove cache and build files

Always run `make check-code` before committing changes.

## Architecture Overview

Crawlee is a Python web scraping and browser automation library with a modular crawler architecture:

### Core Components

- **Service Locator** (`_service_locator.py`) - Dependency injection system for managing shared services
- **Request System** (`_request.py`) - Core request handling and queueing
- **Configuration** (`configuration.py`) - Global configuration management
- **Router** (`router.py`) - Request routing to appropriate handlers

### Crawler Hierarchy

The library provides multiple crawler types in a hierarchical structure:

1. **BasicCrawler** (`crawlers/_basic/`) - Base crawler with request queue management and concurrency
2. **HttpCrawler** (`crawlers/_http/`) - Extends BasicCrawler with HTTP client functionality  
3. **AbstractHttpCrawler** (`crawlers/_abstract_http/`) - Abstract base for parsers
4. **Specialized HTTP Crawlers**:
   - **BeautifulSoupCrawler** (`crawlers/_beautifulsoup/`) - HTML parsing with BeautifulSoup
   - **ParselCrawler** (`crawlers/_parsel/`) - HTML/XML parsing with Parsel
5. **Browser Crawlers**:
   - **PlaywrightCrawler** (`crawlers/_playwright/`) - Full browser automation
   - **AdaptivePlaywrightCrawler** (`crawlers/_adaptive_playwright/`) - Smart rendering detection

### Key Systems

- **Storage Clients** (`storage_clients/`) - Pluggable storage backends (memory, filesystem, Apify)
- **HTTP Clients** (`http_clients/`) - HTTP communication layer with fingerprinting
- **Sessions** (`sessions/`) - Session and cookie management
- **Statistics** (`statistics/`) - Crawling performance metrics
- **Events** (`events/`) - Event system for crawler lifecycle
- **Autoscaling** (`_autoscaling/`) - Dynamic concurrency management

### Optional Dependencies

The project uses conditional imports (`_utils/try_import.py`) to handle optional dependencies:
- BeautifulSoup support requires `beautifulsoup` extra
- Playwright support requires `playwright` extra  
- Parsel support requires `parsel` extra
- Adaptive crawling requires `adaptive-crawler` extra

### Configuration Files

- `pyproject.toml` - Main project configuration with extensive ruff linting rules
- Tool configuration is centralized in pyproject.toml (ruff, mypy, pytest)
- Python 3.10+ required with type hints throughout

## Development Patterns

- **Async/await**: All crawlers use asyncio for concurrency
- **Type hints**: Complete type coverage enforced by mypy
- **Modular design**: Crawler features are composable via inheritance and mixins
- **Plugin architecture**: Storage clients and HTTP clients are pluggable
- **Error handling**: Robust retry mechanisms and error recovery built-in