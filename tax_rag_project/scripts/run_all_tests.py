"""Simple test runner for all tax_rag_scraper tests."""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

# Import test modules
sys.path.insert(0, str(project_root / 'tests'))

# Import after path setup - this is required for the test modules
from test_error_handling import main as test_error_handling  # noqa: E402
from test_rate_limiting import main as test_rate_limiting  # noqa: E402

logger = logging.getLogger(__name__)


async def run_all_tests() -> None:
    """Run all tests sequentially."""
    logger.info('=' * 60)
    logger.info('Running Error Handling Tests')
    logger.info('=' * 60)
    await test_error_handling()

    logger.info('\n%s', '=' * 60)
    logger.info('Running Rate Limiting Tests')
    logger.info('=' * 60)
    await test_rate_limiting()


if __name__ == '__main__':
    asyncio.run(run_all_tests())
