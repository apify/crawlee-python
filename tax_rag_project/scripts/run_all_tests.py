"""Simple test runner for all tax_rag_scraper tests."""

import asyncio
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

# Import test modules
sys.path.insert(0, str(project_root / 'tests'))

from test_error_handling import main as test_error_handling
from test_rate_limiting import main as test_rate_limiting


async def run_all_tests():
    """Run all tests sequentially."""
    print('=' * 60)
    print('Running Error Handling Tests')
    print('=' * 60)
    await test_error_handling()

    print('\n' + '=' * 60)
    print('Running Rate Limiting Tests')
    print('=' * 60)
    await test_rate_limiting()


if __name__ == '__main__':
    asyncio.run(run_all_tests())
