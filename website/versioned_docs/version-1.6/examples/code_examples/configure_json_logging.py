from __future__ import annotations

import asyncio
import inspect
import logging
import sys
from typing import TYPE_CHECKING

from loguru import logger

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext

if TYPE_CHECKING:
    from loguru import Record


# Configure loguru interceptor to capture standard logging output
class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = inspect.currentframe(), 0
        while frame:
            filename = frame.f_code.co_filename
            is_logging = filename == logging.__file__
            is_frozen = 'importlib' in filename and '_bootstrap' in filename
            if depth > 0 and not (is_logging | is_frozen):
                break
            frame = frame.f_back
            depth += 1

        dummy_record = logging.LogRecord('dummy', 0, 'dummy', 0, 'dummy', None, None)
        standard_attrs = set(dummy_record.__dict__.keys())
        extra_dict = {
            key: value
            for key, value in record.__dict__.items()
            if key not in standard_attrs
        }

        (
            logger.bind(**extra_dict)
            .opt(depth=depth, exception=record.exc_info)
            .patch(lambda loguru_record: loguru_record.update({'name': record.name}))
            .log(level, record.getMessage())
        )


# Configure loguru formatter
def formatter(record: Record) -> str:
    basic_format = '[{name}] | <level>{level: ^8}</level> | - {message}'
    if record['extra']:
        basic_format = basic_format + ' {extra}'
    return f'{basic_format}\n'


# Remove default loguru logger
logger.remove()

# Set up loguru with JSONL serialization in file `crawler.log`
logger.add('crawler.log', format=formatter, serialize=True, level='INFO')

# Set up loguru logger for console
logger.add(sys.stderr, format=formatter, colorize=True, level='INFO')

# Configure standard logging to use our interceptor
logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)


async def main() -> None:
    # Initialize crawler with disabled table logs
    crawler = HttpCrawler(
        configure_logging=False,  # Disable default logging configuration
        statistics_log_format='inline',  # Set inline formatting for statistics logs
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # Run the crawler
    await crawler.run(['https://www.crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
