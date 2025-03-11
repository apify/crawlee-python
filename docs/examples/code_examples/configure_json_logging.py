import asyncio
import inspect
import logging

from loguru import logger

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


# Configure loguru interceptor to capture standard logging output
class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
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

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


# Set up loguru with JSONL serialization in file `crawler.log`
logger.add('crawler.log', serialize=True, level='INFO')

# Configure standard logging to use our interceptor
logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)


async def main() -> None:
    # Initialize crawler with disabled table logs
    crawler = HttpCrawler(
        configure_logging=False,  # Disable default logging configuration
        use_table_logs=False,  # Disable table formatting in statistics logs
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # Run the crawler
    await crawler.run(['https://www.crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
