import asyncio
import json
import logging

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext

REQUEST_FIELDS = ('url', 'unique_key', 'retry_count', 'status_code')


class RequestContextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Crawlee passes the request context as `extra`, so the fields are attributes of
        # the log record.
        line = {'level': record.levelname, 'message': record.getMessage()}
        for key in REQUEST_FIELDS:
            if hasattr(record, key):
                line[key] = getattr(record, key)
        if record.exc_info:
            line['exception'] = self.formatException(record.exc_info)
        return json.dumps(line)


async def main() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(RequestContextFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    logging.getLogger('crawlee.crawlers').setLevel(logging.DEBUG)

    crawler = HttpCrawler(statistics_log_format='inline')

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
