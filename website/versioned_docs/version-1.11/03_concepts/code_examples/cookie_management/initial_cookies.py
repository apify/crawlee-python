import asyncio

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.sessions import SessionPool


async def main() -> None:
    crawler = HttpCrawler(
        session_pool=SessionPool(
            # Seed every new session in the pool with these cookies.
            create_session_settings={
                'cookies': [
                    {'name': 'consent', 'value': 'accepted', 'domain': 'httpbingo.org'},
                    {'name': 'locale', 'value': 'en_US', 'domain': 'httpbingo.org'},
                ],
            },
        ),
    )

    @crawler.router.default_handler
    async def handler(context: HttpCrawlingContext) -> None:
        # The endpoint echoes back the cookies it received.
        response = await context.http_response.read()
        context.log.info(response.decode())

    await crawler.run(['https://httpbingo.org/cookies'])


if __name__ == '__main__':
    asyncio.run(main())
