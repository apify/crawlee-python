import asyncio

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    crawler = HttpCrawler()

    @crawler.router.default_handler
    async def handler(context: HttpCrawlingContext) -> None:
        if context.session is None:
            return

        # Set a cookie on the session. It's sent with every following
        # request that runs on this session.
        context.session.cookies.set(
            name='csrf_token',
            value='abc123',
            domain='httpbingo.org',
        )

        # Iterate over the cookies stored on the session, including ones the
        # server set via `Set-Cookie` on the current or an earlier response.
        for cookie in context.session.cookies:
            context.log.info(f'{cookie["name"]}={cookie["value"]}')

        # Read a single cookie value by name.
        token = context.session.cookies['csrf_token']
        context.log.info(f'CSRF token: {token}')

    # The server sets a `session_id` cookie and echoes the request cookies back.
    await crawler.run(['https://httpbingo.org/cookies/set?session_id=42'])


if __name__ == '__main__':
    asyncio.run(main())
