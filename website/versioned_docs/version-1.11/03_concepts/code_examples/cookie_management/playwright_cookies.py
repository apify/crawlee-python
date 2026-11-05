import asyncio

from crawlee import Request
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.sessions import SessionPool


async def main() -> None:
    crawler = PlaywrightCrawler(
        # A single session, so both requests run on the same one.
        session_pool=SessionPool(max_pool_size=1),
    )

    @crawler.router.default_handler
    async def handler(context: PlaywrightCrawlingContext) -> None:
        if context.session is None:
            return

        # The browser receives the `visited` cookie during the first navigation.
        # It's synced back onto the session after the handler returns, so on the
        # second request it's already on `context.session`.
        names = [cookie['name'] for cookie in context.session.cookies]
        context.log.info(f'Session cookies on {context.request.url}: {names}')

        if not context.request.user_data.get('followup'):
            await context.add_requests(
                [
                    Request.from_url(
                        'https://httpbingo.org/cookies',
                        user_data={'followup': True},
                        always_enqueue=True,
                    )
                ]
            )

    await crawler.run(['https://httpbingo.org/cookies/set?visited=1'])


if __name__ == '__main__':
    asyncio.run(main())
