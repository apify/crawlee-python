import asyncio

from crawlee import Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    crawler = HttpCrawler()

    @crawler.router.default_handler
    async def handler(context: HttpCrawlingContext) -> None:
        if context.session is None:
            return

        if context.request.user_data.get('prepared'):
            # Cookies from the setup step are on this pinned session.
            context.log.info(f'Cookies: {context.session.cookies.get_cookies_as_dicts()}')
            return

        # First pass: establish cookies on the current session.
        await context.send_request('https://httpbingo.org/cookies/set?logged_in=1')

        await context.add_requests(
            [
                Request.from_url(
                    context.request.url,
                    # Bind the new request to the session that now has the cookies.
                    session_id=context.session.id,
                    # Mark the request so the handler skips the setup step on it.
                    user_data={'prepared': True},
                    # Run the same URL again despite deduplication.
                    always_enqueue=True,
                )
            ]
        )

    await crawler.run(['https://httpbingo.org/cookies'])


if __name__ == '__main__':
    asyncio.run(main())
