import asyncio
from typing import TYPE_CHECKING, cast

from crawlee.crawlers import BasicCrawlingContext, HttpCrawler, HttpCrawlingContext

if TYPE_CHECKING:
    from crawlee.sessions import CookieParam


async def main() -> None:
    crawler = HttpCrawler()

    @crawler.router.default_handler
    async def handler(context: HttpCrawlingContext) -> None:
        if context.session is None:
            return

        state = await context.use_state(default_value={})
        if not state.get('cookies'):
            # First pass: establish cookies once and store them for every session,
            # then raise to trigger a retry.
            await context.send_request('https://httpbingo.org/cookies/set?logged_in=1')
            state['cookies'] = context.session.cookies.get_cookies_as_dicts()
            raise RuntimeError('retry with cookies')

        context.log.info(f'Cookies: {context.session.cookies.get_cookies_as_dicts()}')

    @crawler.pre_navigation_hook
    async def apply_cookies(context: BasicCrawlingContext) -> None:
        # Runs before navigation. Apply the shared cookies to whichever session
        # handles the request, so every session ends up with them.
        state = await context.use_state(default_value={})
        if context.session and (cookies := state.get('cookies')):
            context.session.cookies.set_cookies(cast('list[CookieParam]', cookies))

    await crawler.run(['https://httpbingo.org/cookies'])


if __name__ == '__main__':
    asyncio.run(main())
