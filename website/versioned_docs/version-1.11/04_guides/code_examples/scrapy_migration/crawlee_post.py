import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def login_page(context: ParselCrawlingContext) -> None:
        # The CSRF token is tied to the session cookie issued for this GET.
        if not context.session:
            raise RuntimeError('Session not found')

        # highlight-start
        # Like Scrapy's `FormRequest.from_response`, the helper keeps the hidden
        # `csrf_token` field, encodes the data and sets the `Content-Type` header.
        requests = await context.extract_form_requests(
            fields={'username': 'user', 'password': 'pass'},
            label='after-login',
            # Bind the POST to the same session so its CSRF cookie matches.
            session_id=context.session.id,
        )
        await context.add_requests(requests)
        # highlight-end

    @crawler.router.handler('after-login')
    async def after_login(context: ParselCrawlingContext) -> None:
        logged_in = context.selector.css('a[href="/logout"]').get() is not None
        await context.push_data({'logged_in': logged_in})

    await crawler.run(['https://quotes.toscrape.com/login'])


if __name__ == '__main__':
    asyncio.run(main())
