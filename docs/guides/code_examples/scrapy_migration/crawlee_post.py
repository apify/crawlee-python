import asyncio
from urllib.parse import urlencode

from crawlee import Request
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def login_page(context: ParselCrawlingContext) -> None:
        # The CSRF token is tied to the session cookie issued for this GET.
        if not context.session:
            raise RuntimeError('Session not found')

        token = context.selector.css('input[name="csrf_token"]::attr(value)').get()
        form = {'csrf_token': token, 'username': 'user', 'password': 'pass'}

        # Crawlee's `payload` is the raw request body, so encode the fields yourself
        # and set the `Content-Type`. Scrapy's `FormRequest` does both for you.
        await context.add_requests(
            [
                Request.from_url(
                    'https://quotes.toscrape.com/login',
                    method='POST',
                    payload=urlencode(form),
                    headers={'content-type': 'application/x-www-form-urlencoded'},
                    label='after-login',
                    # Bind the POST to the same session so its CSRF cookie matches.
                    session_id=context.session.id,
                    # The POST shares the GET's URL. Include the method and payload
                    # in the unique key, or the queue drops it as a duplicate.
                    use_extended_unique_key=True,
                )
            ]
        )

    @crawler.router.handler('after-login')
    async def after_login(context: ParselCrawlingContext) -> None:
        logged_in = context.selector.css('a[href="/logout"]').get() is not None
        await context.push_data({'logged_in': logged_in})

    await crawler.run(['https://quotes.toscrape.com/login'])


if __name__ == '__main__':
    asyncio.run(main())
