import asyncio
import json

from crawlee import HttpHeaders
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.errors import HttpStatusCodeError
from crawlee.sessions import SessionPool

# Using a placeholder refresh token for this example
REFRESH_TOKEN = 'PLACEHOLDER'
UNAUTHORIZED_CODE = 401


async def main() -> None:
    crawler = HttpCrawler(
        max_request_retries=2,
        # Only treat 403 as a blocking status code, not 401
        session_pool=SessionPool(create_session_settings={'blocked_status_codes': [403]}),
        # Don't treat 401 responses as errors
        ignore_http_error_status_codes=[UNAUTHORIZED_CODE],
    )

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        # Now we can handle 401 responses ourselves
        if context.http_response.status_code == UNAUTHORIZED_CODE:
            # Get a fresh access token
            headers = {'authorization': f'Bearer {REFRESH_TOKEN}'}
            response = await context.send_request(
                'https://placeholder.org/refresh', headers=headers
            )
            data = json.loads(response.read())
            # Add the new token to our `Request` headers
            new_headers = {
                **context.request.headers,
                'authorization': f'Bearer {data["access_token"]}',
            }
            context.request.headers = HttpHeaders(new_headers)
            # Trigger a retry with our updated headers
            raise HttpStatusCodeError('Unauthorized', status_code=UNAUTHORIZED_CODE)

    await crawler.run(['http://httpbingo.org/status/401'])


if __name__ == '__main__':
    asyncio.run(main())
