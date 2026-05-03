import asyncio
from typing import cast

from crawlee.browsers import StagehandOptions
from crawlee.crawlers import StagehandCrawler, StagehandCrawlingContext


async def main() -> None:
    # Use Browserbase cloud browser instead of a local Chromium instance.
    crawler = StagehandCrawler(
        stagehand_options=StagehandOptions(
            env='BROWSERBASE',
            browserbase_api_key='your-browserbase-api-key',
            project_id='your-project-id',
            model_api_key='your-openai-api-key',
            model='openai/gpt-4.1-mini',
        ),
        max_requests_per_crawl=5,
    )

    @crawler.router.default_handler
    async def handler(context: StagehandCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        extracted = await context.page.extract(
            instruction='Get the main content of the page',
        )

        extract_result = extracted.data.result

        await context.push_data(cast('dict[str, str | None]', extract_result))

    await crawler.run(['https://example.com'])


if __name__ == '__main__':
    asyncio.run(main())
