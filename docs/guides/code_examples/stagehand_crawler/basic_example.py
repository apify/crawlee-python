import asyncio
from typing import cast

from crawlee.browsers import StagehandOptions
from crawlee.crawlers import StagehandCrawler, StagehandCrawlingContext


async def main() -> None:
    crawler = StagehandCrawler(
        stagehand_options=StagehandOptions(
            model_api_key='your-openai-api-key',
            model='openai/gpt-4.1-mini',
        ),
        max_requests_per_crawl=5,
    )

    @crawler.router.default_handler
    async def handler(context: StagehandCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Dismiss overlays or interact with the page using natural language.
        await context.page.act(input='Click the accept cookies button if present')

        # Extract data from the page using AI.
        extracted = await context.page.extract(
            instruction='Get the page title and the main heading text',
            schema={
                'type': 'object',
                'properties': {
                    'title': {'type': 'string'},
                    'heading': {'type': 'string'},
                },
            },
        )

        extract_result = extracted.data.result

        if isinstance(extract_result, dict):
            # Push extracted data to the dataset
            # Use `cast()` to provide a more specific type hint for the extracted data.
            await context.push_data(cast('dict[str, str | None]', extract_result))

    await crawler.run(['https://example.com'])


if __name__ == '__main__':
    asyncio.run(main())
