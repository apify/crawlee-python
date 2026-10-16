import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        await context.enqueue_links()

    # `run()` returns a snapshot of the statistics at the end of the run.
    final_stats = await crawler.run(['https://crawlee.dev'])

    print(f'Finished requests: {final_stats.requests_finished}')
    print(f'Failed requests: {final_stats.requests_failed}')
    print(f'Average duration: {final_stats.request_avg_finished_duration}')
    print(f'Crawler runtime: {final_stats.crawler_runtime}')

    # The full state, including fields that `FinalStatistics` doesn't expose.
    state = crawler.statistics.state
    print(f'Status codes: {state.requests_with_status_code}')
    print(f'Slowest request: {state.request_max_duration}')

    # Errors grouped by similarity.
    error_tracker = crawler.statistics.error_tracker
    print(f'Total errors: {error_tracker.total}')
    print(f'Most common errors: {error_tracker.get_most_common_errors()}')


if __name__ == '__main__':
    asyncio.run(main())
