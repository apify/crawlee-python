from crawlee.crawlers import AdaptivePlaywrightCrawler

crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
    # Arguments relevant only for PlaywrightCrawler
    playwright_crawler_specific_kwargs={'headless': False, 'browser_type': 'chrome'},
    # Arguments relevant only for ParselCrawler
    static_crawler_specific_kwargs={'additional_http_error_status_codes': [204]},
    # Common arguments relevant to all crawlers
    max_crawl_depth=5,
)
