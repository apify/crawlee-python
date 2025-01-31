from crawlee.crawlers import AdaptivePlaywrightCrawler

crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
    # Arguments relevant only for PlaywrightCrawler
    playwright_crawler_specific_kwargs={'headless': False, 'browser_type': 'chromium'},
    # Arguments relevant only for BeautifulSoupCrawler
    static_crawler_specific_kwargs={'additional_http_error_status_codes': [204]},
    # Common arguments relevant to all crawlers
    max_crawl_depth=5,
)
