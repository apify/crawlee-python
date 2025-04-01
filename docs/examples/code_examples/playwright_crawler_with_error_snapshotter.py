from crawlee.crawlers import PlaywrightCrawler
from crawlee.statistics import Statistics

crawler = PlaywrightCrawler(
    statistics=Statistics.with_default_state(save_error_snapshots=True)
)
