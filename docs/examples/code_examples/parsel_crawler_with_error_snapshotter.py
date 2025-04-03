from crawlee.crawlers import ParselCrawler
from crawlee.statistics import Statistics

crawler = ParselCrawler(
    statistics=Statistics.with_default_state(save_error_snapshots=True)
)
