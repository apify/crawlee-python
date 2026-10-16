from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class BooksSpider(CrawlSpider):
    name = 'books'
    start_urls = ['https://books.toscrape.com/']

    rules = (
        # Follow pagination, no callback.
        Rule(LinkExtractor(restrict_css='li.next')),
        # Extract each book detail page.
        Rule(
            LinkExtractor(restrict_css='article.product_pod h3', allow=r'/catalogue/'),
            callback='parse_book',
        ),
    )

    def parse_book(self, response):
        yield {
            'title': response.css('h1::text').get(),
            'price': response.css('p.price_color::text').get(),
        }
