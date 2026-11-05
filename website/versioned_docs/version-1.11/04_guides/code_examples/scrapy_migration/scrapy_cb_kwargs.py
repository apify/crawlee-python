import scrapy


class BooksSpider(scrapy.Spider):
    name = 'books'
    start_urls = ['https://books.toscrape.com/']

    def parse(self, response):
        for book in response.css('article.product_pod'):
            href = book.css('h3 a::attr(href)').get()
            # Carry the listing price into the detail callback via `cb_kwargs`.
            yield response.follow(
                href,
                callback=self.parse_book,
                cb_kwargs={'listing_price': book.css('p.price_color::text').get()},
            )

    def parse_book(self, response, listing_price):
        yield {
            'title': response.css('h1::text').get(),
            'listing_price': listing_price,
        }
