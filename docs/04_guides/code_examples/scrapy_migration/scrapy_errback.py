import scrapy


class QuotesSpider(scrapy.Spider):
    name = 'quotes'
    start_urls = ['https://quotes.toscrape.com/']

    def start_requests(self):
        for url in self.start_urls:
            # `RetryMiddleware` retries the request automatically before `errback` fires.
            yield scrapy.Request(url, callback=self.parse, errback=self.on_error)

    def parse(self, response):
        for quote in response.css('div.quote'):
            yield {'text': quote.css('span.text::text').get()}

    def on_error(self, failure):
        self.logger.error('Request failed: %s', failure.request.url)
