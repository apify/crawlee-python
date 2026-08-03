import scrapy


class JsQuotesSpider(scrapy.Spider):
    name = 'js-quotes'

    def start_requests(self):
        yield scrapy.Request(
            'https://quotes.toscrape.com/js/',
            meta={'playwright': True},
        )

    def parse(self, response):
        for quote in response.css('div.quote'):
            yield {
                'text': quote.css('span.text::text').get(),
                'author': quote.css('small.author::text').get(),
            }

        next_page = response.css('li.next a::attr(href)').get()
        if next_page is not None:
            # Every followed request has to opt back into rendering.
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={'playwright': True},
            )
