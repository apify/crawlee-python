import scrapy


class AuthorsSpider(scrapy.Spider):
    name = 'authors'
    start_urls = ['https://quotes.toscrape.com/']

    def parse(self, response):
        for href in response.css('div.quote span a::attr(href)').getall():
            yield response.follow(href, callback=self.parse_author)

        next_page = response.css('li.next a::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def parse_author(self, response):
        yield {
            'name': response.css('h3.author-title::text').get(),
            'born': response.css('span.author-born-date::text').get(),
            'bio': response.css('div.author-description::text').get(),
        }
