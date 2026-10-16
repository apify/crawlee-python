import scrapy


class LoginSpider(scrapy.Spider):
    name = 'login'
    start_urls = ['https://quotes.toscrape.com/login']

    def parse(self, response):
        # `from_response` picks up the hidden `csrf_token` field on its own, encodes
        # the data as `form-urlencoded`, and sets the `Content-Type` header.
        yield scrapy.FormRequest.from_response(
            response,
            formdata={'username': 'user', 'password': 'pass'},
            callback=self.after_login,
        )

    def after_login(self, response):
        yield {'logged_in': bool(response.css('a[href="/logout"]'))}
