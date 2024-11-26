from bs4 import BeautifulSoup

from crawlee.http_crawler import ParsedHttpCrawlingContext

BeautifulSoupCrawlingContext = ParsedHttpCrawlingContext[BeautifulSoup]
