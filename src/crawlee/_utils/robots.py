from __future__ import annotations

from asyncio import to_thread
from typing import TYPE_CHECKING
from urllib.robotparser import RobotFileParser

from yarl import URL

from crawlee._utils.web import is_status_code_client_error

if TYPE_CHECKING:
    from crawlee.http_clients import HttpClient
    from crawlee.proxy_configuration import ProxyInfo


class RobotsTxtFile:
    def __init__(self, robots: RobotFileParser) -> None:
        self._robots = robots

    @staticmethod
    async def from_content(url: str, content: str) -> RobotsTxtFile:
        robots = RobotFileParser(url=url)
        robots.parse(content.splitlines())
        return RobotsTxtFile(robots)

    @staticmethod
    async def find(
        url: str, proxy_info: ProxyInfo | None = None, http_client: HttpClient | None = None
    ) -> RobotsTxtFile:
        """Find the robots.txt file for a given URL."""
        robots_url = URL(url).with_path('/robots.txt')
        return await RobotsTxtFile.load(str(robots_url), proxy_info, http_client)

    @staticmethod
    async def load(
        url: str, proxy_info: ProxyInfo | None = None, http_client: HttpClient | None = None
    ) -> RobotsTxtFile:
        """Load the robots.txt file for a given URL."""
        robots = RobotFileParser(url=url)
        if http_client is None:
            await to_thread(robots.read)
        else:
            response = await http_client.send_request(url, proxy_info=proxy_info)
            if is_status_code_client_error(response.status_code):
                robots.allow_all = True  # type: ignore[attr-defined] # allow_all is a valid RobotFileParser
        return RobotsTxtFile(robots)

    def is_allowed(self, url: str, user_agent: str = '*') -> bool:
        """Check if the given URL is allowed for the given user agent."""
        return self._robots.can_fetch(user_agent, url)

    def get_sitemaps(self) -> list[str]:
        """Get the list of sitemaps from the robots.txt file."""
        return self._robots.site_maps() or []

    def get_crawl_delay(self, user_agent: str = '*') -> int | None:
        """Get the crawl delay for the given user agent."""
        crawl_delay = self._robots.crawl_delay(user_agent)
        return int(crawl_delay) if crawl_delay is not None else None
