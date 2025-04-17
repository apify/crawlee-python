from __future__ import annotations

from typing import TYPE_CHECKING

from protego import Protego  # type: ignore[import-untyped]
from yarl import URL

from crawlee._utils.web import is_status_code_client_error

if TYPE_CHECKING:
    from crawlee.http_clients import HttpClient
    from crawlee.proxy_configuration import ProxyInfo


class RobotsTxtFile:
    def __init__(self, url: str, robots: Protego) -> None:
        self._robots = robots
        self._original_url = URL(url).origin()

    @staticmethod
    async def from_content(url: str, content: str) -> RobotsTxtFile:
        robots = Protego.parse(content)
        return RobotsTxtFile(url, robots)

    @staticmethod
    async def find(url: str, http_client: HttpClient, proxy_info: ProxyInfo | None = None) -> RobotsTxtFile:
        """Find the robots.txt file for a given URL."""
        robots_url = URL(url).with_path('/robots.txt')
        return await RobotsTxtFile.load(str(robots_url), http_client, proxy_info)

    @staticmethod
    async def load(url: str, http_client: HttpClient, proxy_info: ProxyInfo | None = None) -> RobotsTxtFile:
        """Load the robots.txt file for a given URL."""
        response = await http_client.send_request(url, proxy_info=proxy_info)
        body = b'User-agent: *\nAllow: /' if is_status_code_client_error(response.status_code) else response.read()

        robots = Protego.parse(body.decode('utf-8'))

        return RobotsTxtFile(url, robots)

    def is_allowed(self, url: str, user_agent: str = '*') -> bool:
        """Check if the given URL is allowed for the given user agent."""
        check_url = URL(url)
        if check_url.origin() != self._original_url:
            return True
        return bool(self._robots.can_fetch(str(check_url), user_agent))

    def get_sitemaps(self) -> list[str]:
        """Get the list of sitemaps from the robots.txt file."""
        return list(self._robots.sitemaps)

    def get_crawl_delay(self, user_agent: str = '*') -> int | None:
        """Get the crawl delay for the given user agent."""
        crawl_delay = self._robots.crawl_delay(user_agent)
        return int(crawl_delay) if crawl_delay is not None else None
