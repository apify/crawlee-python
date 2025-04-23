from __future__ import annotations

from typing import TYPE_CHECKING

from protego import Protego  # type: ignore[import-untyped]
from yarl import URL

from crawlee._utils.web import is_status_code_client_error

if TYPE_CHECKING:
    from typing_extensions import Self

    from crawlee.http_clients import HttpClient
    from crawlee.proxy_configuration import ProxyInfo


class RobotsTxtFile:
    def __init__(self, url: str, robots: Protego) -> None:
        self._robots = robots
        self._original_url = URL(url).origin()

    @classmethod
    async def from_content(cls, url: str, content: str) -> Self:
        """Create a RobotsTxtFile instance from the given content.

        Args:
            url: the URL of the robots.txt file
            content: the content of the robots.txt file
        """
        robots = Protego.parse(content)
        return cls(url, robots)

    @classmethod
    async def find(cls, url: str, http_client: HttpClient, proxy_info: ProxyInfo | None = None) -> Self:
        """Determine the location of a robots.txt file for a URL and fetch it.

        Args:
            url: the URL to fetch robots.txt for
            proxy_info: a `ProxyInfo` to be used for fetching the robots.txt file
            http_client: the HTTP client to use for fetching the robots.txt file
        """
        robots_url = URL(url).with_path('/robots.txt')
        return await cls.load(str(robots_url), http_client, proxy_info)

    @classmethod
    async def load(cls, url: str, http_client: HttpClient, proxy_info: ProxyInfo | None = None) -> Self:
        """Load the robots.txt file for a given URL.

        Args:
            url: the URL to fetch robots.txt for
            proxy_info: a `ProxyInfo` to be used for fetching the robots.txt file
            http_client: the HTTP client to use for fetching the robots.txt file
        """
        response = await http_client.send_request(url, proxy_info=proxy_info)
        body = b'User-agent: *\nAllow: /' if is_status_code_client_error(response.status_code) else response.read()

        robots = Protego.parse(body.decode('utf-8'))

        return cls(url, robots)

    def is_allowed(self, url: str, user_agent: str = '*') -> bool:
        """Check if the given URL is allowed for the given user agent.

        Args:
            url: the URL to check
            user_agent: the user agent to check for
        """
        check_url = URL(url)
        if check_url.origin() != self._original_url:
            return True
        return bool(self._robots.can_fetch(str(check_url), user_agent))

    def get_sitemaps(self) -> list[str]:
        """Get the list of sitemaps urls from the robots.txt file."""
        return list(self._robots.sitemaps)

    def get_crawl_delay(self, user_agent: str = '*') -> int | None:
        """Get the crawl delay for the given user agent.

        Args:
            user_agent: the user-agent to check for
        """
        crawl_delay = self._robots.crawl_delay(user_agent)
        return int(crawl_delay) if crawl_delay is not None else None
