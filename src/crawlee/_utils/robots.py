from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

from protego import Protego
from yarl import URL

from crawlee._utils.sitemap import Sitemap
from crawlee._utils.urls import matches_enqueue_strategy
from crawlee._utils.web import is_status_code_client_error

if TYPE_CHECKING:
    from typing_extensions import Self

    from crawlee._types import EnqueueStrategy
    from crawlee.http_clients import HttpClient
    from crawlee.proxy_configuration import ProxyInfo


logger = getLogger(__name__)


class RobotsTxtFile:
    def __init__(
        self,
        url: str,
        robots: Protego,
        http_client: HttpClient | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> None:
        self._robots = robots
        self._original_url = URL(url).origin()
        self._http_client = http_client
        self._proxy_info = proxy_info

    @classmethod
    async def from_content(cls, url: str, content: str) -> Self:
        """Create a `RobotsTxtFile` instance from the given content.

        Args:
            url: The URL associated with the robots.txt file.
            content: The raw string content of the robots.txt file to be parsed.
        """
        robots = Protego.parse(content)
        return cls(url, robots)

    @classmethod
    async def find(cls, url: str, http_client: HttpClient, proxy_info: ProxyInfo | None = None) -> Self:
        """Determine the location of a robots.txt file for a URL and fetch it.

        Args:
            url: The URL whose domain will be used to find the corresponding robots.txt file.
            http_client: Optional `ProxyInfo` to be used when fetching the robots.txt file. If None, no proxy is used.
            proxy_info: The `HttpClient` instance used to perform the network request for fetching the robots.txt file.
        """
        robots_url = URL(url).with_path('/robots.txt')
        return await cls.load(str(robots_url), http_client, proxy_info)

    @classmethod
    async def load(cls, url: str, http_client: HttpClient, proxy_info: ProxyInfo | None = None) -> Self:
        """Load the robots.txt file for a given URL.

        Args:
            url: The direct URL of the robots.txt file to be loaded.
            http_client: The `HttpClient` instance used to perform the network request for fetching the robots.txt file.
            proxy_info: Optional `ProxyInfo` to be used when fetching the robots.txt file. If None, no proxy is used.
        """
        try:
            response = await http_client.send_request(url, proxy_info=proxy_info)

            body = (
                b'User-agent: *\nAllow: /'
                if is_status_code_client_error(response.status_code)
                else await response.read()
            )
            robots = Protego.parse(body.decode('utf-8'))

        except Exception as e:
            logger.warning(f'Failed to fetch from robots.txt from "{url}" with error: "{e}"')

            robots = Protego.parse('User-agent: *\nAllow: /')

        return cls(url, robots, http_client=http_client, proxy_info=proxy_info)

    def is_allowed(self, url: str, user_agent: str = '*') -> bool:
        """Check if the given URL is allowed for the given user agent.

        Args:
            url: The URL to check against the robots.txt rules.
            user_agent: The user-agent string to check permissions for. Defaults to '*' which matches any user-agent.
        """
        check_url = URL(url)
        if check_url.origin() != self._original_url:
            return True
        return bool(self._robots.can_fetch(str(check_url), user_agent))

    def get_sitemaps(self, *, enqueue_strategy: EnqueueStrategy) -> list[str]:
        """Get the list of sitemap URLs from the robots.txt file, filtered by enqueue strategy.

        Args:
            enqueue_strategy: Strategy used to filter sitemap entries relative to the robots.txt URL's host.
                Pass `'same-hostname'` to match the sitemap protocol's same-host expectation, or `'all'` to
                disable host filtering. Regardless of the strategy, entries with non-`http(s)` schemes are
                always filtered out.
        """
        sitemaps: list[str] = []
        for sitemap_url in self._robots.sitemaps:
            if matches_enqueue_strategy(
                strategy=enqueue_strategy,
                target_url=sitemap_url,
                origin_url=self._original_url,
            ):
                sitemaps.append(sitemap_url)
            else:
                logger.warning(
                    f'Skipping sitemap {sitemap_url!r} listed in robots.txt at {str(self._original_url)!r}: '
                    f'does not match enqueue strategy {enqueue_strategy!r}.'
                )
        return sitemaps

    def get_crawl_delay(self, user_agent: str = '*') -> int | None:
        """Get the crawl delay for the given user agent.

        Args:
            user_agent: The user-agent string to check the crawl delay for. Defaults to '*' which matches any
                user-agent.
        """
        crawl_delay = self._robots.crawl_delay(user_agent)
        return int(crawl_delay) if crawl_delay is not None else None

    async def parse_sitemaps(self, *, enqueue_strategy: EnqueueStrategy) -> Sitemap:
        """Parse the sitemaps from the robots.txt file and return a `Sitemap` instance.

        Args:
            enqueue_strategy: Forwarded to `get_sitemaps`; see that method for details.
        """
        sitemaps = self.get_sitemaps(enqueue_strategy=enqueue_strategy)
        if not self._http_client:
            raise ValueError('HTTP client is required to parse sitemaps.')

        return await Sitemap.load(sitemaps, self._http_client, self._proxy_info)

    async def parse_urls_from_sitemaps(self, *, enqueue_strategy: EnqueueStrategy) -> list[str]:
        """Parse the sitemaps in the robots.txt file and return a list URLs.

        Args:
            enqueue_strategy: Forwarded to `get_sitemaps`; see that method for details.
        """
        sitemap = await self.parse_sitemaps(enqueue_strategy=enqueue_strategy)
        return sitemap.urls
