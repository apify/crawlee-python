from __future__ import annotations

import inspect
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from more_itertools import flatten
from pydantic import AnyHttpUrl, TypeAdapter
from typing_extensions import Protocol
from yarl import URL

from crawlee._utils.crypto import crypto_random_object_id
from crawlee._utils.docs import docs_group

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence

    from crawlee import Request

__all__ = ['ProxyConfiguration', 'ProxyInfo']


@dataclass
@docs_group('Other')
class ProxyInfo:
    """Provides information about a proxy connection that is used for requests."""

    url: str
    """The URL of the proxy."""

    scheme: str
    """The scheme of the proxy."""

    hostname: str
    """The hostname of the proxy."""

    port: int
    """The proxy port."""

    username: str = ''
    """The username for the proxy."""

    password: str = ''
    """The password for the proxy."""

    session_id: str | None = None
    """The identifier of the used proxy session, if used.
    Using the same session ID guarantees getting the same proxy URL."""

    proxy_tier: int | None = None
    """The tier of the proxy."""


@docs_group('Configuration')
class ProxyConfiguration:
    """Configures connection to a proxy server with the provided options.

    Proxy servers are used to prevent target websites from blocking your crawlers based on IP address rate limits or
    blacklists. Setting proxy configuration in your crawlers automatically configures them to use the selected proxies
    for all connections. You can get information about the currently used proxy by inspecting the {@apilink ProxyInfo}
    property in your crawler's page function. There, you can inspect the proxy's URL and other attributes.

    If you want to use your own proxies, use the {@apilink ProxyConfigurationOptions.proxyUrls} option. Your list of
    proxy URLs will be rotated by the configuration if this option is provided.
    """

    def __init__(
        self,
        *,
        proxy_urls: list[str | None] | None = None,
        new_url_function: _NewUrlFunction | None = None,
        tiered_proxy_urls: list[list[str | None]] | None = None,
    ) -> None:
        """Initialize a new instance.

        Exactly one of `proxy_urls`, `tiered_proxy_urls` or `new_url_function` must be specified.

        Args:
            proxy_urls: A list of URLs of proxies that will be rotated in a round-robin fashion
            tiered_proxy_urls: A list of URL tiers (where a tier is a list of proxy URLs). Crawlers will automatically
                try to use the lowest tier (smallest index) where blocking does not happen. The proxy URLs in
                the selected tier will be rotated in a round-robin fashion.
            new_url_function: A function that returns a proxy URL for a given Request. This provides full control over
                the proxy selection mechanism.
        """
        self._next_custom_url_index = 0
        self._used_proxy_urls = dict[str, URL | None]()
        self._url_validator = TypeAdapter(AnyHttpUrl)

        # Validation
        if sum(map(bool, (proxy_urls, new_url_function, list(flatten(tiered_proxy_urls or []))))) != 1:
            raise ValueError(
                'Exactly one of `proxy_urls`, `tiered_proxy_urls` and `new_url_function` '
                'must be specified (and non-empty).'
            )

        self._proxy_urls = [self._create_url(url) for url in proxy_urls] if proxy_urls else []
        self._proxy_tier_tracker = (
            _ProxyTierTracker([[self._create_url(url) for url in tier] for tier in tiered_proxy_urls])
            if tiered_proxy_urls
            else None
        )
        self._new_url_function = new_url_function

    def _create_url(self, url: str | None) -> URL | None:
        """Create URL from input string. None means that intentionally no proxy should be used."""
        if url is None:
            return None

        self._url_validator.validate_python(url)
        return URL(url)

    async def new_proxy_info(
        self, session_id: str | None, request: Request | None, proxy_tier: int | None
    ) -> ProxyInfo | None:
        """Return a new ProxyInfo object based on the configured proxy rotation strategy.

        Args:
            session_id: Session identifier. If provided, same proxy URL will be returned for
                subsequent calls with this ID. Will be auto-generated for tiered proxies if
                not provided.
            request: Request object used for proxy rotation and tier selection. Required for
                tiered proxies to track retries and adjust tier accordingly.
            proxy_tier: Specific proxy tier to use. If not provided, will be automatically
                selected based on configuration.
        """
        if self._proxy_tier_tracker is not None and session_id is None:
            session_id = crypto_random_object_id(6)

        url, proxy_tier = await self._pick_url(session_id, request, proxy_tier)

        if url is None:
            return None

        if url.port is None:
            raise ValueError(f'Port is None for URL: {url}')

        if url.host is None:
            raise ValueError(f'Host is None for URL: {url}')

        info = ProxyInfo(
            url=str(url),
            scheme=url.scheme,
            hostname=url.host,
            port=url.port,
            username=url.user or '',
            password=url.password or '',
        )

        if session_id is not None:
            info.session_id = session_id

        if proxy_tier is not None:
            info.proxy_tier = proxy_tier

        return info

    async def new_url(
        self, session_id: str | None = None, request: Request | None = None, proxy_tier: int | None = None
    ) -> str | None:
        """Return a proxy URL string based on the configured proxy rotation strategy.

        Args:
            session_id: Session identifier. If provided, same proxy URL will be returned for
                subsequent calls with this ID. Will be auto-generated for tiered proxies if
                not provided.
            request: Request object used for proxy rotation and tier selection. Required for
                tiered proxies to track retries and adjust tier accordingly.
            proxy_tier: Specific proxy tier to use. If not provided, will be automatically
                selected based on configuration.
        """
        proxy_info = await self.new_proxy_info(session_id, request, proxy_tier)
        return proxy_info.url if proxy_info else None

    async def _pick_url(
        self, session_id: str | None, request: Request | None, proxy_tier: int | None
    ) -> tuple[URL | None, int | None]:
        if self._new_url_function:
            try:
                result = self._new_url_function(session_id, request)
                if inspect.isawaitable(result):
                    result = await result

                return URL(result) if result is not None else None, None
            except Exception as e:
                raise ValueError('The provided "new_url_function" did not return a valid URL') from e

        if self._proxy_tier_tracker:
            if request is not None and proxy_tier is None:
                hostname = URL(request.url).host
                if hostname is None:
                    raise ValueError('The request URL does not have a hostname')

                if request.last_proxy_tier is not None:
                    self._proxy_tier_tracker.add_error(hostname, request.last_proxy_tier)

                proxy_tier = self._proxy_tier_tracker.predict_tier(hostname)

                request.last_proxy_tier = proxy_tier
                request.forefront = True

            if proxy_tier is not None:
                urls = self._proxy_tier_tracker.get_tier_urls(proxy_tier)
            else:
                urls = self._proxy_tier_tracker.all_urls
        elif self._proxy_urls:
            urls = self._proxy_urls
        else:
            raise RuntimeError('Invalid state')

        if session_id is None:
            url = urls[self._next_custom_url_index % len(urls)]
            self._next_custom_url_index += 1
            return url, proxy_tier

        if session_id not in self._used_proxy_urls:
            self._used_proxy_urls[session_id] = urls[self._next_custom_url_index % len(urls)]
            self._next_custom_url_index += 1

        return self._used_proxy_urls[session_id], proxy_tier


class _ProxyTierTracker:
    """Tracks the state of currently used proxy tiers and their error frequency for individual crawled domains."""

    def __init__(self, tiered_proxy_urls: list[list[URL | None]]) -> None:
        self._tiered_proxy_urls = tiered_proxy_urls
        self._histogram_by_domain = defaultdict[str, list[int]](lambda: [0 for _tier in tiered_proxy_urls])
        self._current_tier_by_domain = defaultdict[str, int](lambda: 0)

    @property
    def all_urls(self) -> Sequence[URL | None]:
        return list(flatten(self._tiered_proxy_urls))

    def get_tier_urls(self, tier_number: int) -> Sequence[URL | None]:
        return self._tiered_proxy_urls[tier_number]

    def add_error(self, domain: str, tier: int) -> None:
        self._histogram_by_domain[domain][tier] += 10

    def predict_tier(self, domain: str) -> int:
        histogram = self._histogram_by_domain[domain]
        current_tier = self._current_tier_by_domain[domain]

        for index, value in enumerate(histogram):
            if index == current_tier:
                continue
            if value > 0:
                histogram[index] -= 1

        left = histogram[current_tier - 1] if current_tier > 0 else float('inf')
        right = histogram[current_tier + 1] if current_tier < len(histogram) - 1 else float('inf')

        if histogram[current_tier] > min(left, right):
            self._current_tier_by_domain[domain] = current_tier - 1 if left <= right else current_tier + 1
        elif histogram[current_tier] == left:
            self._current_tier_by_domain[domain] -= 1

        return self._current_tier_by_domain[domain]


class _NewUrlFunction(Protocol):
    def __call__(
        self,
        session_id: str | None = None,
        request: Request | None = None,
    ) -> str | None | Awaitable[str | None]: ...
