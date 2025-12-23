"""Robots.txt checker for respectful web scraping."""

from contextlib import suppress
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser


class RobotsChecker:
    """Check if URLs can be fetched according to robots.txt rules."""

    def __init__(self) -> None:
        self._parsers: dict[str, RobotFileParser] = {}

    def can_fetch(self, url: str, user_agent: str = '*') -> bool:
        """Check if URL can be fetched according to robots.txt.

        Args:
            url: URL to check
            user_agent: User agent to check for (default: '*' for all agents)

        Returns:
            True if allowed to fetch, False if disallowed
        """
        parsed = urlparse(url)
        base_url = f'{parsed.scheme}://{parsed.netloc}'

        # Cache parser per domain
        if base_url not in self._parsers:
            robots_url = urljoin(base_url, '/robots.txt')
            parser = RobotFileParser()
            parser.set_url(robots_url)

            # If can't read robots.txt, allow by default (fail open - assume access is allowed)
            with suppress(Exception):
                parser.read()

            self._parsers[base_url] = parser

        return self._parsers[base_url].can_fetch(user_agent, url)
