from __future__ import annotations

from functools import cached_property
from importlib.resources import files
from ipaddress import ip_address
from logging import getLogger
from typing import NamedTuple

import impit

from crawlee._utils.web import is_status_code_successful

logger = getLogger(__name__)

PUBLIC_SUFFIX_LIST_URL = 'https://publicsuffix.org/list/public_suffix_list.dat'

PUBLIC_SUFFIX_LIST_SNAPSHOT = files('crawlee._utils') / 'resources' / 'public_suffix_list.dat'

_ICANN_BEGIN = '// ===BEGIN ICANN DOMAINS==='
_ICANN_END = '// ===END ICANN DOMAINS==='

# The download is synchronous and blocks the event loop, so it gets a short timeout.
_DOWNLOAD_TIMEOUT_SECS = 5


class _Rules(NamedTuple):
    """Rules of the ICANN section of the Public Suffix List, split by type."""

    suffixes: frozenset[str]
    """Plain rules, e.g. `co.uk`."""

    wildcards: frozenset[str]
    """Wildcard rules without the leading `*.`, e.g. `kawasaki.jp` for `*.kawasaki.jp`."""

    exceptions: frozenset[str]
    """Exception rules without the leading `!`, e.g. `city.kawasaki.jp`."""


class PublicSuffixList:
    """Registrable domain lookup based on the ICANN section of the Public Suffix List.

    The rules are downloaded on first use, falling back to the bundled snapshot if the download fails. After that,
    they don't change.
    """

    def get_registrable_domain(self, host: str) -> str | None:
        """Return the registrable domain of `host`, or `None` for IP addresses and public suffixes."""
        host = host.rstrip('.')
        if self._is_ip_address(host):
            return None

        labels = host.split('.')
        for i in range(len(labels)):
            suffix = '.'.join(labels[i:])
            if suffix in self._rules.exceptions:
                return suffix
            if suffix in self._rules.suffixes or suffix.partition('.')[2] in self._rules.wildcards:
                return '.'.join(labels[i - 1 :]) if i > 0 else None

        # No rule matched, so the default `*` rule makes the last label the public suffix.
        return '.'.join(labels[-2:]) if len(labels) > 1 else None

    @cached_property
    def _rules(self) -> _Rules:
        try:
            return self._parse_rules(self._download_list())
        except Exception as exc:
            logger.warning(f'Failed to download the Public Suffix List, using the bundled snapshot: {exc!r}')
            return self._parse_rules(PUBLIC_SUFFIX_LIST_SNAPSHOT.read_text(encoding='utf-8'))

    @staticmethod
    def _download_list() -> str:
        response = impit.get(PUBLIC_SUFFIX_LIST_URL, timeout=_DOWNLOAD_TIMEOUT_SECS, follow_redirects=True)
        if not is_status_code_successful(response.status_code):
            raise RuntimeError(f'Unexpected HTTP status code {response.status_code}.')

        return response.content.decode('utf-8')

    @staticmethod
    def _parse_rules(text: str) -> _Rules:
        start = text.find(_ICANN_BEGIN)
        end = text.find(_ICANN_END)
        if start == -1 or end < start:
            raise ValueError('The ICANN section of the Public Suffix List is missing.')

        suffixes: set[str] = set()
        wildcards: set[str] = set()
        exceptions: set[str] = set()
        for line in text[start:end].splitlines():
            rule = line.strip()
            if not rule or rule.startswith('//'):
                continue
            if rule.startswith('!'):
                exceptions.add(rule[1:])
            elif rule.startswith('*.'):
                wildcards.add(rule[2:])
            else:
                suffixes.add(rule)

        return _Rules(frozenset(suffixes), frozenset(wildcards), frozenset(exceptions))

    @staticmethod
    def _is_ip_address(host: str) -> bool:
        try:
            ip_address(host)
        except ValueError:
            return False
        return True


public_suffix_list = PublicSuffixList()
"""The process-wide `PublicSuffixList` instance."""
