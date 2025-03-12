from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.fingerprint_suite._browserforge_adapter import get_available_header_values
from crawlee.fingerprint_suite._consts import (
    BROWSER_TYPE_HEADER_KEYWORD,
)

if TYPE_CHECKING:
    from crawlee.fingerprint_suite._types import SupportedBrowserType


def test_get_common_headers(header_network: dict) -> None:
    header_generator = HeaderGenerator()
    headers = header_generator.get_common_headers()

    assert 'Accept' in headers
    assert headers['Accept'] in get_available_header_values(header_network, {'Accept', 'accept'})
    assert 'Accept-Language' in headers


def test_get_random_user_agent_header() -> None:
    """Test that a random User-Agent header is generated."""
    header_generator = HeaderGenerator()
    headers = header_generator.get_random_user_agent_header()

    assert 'User-Agent' in headers
    assert headers['User-Agent']


@pytest.mark.parametrize('browser_type', ['chrome', 'firefox', 'edge', 'safari'])
def test_get_user_agent_header_stress_test(browser_type: SupportedBrowserType, header_network: dict) -> None:
    """Test that the User-Agent header is consistently generated correctly.

    (Very fast even when stress tested.)"""
    for _ in range(100):
        header_generator = HeaderGenerator()
        headers = header_generator.get_user_agent_header(browser_type=browser_type)

        assert 'User-Agent' in headers
        assert any(keyword in headers['User-Agent'] for keyword in BROWSER_TYPE_HEADER_KEYWORD[browser_type])
        assert headers['User-Agent'] in get_available_header_values(header_network, {'user-agent', 'User-Agent'})


def test_get_user_agent_header_invalid_browser_type() -> None:
    """Test that an invalid browser type raises a ValueError."""
    header_generator = HeaderGenerator()

    with pytest.raises(ValueError, match='Unsupported browser type'):
        header_generator.get_user_agent_header(browser_type='invalid_browser')  # type: ignore[arg-type]


def test_get_sec_ch_ua_headers_chromium(header_network: dict) -> None:
    """Test that Sec-Ch-Ua headers are generated correctly for Chrome."""
    header_generator = HeaderGenerator()
    headers = header_generator.get_sec_ch_ua_headers(browser_type='chrome')

    assert headers.get('sec-ch-ua') in get_available_header_values(header_network, 'sec-ch-ua')
    assert headers.get('sec-ch-ua-mobile') in get_available_header_values(header_network, 'sec-ch-ua-mobile')
    assert headers.get('sec-ch-ua-platform') in get_available_header_values(header_network, 'sec-ch-ua-platform')


def test_get_sec_ch_ua_headers_firefox() -> None:
    """Test that sec-ch-ua headers are not generated for Firefox."""
    header_generator = HeaderGenerator()
    headers = header_generator.get_sec_ch_ua_headers(browser_type='firefox')

    assert not headers


def test_get_sec_ch_ua_headers_invalid_browser_type() -> None:
    """Test that an invalid browser type raises a ValueError for sec-ch-ua headers."""
    header_generator = HeaderGenerator()

    with pytest.raises(ValueError, match='Unsupported browser type'):
        header_generator.get_sec_ch_ua_headers(browser_type='invalid_browser')  # type: ignore[arg-type]
