from __future__ import annotations

import pytest

from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.fingerprint_suite._browserforge_adapter import get_user_agent_pool
from crawlee.fingerprint_suite._consts import (
    BROWSER_TYPE_HEADER_KEYWORD,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_MOBILE,
    PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_PLATFORM,
)
from crawlee.fingerprint_suite._types import SupportedBrowserType

@pytest.fixture(scope="session")
def user_agents_pool():
    return get_user_agent_pool()

def test_get_common_headers() -> None:
    header_generator = HeaderGenerator()
    headers = header_generator.get_common_headers()

    assert 'Accept' in headers
    assert 'Accept-Language' in headers


def test_get_random_user_agent_header() -> None:
    """Test that a random User-Agent header is generated."""
    header_generator = HeaderGenerator()
    headers = header_generator.get_random_user_agent_header()

    assert 'User-Agent' in headers
    assert headers['User-Agent']



@pytest.mark.parametrize('_', range(100))
@pytest.mark.parametrize('browser_type', [
    'chromium','firefox','edge','webkit'
])
def test_get_user_agent_header_stress_test(browser_type: SupportedBrowserType,user_agents_pool,_) -> None:
    """Test that the User-Agent header is consistently generated correctly.

    (Very fast even when stress tested.)"""
    header_generator = HeaderGenerator()
    headers = header_generator.get_user_agent_header(browser_type=browser_type)

    assert 'User-Agent' in headers
    assert any(keyword in headers['User-Agent'] for keyword in BROWSER_TYPE_HEADER_KEYWORD[browser_type])
    assert headers['User-Agent'] in user_agents_pool


def test_get_user_agent_header_invalid_browser_type() -> None:
    """Test that an invalid browser type raises a ValueError."""
    header_generator = HeaderGenerator()

    with pytest.raises(ValueError, match='Unsupported browser type'):
        header_generator.get_user_agent_header(browser_type='invalid_browser')  # type: ignore[arg-type]


def test_get_sec_ch_ua_headers_chromium() -> None:
    """Test that Sec-Ch-Ua headers are generated correctly for Chromium."""
    header_generator = HeaderGenerator()
    headers = header_generator.get_sec_ch_ua_headers(browser_type='chromium')

    assert 'Sec-Ch-Ua' in headers
    assert headers['Sec-Ch-Ua'] == PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA
    assert 'Sec-Ch-Ua-Mobile' in headers
    assert headers['Sec-Ch-Ua-Mobile'] == PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_MOBILE
    assert 'Sec-Ch-Ua-Platform' in headers
    assert headers['Sec-Ch-Ua-Platform'] == PW_CHROMIUM_HEADLESS_DEFAULT_SEC_CH_UA_PLATFORM


def test_get_sec_ch_ua_headers_firefox() -> None:
    """Test that Sec-Ch-Ua headers are not generated for Firefox."""
    header_generator = HeaderGenerator()
    headers = header_generator.get_sec_ch_ua_headers(browser_type='firefox')

    assert not headers


def test_get_sec_ch_ua_headers_webkit() -> None:
    """Test that Sec-Ch-Ua headers are not generated for WebKit."""
    header_generator = HeaderGenerator()
    headers = header_generator.get_sec_ch_ua_headers(browser_type='webkit')

    assert not headers


def test_get_sec_ch_ua_headers_invalid_browser_type() -> None:
    """Test that an invalid browser type raises a ValueError for Sec-Ch-Ua headers."""
    header_generator = HeaderGenerator()

    with pytest.raises(ValueError, match='Unsupported browser type'):
        header_generator.get_sec_ch_ua_headers(browser_type='invalid_browser')  # type: ignore[arg-type]
