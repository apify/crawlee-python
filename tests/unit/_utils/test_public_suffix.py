from __future__ import annotations

import logging
from typing import Any
from unittest.mock import ANY, Mock

import impit
import pytest

from crawlee._utils.public_suffix import PUBLIC_SUFFIX_LIST_URL, PublicSuffixList

# A downloaded list that differs from the snapshot: `fresh.com` is a public suffix only here, and `private.com` is
# listed in the private section.
DOWNLOADED_LIST = (
    '// ===BEGIN ICANN DOMAINS===\n'
    '// com : https://www.iana.org/domains/root/db/com.html\n'
    'com\n'
    '\n'
    'fresh.com\n'
    '// ===END ICANN DOMAINS===\n'
    '// ===BEGIN PRIVATE DOMAINS===\n'
    'private.com\n'
    '// ===END PRIVATE DOMAINS===\n'
)


def mock_download(
    monkeypatch: pytest.MonkeyPatch,
    *,
    status_code: int = 200,
    body: str = DOWNLOADED_LIST,
    error: Exception | None = None,
) -> Mock:
    get = Mock(return_value=Mock(status_code=status_code, content=body.encode()), side_effect=error)
    monkeypatch.setattr('crawlee._utils.public_suffix.impit', Mock(get=get))
    return get


@pytest.mark.parametrize(
    ('host', 'expected'),
    [
        pytest.param('www.example.com', 'example.com', id='subdomain'),
        pytest.param('example.com', 'example.com', id='registrable'),
        pytest.param('a.b.example.co.uk', 'example.co.uk', id='multi_label_suffix'),
        pytest.param('co.uk', None, id='public_suffix'),
        pytest.param('a.kawasaki.jp', None, id='wildcard_suffix'),
        pytest.param('b.a.kawasaki.jp', 'b.a.kawasaki.jp', id='wildcard_registrable'),
        pytest.param('foo.city.kawasaki.jp', 'city.kawasaki.jp', id='exception'),
        pytest.param('foo.example.local', 'example.local', id='unknown_tld'),
        pytest.param('www.example.com.', 'example.com', id='trailing_dot'),
        pytest.param('localhost', None, id='single_label'),
        pytest.param('127.0.0.1', None, id='ipv4'),
        pytest.param('::1', None, id='ipv6'),
    ],
)
def test_registrable_domain(host: str, expected: str | None) -> None:
    """Lookups follow the ICANN rules of the bundled snapshot."""
    assert PublicSuffixList().get_registrable_domain(host) == expected


def test_lookup_downloads_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """The first lookup downloads the current list."""
    get = mock_download(monkeypatch)
    assert PublicSuffixList().get_registrable_domain('a.b.fresh.com') == 'b.fresh.com'
    get.assert_called_once_with(PUBLIC_SUFFIX_LIST_URL, timeout=ANY, follow_redirects=True)


def test_ip_skips_download(monkeypatch: pytest.MonkeyPatch) -> None:
    """IP addresses are resolved without loading the list."""
    get = mock_download(monkeypatch)
    assert PublicSuffixList().get_registrable_domain('127.0.0.1') is None
    get.assert_not_called()


def test_private_section_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rules outside the ICANN section of the downloaded list are ignored."""
    mock_download(monkeypatch)
    assert PublicSuffixList().get_registrable_domain('a.b.private.com') == 'private.com'


@pytest.mark.parametrize(
    'download',
    [
        pytest.param({'error': impit.TimeoutException('Request timeout exceeded.')}, id='request_error'),
        pytest.param({'status_code': 503}, id='error_status'),
        pytest.param({'body': '<html>Captive portal</html>'}, id='no_icann_section'),
    ],
)
def test_lookup_falls_back_to_snapshot(
    download: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed download leaves the snapshot in use and logs a warning."""
    mock_download(monkeypatch, **download)
    with caplog.at_level(logging.WARNING):
        assert PublicSuffixList().get_registrable_domain('a.b.fresh.com') == 'fresh.com'

    assert 'using the bundled snapshot' in caplog.text


@pytest.mark.parametrize(
    'download',
    [
        pytest.param({}, id='success'),
        pytest.param({'status_code': 503}, id='failure'),
    ],
)
def test_list_downloaded_once(download: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Later lookups reuse the loaded list, even after a failed download."""
    get = mock_download(monkeypatch, **download)
    public_suffix_list = PublicSuffixList()
    public_suffix_list.get_registrable_domain('www.example.com')
    public_suffix_list.get_registrable_domain('www.example.org')

    get.assert_called_once()
