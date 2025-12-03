from __future__ import annotations

import pytest

from crawlee._types import HttpHeaders
from crawlee._utils.requests import compute_unique_key, normalize_url


@pytest.mark.parametrize(
    ('url', 'expected_output', 'keep_url_fragment'),
    [
        ('https://example.com/?utm_source=test&utm_medium=test&key=value', 'https://example.com/?key=value', False),
        (
            'http://example.com/?key=value&another_key=another_value',
            'http://example.com/?another_key=another_value&key=value',
            False,
        ),
        ('HTTPS://EXAMPLE.COM/?KEY=VALUE', 'https://example.com/?key=value', False),
        ('', '', False),
        ('http://example.com/#fragment', 'http://example.com/#fragment', True),
        ('http://example.com/#fragment', 'http://example.com', False),
        ('  https://example.com/  ', 'https://example.com', False),
        ('http://example.com/?b=2&a=1', 'http://example.com/?a=1&b=2', False),
    ],
    ids=[
        'remove_utm_params',
        'retain_sort_non_utm_params',
        'convert_scheme_netloc_to_lowercase',
        'handle_empty_url',
        'retain_fragment',
        'remove_fragment',
        'trim_whitespace',
        'sort_query_params',
    ],
)
def test_normalize_url(url: str, expected_output: str, *, keep_url_fragment: bool) -> None:
    output = normalize_url(url, keep_url_fragment=keep_url_fragment)
    assert output == expected_output


def test_compute_unique_key_basic() -> None:
    url = 'https://crawlee.dev'
    uk_get = compute_unique_key(url, method='GET')
    uk_post = compute_unique_key(url, method='POST')
    assert url == uk_get == uk_post


def test_compute_unique_key_handles_fragments() -> None:
    url = 'https://crawlee.dev/#fragment'
    uk_with_fragment = compute_unique_key(url, keep_url_fragment=True)
    assert uk_with_fragment == url

    uk_without_fragment = compute_unique_key(url, 'GET', keep_url_fragment=False)
    assert uk_without_fragment == 'https://crawlee.dev'


def test_compute_unique_key_handles_payload() -> None:
    url = 'https://crawlee.dev'
    payload = b'{"key": "value"}'

    # Payload without extended unique key
    uk = compute_unique_key(url, method='POST', payload=payload, use_extended_unique_key=False)
    assert uk == url

    # Extended unique key and payload is None
    uk = compute_unique_key(url, method='POST', payload=None, use_extended_unique_key=True)
    assert uk == 'POST|e3b0c442|e3b0c442|https://crawlee.dev'

    # Extended unique key and payload is bytes
    uk = compute_unique_key(url, method='POST', payload=payload, use_extended_unique_key=True)
    assert uk == 'POST|e3b0c442|9724c1e2|https://crawlee.dev'


def test_compute_unique_key_handles_headers() -> None:
    url = 'https://crawlee.dev'
    headers = HttpHeaders({'Accept': '*/*', 'Content-Type': 'application/json'})
    uk = compute_unique_key(url, headers=headers, use_extended_unique_key=False)
    assert uk == url

    extended_uk_expected = 'GET|4e1a2cf6|e3b0c442|https://crawlee.dev'

    uk = compute_unique_key(url, headers=headers, use_extended_unique_key=True)
    assert uk == extended_uk_expected

    # Accept-Encoding header should not be included.
    headers = HttpHeaders({'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate', 'Content-Type': 'application/json'})
    uk = compute_unique_key(url, headers=headers, use_extended_unique_key=True)
    assert uk == extended_uk_expected


def test_compute_unique_key_complex() -> None:
    url = 'https://crawlee.dev'
    headers = HttpHeaders({'Accept': '*/*', 'Content-Type': 'application/json'})
    payload = b'{"key": "value"}'

    uk = compute_unique_key(
        url,
        method='POST',
        headers=headers,
        payload=payload,
        session_id='test_session',
        use_extended_unique_key=False,
    )
    assert uk == url

    extended_uk = compute_unique_key(
        url,
        method='POST',
        headers=headers,
        payload=payload,
        session_id='test_session',
        use_extended_unique_key=True,
    )
    assert extended_uk == 'POST|4e1a2cf6|9724c1e2|test_session|https://crawlee.dev'


def test_compute_unique_key_post_with_none_payload() -> None:
    url = 'https://crawlee.dev'
    expected_output = 'POST|e3b0c442|e3b0c442|https://crawlee.dev'
    output = compute_unique_key(url, 'POST', payload=None, use_extended_unique_key=True)
    assert output == expected_output


def test_compute_unique_key_with_whitespace_in_headers() -> None:
    url = 'https://crawlee.dev'
    headers = HttpHeaders({'Content-Type': 'application/json'})
    headers_with_whitespaces = HttpHeaders({'Content-Type': ' application/json '})

    expected_output = 'GET|60d83e70|e3b0c442|https://crawlee.dev'
    uk_1 = compute_unique_key(url, headers=headers, use_extended_unique_key=True)
    assert uk_1 == expected_output

    uk_2 = compute_unique_key(url, headers=headers_with_whitespaces, use_extended_unique_key=True)
    assert uk_2 == expected_output
