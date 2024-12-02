from __future__ import annotations

import re
from base64 import b64encode
from hashlib import sha256
from logging import getLogger
from typing import TYPE_CHECKING

from yarl import URL

from crawlee._utils.crypto import compute_short_hash

if TYPE_CHECKING:
    from crawlee._types import HttpHeaders, HttpMethod, HttpPayload

logger = getLogger(__name__)


def unique_key_to_request_id(unique_key: str, *, request_id_length: int = 15) -> str:
    """Generate a deterministic request ID based on a unique key.

    Args:
        unique_key: The unique key to convert into a request ID.
        request_id_length: The length of the request ID.

    Returns:
        A URL-safe, truncated request ID based on the unique key.
    """
    # Encode the unique key and compute its SHA-256 hash
    hashed_key = sha256(unique_key.encode('utf-8')).digest()

    # Encode the hash in base64 and decode it to get a string
    base64_encoded = b64encode(hashed_key).decode('utf-8')

    # Remove characters that are not URL-safe ('+', '/', or '=')
    url_safe_key = re.sub(r'(\+|\/|=)', '', base64_encoded)

    # Truncate the key to the desired length
    return url_safe_key[:request_id_length]


def normalize_url(url: str, *, keep_url_fragment: bool = False) -> str:
    """Normalizes a URL.

    This function cleans and standardizes a URL by removing leading and trailing whitespaces,
    converting the scheme and netloc to lower case, stripping unwanted tracking parameters
    (specifically those beginning with 'utm_'), sorting the remaining query parameters alphabetically,
    and optionally retaining the URL fragment. The goal is to ensure that URLs that are functionally
    identical but differ in trivial ways (such as parameter order or casing) are treated as the same.

    Args:
        url: The URL to be normalized.
        keep_url_fragment: Flag to determine whether the fragment part of the URL should be retained.

    Returns:
        A string containing the normalized URL.
    """
    # Parse the URL
    parsed_url = URL(url.strip())

    # Remove any 'utm_' parameters
    search_params = [(k, v) for k, v in parsed_url.query.items() if not k.startswith('utm_')]

    # Construct the new query string
    sorted_search_params = sorted(search_params)

    # Construct the final URL
    yarl_new_url = parsed_url.with_query(sorted_search_params)
    yarl_new_url = yarl_new_url.with_path(
        yarl_new_url.path.removesuffix('/'), keep_query=True, keep_fragment=keep_url_fragment
    )

    return str(yarl_new_url).lower()


def compute_unique_key(
    url: str,
    method: HttpMethod = 'GET',
    headers: HttpHeaders | None = None,
    payload: HttpPayload | None = None,
    *,
    keep_url_fragment: bool = False,
    use_extended_unique_key: bool = False,
) -> str:
    """Compute a unique key for caching & deduplication of requests.

    This function computes a unique key by normalizing the provided URL and method. If `use_extended_unique_key`
    is True and a payload is provided, the payload is hashed and included in the key. Otherwise, the unique key
    is just the normalized URL. Additionally, if HTTP headers are provided, the whitelisted headers are hashed
    and included in the key.

    Args:
        url: The request URL.
        method: The HTTP method.
        headers: The HTTP headers.
        payload: The data to be sent as the request body.
        keep_url_fragment: A flag indicating whether to keep the URL fragment.
        use_extended_unique_key: A flag indicating whether to include a hashed payload in the key.

    Returns:
        A string representing the unique key for the request.
    """
    # Normalize the URL.
    try:
        normalized_url = normalize_url(url, keep_url_fragment=keep_url_fragment)
    except Exception as exc:
        logger.warning(f'Failed to normalize URL: {exc}')
        normalized_url = url

    # Normalize the method.
    normalized_method = method.upper()

    # Compute and return the extended unique key if required.
    if use_extended_unique_key:
        payload_hash = _get_payload_hash(payload)
        headers_hash = _get_headers_hash(headers)

        # Return the extended unique key. Use pipe as a separator of the different parts of the unique key.
        return f'{normalized_method}|{headers_hash}|{payload_hash}|{normalized_url}'

    # Log information if there is a non-GET request with a payload.
    if normalized_method != 'GET' and payload:
        logger.info(
            f'{normalized_method} request with a payload detected. By default, requests to the same URL with '
            'different methods or payloads will be deduplicated. Use "use_extended_unique_key" to include payload '
            'and headers in the unique key and avoid deduplication in these cases.'
        )

    # Return the normalized URL as the unique key.
    return normalized_url


def _get_payload_hash(payload: HttpPayload | None) -> str:
    payload_in_bytes = b'' if payload is None else payload
    return compute_short_hash(payload_in_bytes)


def _get_headers_hash(headers: HttpHeaders | None) -> str:
    # HTTP headers which will be included in the hash computation.
    whitelisted_headers = {'accept', 'accept-language', 'authorization', 'content-type'}

    if headers is None:
        normalized_headers = b''
    else:
        filtered_headers = {key: value for key, value in headers.items() if key in whitelisted_headers}
        normalized_headers = '|'.join(f'{k}:{v}' for k, v in filtered_headers.items()).encode('utf-8')

    return compute_short_hash(normalized_headers)
