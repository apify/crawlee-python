from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.http_clients import CurlImpersonateHttpClient
from crawlee.sessions import Session

if TYPE_CHECKING:
    from yarl import URL


async def test_internal_session_cookie_jar_stays_empty(server_url: URL) -> None:
    """Test that response cookies never accumulate in the jar of the internally cached `curl_cffi` session."""
    session = Session()
    client = CurlImpersonateHttpClient()

    async with client:
        for index in range(5):
            url = server_url.with_path('set_cookies').extend_query(**{f'c{index}': index})
            await client.send_request(str(url), session=session)

        curl_session = client._client_by_proxy_url[None]
        assert list(curl_session._cookies.jar) == []

    # The cookies are read from the `curl` handle, so they still reach the crawlee session.
    assert {cookie['name'] for cookie in session.cookies} == {f'c{index}' for index in range(5)}
