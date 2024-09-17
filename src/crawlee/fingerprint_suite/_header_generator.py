from __future__ import annotations

import random
from typing import TYPE_CHECKING

from crawlee.fingerprint_suite._consts import COMMON_ACCEPT, COMMON_ACCEPT_LANGUAGE, USER_AGENT_POOL

if TYPE_CHECKING:
    from collections.abc import Mapping


class HeaderGenerator:
    """Generates common headers for HTTP requests."""

    def get_common_headers(self) -> Mapping[str, str]:
        """Get common headers for HTTP requests.

        We do not modify the 'Accept-Encoding', 'Connection' and other headers. They should be included and handled
        by the HTTP client.

        Returns:
            Dictionary containing common headers.
        """
        return {
            'Accept': COMMON_ACCEPT,
            'Accept-Language': COMMON_ACCEPT_LANGUAGE,
            'User-Agent': random.choice(USER_AGENT_POOL),
        }
