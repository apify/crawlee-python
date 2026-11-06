from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from crawlee._request import Request


class LoggerOnce:
    """Emits each log message at most once, keyed by an explicit string.

    Useful for diagnostic warnings that would otherwise spam the log when the same condition recurs (per-request
    misconfiguration warnings, repeated fallback paths, etc.). Deduplication scope follows the lifetime of the
    instance - a module-level instance gives process-wide dedup; an attribute on a class gives per-instance dedup.

    Safe to call from multiple threads - some callers (e.g. system metric sampling) run in a worker thread.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def log(self, message: str, *, key: str, level: int = logging.INFO) -> None:
        """Log `message` at `level` the first time `key` is seen on this instance; later calls are no-ops.

        Args:
            message: The message to log.
            key: Deduplication key. Two calls with the same key emit at most once.
            level: Standard `logging` level (e.g. `logging.WARNING`). Defaults to `logging.INFO`.
        """
        # The check and the insert have to be atomic, otherwise two threads racing on the same key both emit.
        with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)

        self._logger.log(level, message)


def request_log_extra(request: Request) -> dict[str, Any]:
    """Return the fields that identify `request` in a log record, to pass as `extra`."""
    return {'url': request.url, 'unique_key': request.unique_key, 'retry_count': request.retry_count}
