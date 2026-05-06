from __future__ import annotations

import logging


class LoggerOnce:
    """Emits each log message at most once, keyed by an explicit string.

    Useful for diagnostic warnings that would otherwise spam the log when the same condition recurs (per-request
    misconfiguration warnings, repeated fallback paths, etc.). Deduplication scope follows the lifetime of the
    instance — a module-level instance gives process-wide dedup; an attribute on a class gives per-instance dedup.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._seen: set[str] = set()

    def log(self, message: str, *, key: str, level: int = logging.INFO) -> None:
        """Log `message` at `level` the first time `key` is seen on this instance; later calls are no-ops.

        Args:
            message: The message to log.
            key: Deduplication key. Two calls with the same key emit at most once.
            level: Standard `logging` level (e.g. `logging.WARNING`). Defaults to `logging.INFO`.
        """
        if key in self._seen:
            return
        self._seen.add(key)
        self._logger.log(level, message)
