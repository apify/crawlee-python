from __future__ import annotations

import hashlib
import re
import string
from typing import TYPE_CHECKING

from crawlee.storages import KeyValueStore

if TYPE_CHECKING:
    from crawlee._types import BasicCrawlingContext


class ErrorSnapshotter:
    MAX_ERROR_CHARACTERS = 30
    MAX_HASH_LENGTH = 30
    MAX_FILENAME_LENGTH = 250
    BASE_MESSAGE = 'An error occurred'
    SNAPSHOT_PREFIX = 'ERROR_SNAPSHOT'
    ALLOWED_CHARACTERS = string.ascii_letters + string.digits + '!-_.'

    def __init__(self, *, snapshot_kvs_name: str | None = None) -> None:
        self._kvs_name = snapshot_kvs_name

    async def capture_snapshot(
        self,
        error_message: str,
        file_and_line: str,
        context: BasicCrawlingContext,
    ) -> None:
        """Capture error snapshot and save it to key value store."""
        snapshot = await context.get_snapshot()
        if not snapshot:
            return

        base = self._get_snapshot_base_name(error_message, file_and_line)
        kvs = await KeyValueStore.open(name=self._kvs_name)

        # Save HTML snapshot if present
        if snapshot.html:
            key_html = f'{base}.html'
            await kvs.set_value(key_html, snapshot.html, content_type='text/html')

        # Save screenshot snapshot if present
        if snapshot.screenshot:
            key_jpg = f'{base}.jpg'
            await kvs.set_value(key_jpg, snapshot.screenshot, content_type='image/jpeg')

    def _sanitize_filename(self, filename: str) -> str:
        return re.sub(f'[^{re.escape(self.ALLOWED_CHARACTERS)}]', '', filename[: self.MAX_FILENAME_LENGTH])

    def _get_snapshot_base_name(self, error_message: str, file_and_line: str) -> str:
        sha1_hash = hashlib.sha1()  # noqa:S324 # Collisions related attacks are of no concern here.
        sha1_hash.update(file_and_line.encode('utf-8'))
        hashed_file_and_text = sha1_hash.hexdigest()[: self.MAX_HASH_LENGTH]
        error_message_start = (error_message or self.BASE_MESSAGE)[: self.MAX_ERROR_CHARACTERS]
        return self._sanitize_filename(f'{self.SNAPSHOT_PREFIX}_{hashed_file_and_text}_{error_message_start}')
