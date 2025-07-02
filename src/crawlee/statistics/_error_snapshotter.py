from __future__ import annotations

import asyncio
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
        """Capture error snapshot and save it to key value store.

        It saves the error snapshot directly to a key value store. It can't use `context.get_key_value_store` because
        it returns `KeyValueStoreChangeRecords` which is commited to the key value store only if the `RequestHandler`
        returned without an exception. ErrorSnapshotter is on the contrary active only when `RequestHandler` fails with
        an exception.

        Args:
            error_message: Used in filename of the snapshot.
            file_and_line: Used in filename of the snapshot.
            context: Context that is used to get the snapshot.
        """
        if snapshot := await context.get_snapshot():
            kvs = await KeyValueStore.open(name=self._kvs_name)
            snapshot_base_name = self._get_snapshot_base_name(error_message, file_and_line)
            snapshot_save_tasks = list[asyncio.Task]()

            if snapshot.html:
                snapshot_save_tasks.append(
                    asyncio.create_task(self._save_html(kvs, snapshot.html, base_name=snapshot_base_name))
                )

            if snapshot.screenshot:
                snapshot_save_tasks.append(
                    asyncio.create_task(self._save_screenshot(kvs, snapshot.screenshot, base_name=snapshot_base_name))
                )

            await asyncio.gather(*snapshot_save_tasks)

    async def _save_html(self, kvs: KeyValueStore, html: str, base_name: str) -> None:
        file_name = f'{base_name}.html'
        await kvs.set_value(file_name, html, content_type='text/html')

    async def _save_screenshot(self, kvs: KeyValueStore, screenshot: bytes, base_name: str) -> None:
        file_name = f'{base_name}.jpg'
        await kvs.set_value(file_name, screenshot, content_type='image/jpeg')

    def _sanitize_filename(self, filename: str) -> str:
        return re.sub(f'[^{re.escape(self.ALLOWED_CHARACTERS)}]', '', filename[: self.MAX_FILENAME_LENGTH])

    def _get_snapshot_base_name(self, error_message: str, file_and_line: str) -> str:
        sha1_hash = hashlib.sha1()  # noqa:S324 # Collisions related attacks are of no concern here.
        sha1_hash.update(file_and_line.encode('utf-8'))
        hashed_file_and_text = sha1_hash.hexdigest()[: self.MAX_HASH_LENGTH]
        error_message_start = (error_message or self.BASE_MESSAGE)[: self.MAX_ERROR_CHARACTERS]
        return self._sanitize_filename(f'{self.SNAPSHOT_PREFIX}_{hashed_file_and_text}_{error_message_start}')
