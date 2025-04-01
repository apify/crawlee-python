import asyncio
import hashlib
import re
import string

from crawlee._types import BasicCrawlingContext
from crawlee.storages import KeyValueStore


class ErrorSnapshotter:
    MAX_ERROR_CHARACTERS = 30
    MAX_HASH_LENGTH = 30
    MAX_FILENAME_LENGTH = 250
    BASE_MESSAGE = 'An error occurred'
    SNAPSHOT_PREFIX = 'ERROR_SNAPSHOT'
    ALLOWED_CHARACTERS = string.ascii_letters + string.digits + '!-_.'

    async def capture_snapshot(
        self, error_message: str, file_and_line: str, context: BasicCrawlingContext, kvs: KeyValueStore
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
            kvs: Key value store used to save the collected snapshot.
        """
        if snapshot := await context.get_snapshot():
            snapshot_base_name = self._get_snapshot_base_name(error_message, file_and_line)
            snapshot_save_tasks = []
            if snapshot.html:
                snapshot_save_tasks.append(
                    asyncio.create_task(self._save_html(snapshot.html, base_name=snapshot_base_name, kvs=kvs))
                )
            if snapshot.screenshot:
                snapshot_save_tasks.append(
                    asyncio.create_task(
                        self._save_screenshot(snapshot.screenshot, base_name=snapshot_base_name, kvs=kvs)
                    )
                )
            await asyncio.gather(*snapshot_save_tasks)

    @staticmethod
    async def _save_html(html: str, base_name: str, kvs: KeyValueStore) -> None:
        file_name = f'{base_name}.html'
        await kvs.set_value(file_name, html, content_type='text/html')

    @staticmethod
    async def _save_screenshot(screenshot: bytes, base_name: str, kvs: KeyValueStore) -> None:
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
