import hashlib

from pathvalidate import sanitize_filename

from crawlee._types import BasicCrawlingContext
from crawlee.storages import KeyValueStore


class ErrorSnapshotter:
    MAX_ERROR_CHARACTERS = 30
    MAX_HASH_LENGTH = 30
    MAX_FILENAME_LENGTH = 250
    BASE_MESSAGE = 'An error occurred'
    SNAPSHOT_PREFIX = 'ERROR_SNAPSHOT'

    async def capture_snapshot(self, error_message: str, file_and_line: str, context: BasicCrawlingContext, kvs: KeyValueStore) ->None:
        """Capture error snapshot.

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
        if context.get_snapshot:
            snapshot = await context.get_snapshot()
            snapshot_base_name = self._get_snapshot_base_name(error_message, file_and_line)
            if snapshot.html:
                await self._save_html(snapshot.html, base_name=snapshot_base_name, kvs = kvs)
            if snapshot.screenshot:
                await self._save_screenshot(snapshot.screenshot, base_name=snapshot_base_name, kvs = kvs)

    async def _save_html(self, html: str, base_name :str, kvs: KeyValueStore)->None:
        file_name = f'{base_name}.html'
        await kvs.set_value(file_name, html, content_type='text/html')


    async def _save_screenshot(self, screenshot: bytes, base_name: str, kvs: KeyValueStore)->None:
        file_name = f'{base_name}.jpg'
        await kvs.set_value(file_name, screenshot, content_type='image/jpeg')


    def _get_snapshot_base_name(self, error_message: str, file_and_line: str) -> str:
        sha1_hash = hashlib.sha1()
        sha1_hash.update(file_and_line.encode('utf-8'))
        hashed_file_and_text = sha1_hash.hexdigest()
        error_message_start = (error_message or self.BASE_MESSAGE)[:self.MAX_ERROR_CHARACTERS]
        return sanitize_filename(f'{self.SNAPSHOT_PREFIX}_{hashed_file_and_text}_{error_message_start}', max_len=self.MAX_FILENAME_LENGTH)
