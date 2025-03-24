from crawlee._types import BasicCrawlingContext


class ErrorSnapshotter:
    MAX_ERROR_CHARACTERS = 30
    MAX_HASH_LENGTH = 30
    MAX_FILENAME_LENGTH = 250
    BASE_MESSAGE = 'An error occurred'
    SNAPSHOT_PREFIX = 'ERROR_SNAPSHOT'

    def capture_snapshot(self, error: Exception, context: BasicCrawlingContext) ->None:
        if context.get_snapshot:
            snapshot = context.get_snapshot()
            if snapshot.html:
                self._save_html(snapshot.html)
            if snapshot.screenshot:
                self._save_screenshot(snapshot.screenshot)

    def _save_html(self, html: str)->None:
        pass

    def _save_screenshot(self, screenshot: bytes)->None:
        pass
