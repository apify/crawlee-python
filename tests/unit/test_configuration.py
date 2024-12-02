# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee.configuration import Configuration
from crawlee.http_crawler import HttpCrawler, HttpCrawlingContext
from crawlee.memory_storage_client._memory_storage_client import MemoryStorageClient
from crawlee.service_container import get_configuration, set_storage_client

if TYPE_CHECKING:
    from pathlib import Path

    from yarl import URL


def test_global_configuration_works() -> None:
    assert (
        Configuration.get_global_configuration()
        is Configuration.get_global_configuration()
        is get_configuration()
        is get_configuration()
    )


async def test_storage_not_persisted_when_disabled(tmp_path: Path, httpbin: URL) -> None:
    config = Configuration(
        persist_storage=False,
        write_metadata=False,
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )
    storage_client = MemoryStorageClient(config)
    set_storage_client(storage_client, force=True)

    crawler = HttpCrawler()

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(httpbin)])

    # Verify that no files were created in the storage directory.
    content = list(tmp_path.iterdir())
    assert content == [], 'Expected the storage directory to be empty, but it is not.'


async def test_storage_persisted_when_enabled(tmp_path: Path, httpbin: URL) -> None:
    config = Configuration(
        persist_storage=True,
        write_metadata=True,
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )
    storage_client = MemoryStorageClient(config)
    set_storage_client(storage_client, force=True)

    crawler = HttpCrawler()

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(httpbin)])

    # Verify that files were created in the storage directory.
    content = list(tmp_path.iterdir())
    assert content != [], 'Expected the storage directory to contain files, but it does not.'
