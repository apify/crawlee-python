# TODO: Update crawlee_storage_dir args once the Pydantic bug is fixed
# https://github.com/apify/crawlee-python/issues/146

from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.storage_clients import MemoryStorageClient

if TYPE_CHECKING:
    from pathlib import Path

    from yarl import URL


def test_global_configuration_works() -> None:
    assert (
        Configuration.get_global_configuration()
        is Configuration.get_global_configuration()
        is service_locator.get_configuration()
        is service_locator.get_configuration()
    )


def test_global_configuration_works_reversed() -> None:
    assert (
        service_locator.get_configuration()
        is service_locator.get_configuration()
        is Configuration.get_global_configuration()
        is Configuration.get_global_configuration()
    )


async def test_storage_not_persisted_when_disabled(tmp_path: Path, httpbin: URL) -> None:
    config = Configuration(
        persist_storage=False,
        write_metadata=False,
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )
    storage_client = MemoryStorageClient.from_config(config)

    crawler = HttpCrawler(storage_client=storage_client)

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
    storage_client = MemoryStorageClient.from_config(config)

    crawler = HttpCrawler(storage_client=storage_client)

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(httpbin)])

    # Verify that files were created in the storage directory.
    content = list(tmp_path.iterdir())
    assert content != [], 'Expected the storage directory to contain files, but it does not.'
