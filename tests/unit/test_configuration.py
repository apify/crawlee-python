from __future__ import annotations

from typing import TYPE_CHECKING

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.statistics import Statistics
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storage_clients._file_system._storage_client import FileSystemStorageClient

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


async def test_storage_not_persisted_when_non_persistable_storage_used(tmp_path: Path, server_url: URL) -> None:
    """Make the Crawler use MemoryStorageClient which can't persist state."""
    service_locator.set_configuration(Configuration(storage_dir=str(tmp_path)))
    crawler = HttpCrawler(storage_client=MemoryStorageClient())

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(server_url)])

    # Verify that no files were created in the storage directory.
    content = list(tmp_path.iterdir())
    assert content == [], 'Expected the storage directory to be empty, but it is not.'


async def test_storage_persisted_with_explicit_statistics_with_persistable_storage(
    tmp_path: Path, server_url: URL
) -> None:
    """Make the Crawler use MemoryStorageClient which can't persist state,
    but pass explicit statistics to it which will use global FileSystemStorageClient() that can persist state."""

    configuration = Configuration(storage_dir=str(tmp_path))
    service_locator.set_configuration(configuration)
    service_locator.set_storage_client(FileSystemStorageClient())

    crawler = HttpCrawler(
        storage_client=MemoryStorageClient(), statistics=Statistics.with_default_state(persistence_enabled=True)
    )

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(server_url)])

    # Verify that files were created in the storage directory.
    content = list(tmp_path.iterdir())
    assert content != [], 'Expected the storage directory to contain files, but it does not.'


async def test_storage_persisted_when_enabled(tmp_path: Path, server_url: URL) -> None:
    configuration = Configuration(
        storage_dir=str(tmp_path),
    )

    storage_client = FileSystemStorageClient()

    crawler = HttpCrawler(
        configuration=configuration,
        storage_client=storage_client,
    )

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(server_url)])

    # Verify that files were created in the storage directory.
    content = list(tmp_path.iterdir())
    assert content != [], 'Expected the storage directory to contain files, but it does not.'
