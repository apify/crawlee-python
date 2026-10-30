from __future__ import annotations

from typing import TYPE_CHECKING

from anyio import Path as AnyioPath

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.statistics import Statistics
from crawlee.storage_backends import MemoryStorageBackend
from crawlee.storage_backends._file_system._storage_backend import FileSystemStorageBackend

if TYPE_CHECKING:
    from pathlib import Path

    from yarl import URL


async def test_storage_not_persisted_when_non_persistable_storage_used(tmp_path: Path, server_url: URL) -> None:
    """Make the Crawler use MemoryStorageBackend which can't persist state."""
    service_locator.set_configuration(Configuration(storage_dir=str(tmp_path)))
    crawler = HttpCrawler(storage_backend=MemoryStorageBackend())

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(server_url)])

    # Verify that no files were created in the storage directory.
    content = [path async for path in AnyioPath(tmp_path).iterdir()]
    assert content == [], 'Expected the storage directory to be empty, but it is not.'


async def test_storage_persisted_with_explicit_statistics_with_persistable_storage(
    tmp_path: Path, server_url: URL
) -> None:
    """Make the Crawler use MemoryStorageBackend which can't persist state,
    but pass explicit statistics to it which will use global FileSystemStorageBackend() that can persist state."""

    configuration = Configuration(storage_dir=str(tmp_path))
    service_locator.set_configuration(configuration)
    service_locator.set_storage_backend(FileSystemStorageBackend())

    crawler = HttpCrawler(
        storage_backend=MemoryStorageBackend(), statistics=Statistics.with_default_state(persistence_enabled=True)
    )

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(server_url)])

    # Verify that files were created in the storage directory.
    content = [path async for path in AnyioPath(tmp_path).iterdir()]
    assert content != [], 'Expected the storage directory to contain files, but it does not.'


async def test_storage_persisted_when_enabled(tmp_path: Path, server_url: URL) -> None:
    configuration = Configuration(
        storage_dir=str(tmp_path),
    )

    storage_backend = FileSystemStorageBackend()

    crawler = HttpCrawler(
        configuration=configuration,
        storage_backend=storage_backend,
    )

    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        await context.push_data({'url': context.request.url})

    await crawler.run([str(server_url)])

    # Verify that files were created in the storage directory.
    content = [path async for path in AnyioPath(tmp_path).iterdir()]
    assert content != [], 'Expected the storage directory to contain files, but it does not.'
