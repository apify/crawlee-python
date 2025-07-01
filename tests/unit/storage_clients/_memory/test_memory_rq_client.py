from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from crawlee import Request
from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.storage_clients._memory import MemoryRequestQueueClient


@pytest.fixture
async def rq_client() -> AsyncGenerator[MemoryRequestQueueClient, None]:
    """Fixture that provides a fresh memory request queue client for each test."""
    client = await MemoryStorageClient().create_rq_client(name='test_rq')
    yield client
    await client.drop()


async def test_memory_specific_purge_behavior() -> None:
    """Test memory-specific purge behavior and in-memory storage characteristics."""
    configuration = Configuration(purge_on_start=True)

    # Create RQ and add data
    rq_client1 = await MemoryStorageClient().create_rq_client(
        name='test_purge_rq',
        configuration=configuration,
    )
    request = Request.from_url(url='https://example.com/initial')
    await rq_client1.add_batch_of_requests([request])

    # Verify request was added
    assert await rq_client1.is_empty() is False

    # Reopen with same storage client instance
    rq_client2 = await MemoryStorageClient().create_rq_client(
        name='test_purge_rq',
        configuration=configuration,
    )

    # Verify queue was purged (memory storage specific behavior)
    assert await rq_client2.is_empty() is True


async def test_memory_metadata_updates(rq_client: MemoryRequestQueueClient) -> None:
    """Test that metadata timestamps are updated correctly in memory storage."""
    # Record initial timestamps
    metadata = await rq_client.get_metadata()
    initial_created = metadata.created_at
    initial_accessed = metadata.accessed_at
    initial_modified = metadata.modified_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a read operation
    await rq_client.is_empty()

    # Verify timestamps (memory-specific behavior)
    metadata = await rq_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.accessed_at > initial_accessed
    assert metadata.modified_at == initial_modified

    accessed_after_read = metadata.accessed_at

    # Wait a moment to ensure timestamps can change
    await asyncio.sleep(0.01)

    # Perform a write operation
    await rq_client.add_batch_of_requests([Request.from_url('https://example.com')])

    # Verify timestamps were updated
    metadata = await rq_client.get_metadata()
    assert metadata.created_at == initial_created
    assert metadata.modified_at > initial_modified
    assert metadata.accessed_at > accessed_after_read
