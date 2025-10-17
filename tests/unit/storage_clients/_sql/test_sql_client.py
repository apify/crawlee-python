from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from crawlee.configuration import Configuration
from crawlee.storage_clients import SqlStorageClient

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    """Temporary configuration for tests."""
    return Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
    )


@pytest.mark.parametrize(
    ('connection_parameter'),
    [pytest.param('connection_string', id='with connection string'), pytest.param('engine', id='with engine')],
)
async def test_deepcopy_with_engine_init(
    configuration: Configuration, tmp_path: Path, connection_parameter: str
) -> None:
    """Test that SQL dataset client creates tables with a connection string."""
    storage_dir = tmp_path / 'test_table.db'
    connection_string = f'sqlite+aiosqlite:///{storage_dir}'
    sql_kwargs: dict[str, Any] = {}
    if connection_parameter == 'connection_string':
        sql_kwargs['connection_string'] = connection_string
    else:
        engine = create_async_engine(connection_string, future=True, echo=False)
        sql_kwargs['engine'] = engine
    async with SqlStorageClient(**sql_kwargs) as storage_client:
        copy_storage_client = deepcopy(storage_client)

        # Ensure that the copy is a new instance
        assert copy_storage_client is not storage_client
        # Ensure that the copy uses the same engine
        assert copy_storage_client._engine is storage_client._engine

        # Ensure that the copy can create a new dataset client
        copy_dataset_client = await copy_storage_client.create_dataset_client(
            configuration=configuration, name='new-dataset'
        )
        dataset_client = await storage_client.create_dataset_client(configuration=configuration, name='new-dataset')

        # Ensure that the metadata from both clients is the same
        copy_metadata = await copy_dataset_client.get_metadata()
        storage_metadata = await dataset_client.get_metadata()
        assert copy_metadata == storage_metadata
