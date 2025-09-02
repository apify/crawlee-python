import asyncio
from datetime import timedelta

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset


async def main() -> None:
    configuration = Configuration(
        log_level='DEBUG',
        headless=False,
        persist_state_interval=timedelta(seconds=30),
    )
    # Set the custom configuration as the global default configuration.
    service_locator.set_configuration(configuration)

    # Use the global defaults when creating the dataset (or other storage).
    dataset_1 = await Dataset.open()

    # Or set explicitly specific configuration if
    # you do not want to rely on global defaults.
    dataset_2 = await Dataset.open(
        storage_client=MemoryStorageClient(), configuration=configuration
    )


if __name__ == '__main__':
    asyncio.run(main())
