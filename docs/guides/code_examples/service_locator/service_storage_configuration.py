import asyncio
from datetime import timedelta

from crawlee.configuration import Configuration
from crawlee.storages import Dataset


async def main() -> None:
    configuration = Configuration(
        log_level='DEBUG',
        headless=False,
        persist_state_interval=timedelta(seconds=30),
    )

    # Pass the configuration to the dataset (or other storage) when opening it.
    dataset = await Dataset.open(
        configuration=configuration,
    )


if __name__ == '__main__':
    asyncio.run(main())
