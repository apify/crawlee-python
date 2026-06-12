import asyncio

from crawlee.storages import Dataset


async def main() -> None:
    # Named storage (persists across runs)
    dataset_named = await Dataset.open(name='my-persistent-dataset')

    # Unnamed storage with alias (purged on start)
    dataset_unnamed = await Dataset.open(alias='temporary-results')

    # Default unnamed storage (purged on start)
    dataset_default = await Dataset.open()


if __name__ == '__main__':
    asyncio.run(main())
