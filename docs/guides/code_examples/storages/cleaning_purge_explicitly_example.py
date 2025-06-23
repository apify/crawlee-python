import asyncio

from crawlee.storages import Dataset


async def main() -> None:
    # Create storage client with configuration
    dataset = await Dataset.open(name='my-dataset')

    # Purge the dataset explicitly - purging will remove all items from the dataset.
    # But keeps the dataset itself and its metadata.
    await dataset.purge()

    # Or you can drop the dataset completely, which will remove the dataset
    # and all its items.
    await dataset.drop()


if __name__ == '__main__':
    asyncio.run(main())
