import asyncio

from crawlee.storages import Dataset


async def main() -> None:
    # Open dataset manually using asynchronous constructor open().
    dataset = await Dataset.open()

    # Interact with dataset directly.
    await dataset.push_data({'key': 'value'})


if __name__ == '__main__':
    asyncio.run(main())
