import asyncio

from crawlee.storages import KeyValueStore


async def main() -> None:
    store = await KeyValueStore.open()
    # Store the screenshot in the key-value store.
    await store.set_value(key='some-key', value={'foo': 'bar'})

    # Get value from your defined key-value store
    value = store.get_value('some-key')

    # Delete a record from the named key-value store
    await store.set_value(key='some-key', value=None)


if __name__ == '__main__':
    asyncio.run(main())
