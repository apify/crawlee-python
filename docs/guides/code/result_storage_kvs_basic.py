# Import the asyncio module to support asynchronous programming
import asyncio

# Import KeyValueStore from the crawlee.storages module
from crawlee.storages import KeyValueStore


async def main() -> None:
    # Open a KeyValueStore asynchronously (e.g., for storing key-value pairs)
    store = await KeyValueStore.open()

    # Set a key-value pair in the store with the key 'some-key' and the value {'foo': 'bar'}
    await store.set_value(key='some-key', value={'foo': 'bar'})

    # Retrieve the value associated with 'some-key' from the store
    value = store.get_value('some-key')

    # Delete the value associated with 'some-key' by setting its value to None
    await store.set_value(key='some-key', value=None)


if __name__ == '__main__':
    asyncio.run(main())
