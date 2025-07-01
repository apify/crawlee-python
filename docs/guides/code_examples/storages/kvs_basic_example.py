import asyncio

from crawlee.storages import KeyValueStore


async def main() -> None:
    # Open the key-value store, if it does not exist, it will be created.
    # Leave name empty to use the default KVS.
    kvs = await KeyValueStore.open(name='my-key-value-store')

    # Set a value associated with 'some-key'.
    await kvs.set_value(key='some-key', value={'foo': 'bar'})

    # Get the value associated with 'some-key'.
    value = kvs.get_value('some-key')
    # Do something with it...

    # Delete the value associated with 'some-key' by setting it to None.
    await kvs.set_value(key='some-key', value=None)

    # Remove the key-value store.
    await kvs.drop()


if __name__ == '__main__':
    asyncio.run(main())
