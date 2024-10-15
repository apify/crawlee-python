import asyncio
from crawlee.storages import KeyValueStore

async def main() -> None:
    kv_store = await KeyValueStore.open(name='my-store')

    # Save a string value
    await kv_store.set_value('greeting', 'Hello, world!')

    # Save an integer value
    await kv_store.set_value('year', 2024)

    # Retrieve values by key
    greeting = await kv_store.get_value('greeting')
    year = await kv_store.get_value('year')
    print(greeting)  # Output: Hello, world!
    print(year)      # Output: 2024

    # Remove the key-value store
    await kv_store.drop()

if __name__ == '__main__':
    asyncio.run(main())
