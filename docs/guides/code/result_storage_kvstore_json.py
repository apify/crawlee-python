import asyncio
from crawlee.storages import KeyValueStore

async def main() -> None:
    # Open a key-value store named 'my-json-store'
    kv_store = await KeyValueStore.open(name='my-json-store')

    # Save a JSON object
    await kv_store.set_value('user-profile', {'name': 'Alice', 'age': 27})

    # Retrieve the JSON object
    profile = await kv_store.get_value('user-profile')
    print(profile)  # Output: {'name': 'Alice', 'age': 27}

    # Remove the key-value store
    await kv_store.drop()

if __name__ == '__main__':
    asyncio.run(main())
