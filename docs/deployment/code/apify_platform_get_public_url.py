from apify import KeyValueStore


async def main() -> None:
    store = await KeyValueStore.open()
    await store.set_value('your-file', {'foo': 'bar'})
    url = store.get_public_url('your-file')
    # https://api.apify.com/v2/key-value-stores/<your-store-id>/records/your-file
