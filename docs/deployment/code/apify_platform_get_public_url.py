from apify import Actor


async def main() -> None:
    store = await Actor.open_key_value_store()
    await store.set_value('your-file', {'foo': 'bar'})
    # url = store.get_public_url('your-file')  # noqa: ERA001
    # https://api.apify.com/v2/key-value-stores/<your-store-id>/records/your-file
