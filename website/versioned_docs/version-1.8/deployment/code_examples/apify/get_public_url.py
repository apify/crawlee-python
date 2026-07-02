import asyncio

from apify import Actor


async def main() -> None:
    async with Actor:
        store = await Actor.open_key_value_store()
        await store.set_value('your-file', {'foo': 'bar'})
        url = store.get_public_url('your-file')
        Actor.log.info(f'KVS public URL: {url}')
        # https://api.apify.com/v2/key-value-stores/<your-store-id>/records/your-file


if __name__ == '__main__':
    asyncio.run(main())
