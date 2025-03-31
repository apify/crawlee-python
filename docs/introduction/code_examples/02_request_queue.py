from crawlee.storages import RequestQueue


async def main() -> None:
    # First you create the request queue instance.
    rq = await RequestQueue.open()

    # And then you add one or more requests to it.
    await rq.add_request('https://crawlee.dev')
