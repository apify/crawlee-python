import asyncio

from crawlee.storages import RequestQueue


async def main() -> None:
    # Open the request queue, if it does not exist, it will be created.
    # Leave name empty to use the default request queue.
    request_queue = await RequestQueue.open(name='my-request-queue')

    # Add a single request.
    await request_queue.add_request('https://apify.com/')

    # Add multiple requests as a batch.
    await request_queue.add_requests(
        ['https://crawlee.dev/', 'https://crawlee.dev/python/']
    )

    # Fetch and process requests from the queue.
    while request := await request_queue.fetch_next_request():
        # Do something with it...

        # And mark it as handled.
        await request_queue.mark_request_as_handled(request)

    # Remove the request queue.
    await request_queue.drop()


if __name__ == '__main__':
    asyncio.run(main())
