import asyncio

from crawlee.storages import RequestList


async def main() -> None:
    # Open the request list, if it does not exist, it will be created.
    # Leave name empty to use the default request list.
    request_list = RequestList(
        name='my-request-list',
        requests=['https://apify.com/', 'https://crawlee.dev/', 'https://crawlee.dev/python/'],
    )

    # You can interact with the request list in the same way as with the request queue.
    await request_list.add_requests_batched(
        [
            'https://crawlee.dev/python/docs/quick-start',
            'https://crawlee.dev/python/api',
        ]
    )

    # Fetch and process requests from the queue.
    while request := await request_list.fetch_next_request():
        # Do something with it...

        # And mark it as handled.
        await request_list.mark_request_as_handled(request)

    # Remove the request queue.
    await request_list.drop()


if __name__ == '__main__':
    asyncio.run(main())
