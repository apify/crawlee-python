import asyncio

from crawlee.request_loaders import RequestList


async def main() -> None:
    # Open the request list, if it does not exist, it will be created.
    # Leave name empty to use the default request list.
    request_list = RequestList(
        name='my-request-list',
        requests=[
            'https://apify.com/',
            'https://crawlee.dev/',
            'https://crawlee.dev/python/',
        ],
    )

    # Fetch and process requests from the queue.
    while request := await request_list.fetch_next_request():
        # Do something with it...

        # And mark it as handled.
        await request_list.mark_request_as_handled(request)


if __name__ == '__main__':
    asyncio.run(main())
