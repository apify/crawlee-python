import asyncio

from crawlee import Request
from crawlee.storages import RequestQueue


async def main() -> None:
    request_queue = await RequestQueue.open(alias='deduplication-demo')

    requests = [
        # The unique key is the normalized URL.
        Request.from_url('https://crawlee.dev/docs/'),
        # Same unique key: trailing slash, `utm_` parameters and fragment are ignored.
        Request.from_url('https://CRAWLEE.dev/docs?utm_source=newsletter#intro'),
        # A random prefix makes the unique key different every time.
        Request.from_url('https://crawlee.dev/docs', always_enqueue=True),
        # A custom unique key replaces the computed one.
        Request.from_url('https://crawlee.dev/docs', unique_key='docs-second-visit'),
        # POST requests to the same URL that differ only by payload.
        Request.from_url(
            'https://crawlee.dev/search',
            method='POST',
            payload='{"query": "crawler"}',
            use_extended_unique_key=True,
        ),
        Request.from_url(
            'https://crawlee.dev/search',
            method='POST',
            payload='{"query": "storage"}',
            use_extended_unique_key=True,
        ),
    ]

    for request in requests:
        processed = await request_queue.add_request(request)
        if processed is None:
            continue
        print(f'{processed.was_already_present!s:<5} {request.unique_key}')

    total = await request_queue.get_total_count()
    print(f'Requests in the queue: {total}')

    await request_queue.drop()


if __name__ == '__main__':
    asyncio.run(main())
