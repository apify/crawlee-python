import asyncio
from urllib.parse import urlencode

from crawlee import Request


async def main() -> None:
    # Prepare a POST request to the form endpoint.
    request = Request.from_url(
        url='https://httpbin.org/post',
        method='POST',
        headers={'content-type': 'application/x-www-form-urlencoded'},
        payload=urlencode(
            {
                'custname': 'John Doe',
                'custtel': '1234567890',
                'custemail': 'johndoe@example.com',
                'size': 'large',
                'topping': ['bacon', 'cheese', 'mushroom'],
                'delivery': '13:00',
                'comments': 'Please ring the doorbell upon arrival.',
            }
        ).encode(),
    )


if __name__ == '__main__':
    asyncio.run(main())
