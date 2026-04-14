import asyncio

from crawlee.proxy_configuration import ProxyConfiguration


async def main() -> None:
    proxy_configuration = ProxyConfiguration(
        proxy_urls=[
            'http://proxy-1.com/',
            'http://proxy-2.com/',
        ]
    )

    # The proxy URLs are rotated in a round-robin.
    proxy_url_1 = await proxy_configuration.new_url()  # http://proxy-1.com/
    proxy_url_2 = await proxy_configuration.new_url()  # http://proxy-2.com/
    proxy_url_3 = await proxy_configuration.new_url()  # http://proxy-1.com/


if __name__ == '__main__':
    asyncio.run(main())
