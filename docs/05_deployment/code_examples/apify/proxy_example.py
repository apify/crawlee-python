import asyncio

from apify import Actor


async def main() -> None:
    async with Actor:
        # Create a new Apify Proxy configuration. The password can be found at
        # https://console.apify.com/proxy/http-settings and should be provided either
        # as a parameter "password" or as an environment variable "APIFY_PROXY_PASSWORD".
        proxy_configuration = await Actor.create_proxy_configuration(
            password='apify_proxy_YOUR_PASSWORD',
        )

        if not proxy_configuration:
            Actor.log.warning('Failed to create proxy configuration.')
            return

        proxy_url = await proxy_configuration.new_url()
        Actor.log.info(f'Proxy URL: {proxy_url}')


if __name__ == '__main__':
    asyncio.run(main())
