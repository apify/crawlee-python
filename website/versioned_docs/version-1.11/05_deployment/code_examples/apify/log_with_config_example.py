import asyncio

from apify import Actor, Configuration


async def main() -> None:
    # Create a new configuration with your API key. You can find it at
    # https://console.apify.com/settings/integrations. It can be provided either
    # as a parameter "token" or as an environment variable "APIFY_TOKEN".
    config = Configuration(
        token='apify_api_YOUR_TOKEN',
    )

    async with Actor(config):
        Actor.log.info('Hello from Apify platform!')


if __name__ == '__main__':
    asyncio.run(main())
