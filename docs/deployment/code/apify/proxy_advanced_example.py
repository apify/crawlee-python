from apify import Actor  # type: ignore[import-not-found]


async def main() -> None:
    async with Actor:
        proxy_configuration = await Actor.create_proxy_configuration(
            password='apify_proxy_YOUR_PASSWORD',
            # Specify the proxy group to use.
            groups=['RESIDENTIAL'],
            # Set the country code for the proxy.
            country_code='US',
        )

        # ...
