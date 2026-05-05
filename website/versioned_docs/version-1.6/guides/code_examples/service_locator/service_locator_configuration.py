import asyncio
from datetime import timedelta

from crawlee import service_locator
from crawlee.configuration import Configuration


async def main() -> None:
    configuration = Configuration(
        log_level='DEBUG',
        headless=False,
        persist_state_interval=timedelta(seconds=30),
    )

    # Register configuration via service locator.
    service_locator.set_configuration(configuration)


if __name__ == '__main__':
    asyncio.run(main())
