import asyncio
import logging

from crawlee import service_locator
from crawlee.request_loaders import RequestList

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)


# Disable clearing the `KeyValueStore` on each run.
# This is necessary so that the state keys are not cleared at startup.
# The recommended way to achieve this behavior is setting the environment variable
# `CRAWLEE_PURGE_ON_START=0`
configuration = service_locator.get_configuration()
configuration.purge_on_start = False


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
        # Enable persistence
        persist_state_key='my-persist-state',
        persist_requests_key='my-persist-requests',
    )

    # We receive only one request.
    # Each time you run it, it will be a new request until you exhaust the `RequestList`.
    request = await request_list.fetch_next_request()
    if request:
        logger.info(f'Processing request: {request.url}')
        # Do something with it...

        # And mark it as handled.
        await request_list.mark_request_as_handled(request)


if __name__ == '__main__':
    asyncio.run(main())
