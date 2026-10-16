import asyncio
from datetime import timedelta

from crawlee import service_locator
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.events import (
    Event,
    EventPersistStateData,
    EventSystemInfoData,
    LocalEventManager,
)


async def main() -> None:
    # Register an event manager with a shorter persist state interval, so that
    # the event fires several times during this short crawl.
    event_manager = LocalEventManager(persist_state_interval=timedelta(seconds=2))
    service_locator.set_event_manager(event_manager)

    async def on_persist_state(event_data: EventPersistStateData) -> None:
        print(f'Persist state event (is_migrating={event_data.is_migrating})')

    async def on_system_info(event_data: EventSystemInfoData) -> None:
        cpu = event_data.cpu_info.used_ratio
        memory = event_data.memory_info.current_size
        print(f'System info event: CPU {cpu:.0%}, memory {memory}')

    event_manager.on(event=Event.PERSIST_STATE, listener=on_persist_state)
    event_manager.on(event=Event.SYSTEM_INFO, listener=on_system_info)

    crawler = ParselCrawler(max_requests_per_crawl=20)

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev'])

    # Stop listening to the system info events.
    event_manager.off(event=Event.SYSTEM_INFO, listener=on_system_info)


if __name__ == '__main__':
    asyncio.run(main())
