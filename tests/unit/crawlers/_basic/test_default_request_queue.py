from crawlee.configuration import Configuration
from crawlee.crawlers import AdaptivePlaywrightCrawler, BasicCrawler
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue


async def test_first_crawler_uses_global_default_request_queue() -> None:
    storage_client = MemoryStorageClient()
    configuration = Configuration()
    crawler = BasicCrawler(configuration=configuration, storage_client=storage_client)

    queue = await crawler.get_request_manager()
    global_default = await RequestQueue.open(configuration=configuration, storage_client=storage_client)

    assert queue is global_default


async def test_three_crawlers_have_independent_default_request_queues() -> None:
    storage_client = MemoryStorageClient()
    configuration = Configuration()
    crawlers = [
        BasicCrawler(configuration=configuration, storage_client=storage_client),
        BasicCrawler(configuration=configuration, storage_client=storage_client),
        BasicCrawler(configuration=configuration, storage_client=storage_client),
    ]
    queues = [await crawler.get_request_manager() for crawler in crawlers]

    for queue in queues:
        await queue.add_request('https://example.test/same-request')

    assert len({id(queue) for queue in queues}) == 3
    assert [await queue.get_total_count() for queue in queues] == [1, 1, 1]


async def test_explicit_first_id_does_not_control_global_default_selection() -> None:
    storage_client = MemoryStorageClient()
    configuration = Configuration()
    first = BasicCrawler(id=42, configuration=configuration, storage_client=storage_client)
    later = BasicCrawler(configuration=configuration, storage_client=storage_client)

    first_queue = await first.get_request_manager()
    later_queue = await later.get_request_manager()

    assert first._id == 42
    assert later._id == 0
    assert first_queue is await RequestQueue.open(configuration=configuration, storage_client=storage_client)
    assert later_queue is await RequestQueue.open(
        alias=f'__default_{later._id}__', configuration=configuration, storage_client=storage_client
    )
    assert first_queue is not later_queue


async def test_injected_first_manager_consumes_global_default_selection() -> None:
    storage_client = MemoryStorageClient()
    configuration = Configuration()
    injected = await RequestQueue.open(alias='injected', configuration=configuration, storage_client=storage_client)
    first = BasicCrawler(request_manager=injected, configuration=configuration, storage_client=storage_client)
    later = BasicCrawler(configuration=configuration, storage_client=storage_client)

    later_queue = await later.get_request_manager()

    assert first._id == 0
    assert later._id == 1
    assert await first.get_request_manager() is injected
    assert later_queue is await RequestQueue.open(
        alias=f'__default_{later._id}__', configuration=configuration, storage_client=storage_client
    )
    assert later_queue is not await RequestQueue.open(configuration=configuration, storage_client=storage_client)


async def test_repeated_later_explicit_ids_share_stable_default_alias() -> None:
    storage_client = MemoryStorageClient()
    configuration = Configuration()
    _ = BasicCrawler(configuration=configuration, storage_client=storage_client)
    first = BasicCrawler(id=42, configuration=configuration, storage_client=storage_client)
    second = BasicCrawler(id=42, configuration=configuration, storage_client=storage_client)

    first_queue = await first.get_request_manager()
    second_queue = await second.get_request_manager()

    assert first_queue is second_queue
    assert first_queue is await first.get_request_manager()
    assert first_queue is await RequestQueue.open(
        alias=f'__default_{first._id}__', configuration=configuration, storage_client=storage_client
    )


async def test_adaptive_subclass_construction_preserves_ids_and_queue_ownership() -> None:
    storage_client = MemoryStorageClient()
    configuration = Configuration()
    adaptive = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
        configuration=configuration, storage_client=storage_client
    )
    following = BasicCrawler(configuration=configuration, storage_client=storage_client)

    adaptive_queue = await adaptive.get_request_manager()
    following_queue = await following.get_request_manager()
    await adaptive_queue.add_request('https://example.test/adaptive')

    assert adaptive._id == 0
    assert following._id == 3
    assert adaptive._pw_context_pipeline is not None
    assert adaptive._static_context_pipeline is not None
    assert following_queue is await RequestQueue.open(
        alias=f'__default_{following._id}__', configuration=configuration, storage_client=storage_client
    )
    assert await adaptive_queue.get_total_count() == 1
    assert await following_queue.get_total_count() == 0
    assert adaptive_queue is not following_queue
