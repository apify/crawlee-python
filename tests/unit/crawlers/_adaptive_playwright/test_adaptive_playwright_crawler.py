from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from crawlee.autoscaling import ConcurrencySystem
from crawlee.crawlers import AdaptivePlaywrightCrawler


async def test_accepts_concurrency_system() -> None:
    """A supplied concurrency system is used as is, since the lower default concurrency applies only without one."""
    concurrency_system = ConcurrencySystem()
    crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(concurrency_system=concurrency_system)

    assert crawler.concurrency_system is concurrency_system


@pytest.mark.parametrize(
    ('rendering_type', 'save_response_cookies', 'expected_cookies'),
    [
        pytest.param('static', True, {'a'}, id='static, saved'),
        pytest.param('static', False, set(), id='static, not saved'),
        pytest.param('client only', True, {'a'}, id='browser, saved'),
        pytest.param('client only', False, set(), id='browser, not saved'),
    ],
)
async def test_save_response_cookies(
    *, rendering_type: RenderingType, save_response_cookies: bool, expected_cookies: set[str], server_url: URL
) -> None:
    """Test that `save_response_cookies` controls saving the response cookies to the session in both sub crawlers."""
    predictor = _SimpleRenderingTypePredictor(
        rendering_types=cycle([rendering_type]), detection_probability_recommendation=cycle([0])
    )

    async with SessionPool(max_pool_size=1) as session_pool:
        crawler = AdaptivePlaywrightCrawler.with_parsel_static_parser(
            rendering_type_predictor=predictor,
            save_response_cookies=save_response_cookies,
            session_pool=session_pool,
        )

        @crawler.router.default_handler
        async def request_handler(context: AdaptivePlaywrightCrawlingContext) -> None:
            pass

        await crawler.run([str(server_url.with_path('set_cookies').extend_query(a=1))])

        session = await session_pool.get_session()

    assert {cookie['name'] for cookie in session.cookies.get_cookies_as_dicts()} == expected_cookies


@pytest.mark.parametrize(
    'optional_module_name',
    [
        pytest.param('playwright', id='playwright'),
        pytest.param('rapidfuzz', id='rapidfuzz'),
        pytest.param('sklearn', id='sklearn'),
    ],
)
def test_import_error_handled(optional_module_name: str) -> None:
    # Block the package and all its cached submodules to prevent submodule cache entries
    # from bypassing the blocked top-level package.
    blocked = {
        mod_name: None
        for mod_name in sys.modules
        if mod_name == optional_module_name or mod_name.startswith(f'{optional_module_name}.')
    }

    with patch.dict('sys.modules', blocked):
        for mod_name in list(sys.modules):
            if mod_name == 'crawlee.crawlers' or mod_name.startswith('crawlee.crawlers._adaptive_playwright'):
                sys.modules.pop(mod_name, None)

        with pytest.raises(ImportError):
            from crawlee.crawlers import AdaptivePlaywrightCrawler  # noqa: F401 PLC0415
