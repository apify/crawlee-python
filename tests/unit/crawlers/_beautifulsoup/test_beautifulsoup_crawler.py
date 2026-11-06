from __future__ import annotations

import sys
from unittest import mock

import pytest

from crawlee import HttpHeaders
from crawlee.crawlers import BeautifulSoupCrawler
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser

_CZECH = 'Test dekódování znaků českého jazyka'


_UKRAINIAN = 'Тест декодування символів української мови'


_CHINESE = '中文字符解码测试'


def test_default_logger() -> None:
    """The default crawler logger is named after the crawler class, under `crawlee.crawlers`."""
    assert BeautifulSoupCrawler().log.name == 'crawlee.crawlers.BeautifulSoupCrawler'


def test_import_error_handled() -> None:
    # Simulate ImportError for BeautifulSoup
    with mock.patch.dict('sys.modules', {'bs4': None}):
        # Invalidate BeautifulSoupCrawler import
        for mod_name in list(sys.modules):
            if mod_name == 'crawlee.crawlers' or mod_name.startswith('crawlee.crawlers._beautifulsoup'):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.crawlers import BeautifulSoupCrawler  # noqa: F401 PLC0415


@pytest.mark.parametrize(
    ('texts', 'body', 'content_type'),
    [
        pytest.param(
            [_CZECH],
            f'<p>{_CZECH}</p>'.encode('cp1250'),
            'text/html; charset=windows-1250',
            id='header',
        ),
        pytest.param(
            [_CHINESE],
            b'<head><meta http-equiv="Content-Type" content="text/html; charset=GBK"></head>'
            + f'<p>{_CHINESE}</p>'.encode('gbk'),
            'text/html',
            id='meta',
        ),
        pytest.param(
            [_CZECH, '\ufffd'],
            f'<head><meta charset="windows-1250"></head><p>{_CZECH}</p>'.encode('cp1250') + b'<p>\x81</p>',
            'text/html',
            id='meta-with-invalid-byte',
        ),
        pytest.param([_UKRAINIAN], f'<p>{_UKRAINIAN}</p>'.encode(), 'text/html', id='undeclared'),
    ],
)
async def test_parse_decodes_page_encoding(texts: list[str], body: bytes, content_type: str) -> None:
    """The page is decoded with the encoding its header or `<meta>` tag declares."""
    response = mock.Mock(headers=HttpHeaders({'Content-Type': content_type}), read=mock.AsyncMock(return_value=body))

    soup = await BeautifulSoupParser().parse(response)

    assert [p.get_text() for p in soup.find_all('p')] == texts


async def test_parse_leaves_undeclared_encoding_to_beautifulsoup() -> None:
    """A page declaring no encoding is left to `BeautifulSoup` to detect."""
    body = f'<p>{_UKRAINIAN}</p>'.encode('cp1251')
    response = mock.Mock(headers=HttpHeaders({'Content-Type': 'text/html'}), read=mock.AsyncMock(return_value=body))

    soup = await BeautifulSoupParser().parse(response)

    assert soup.original_encoding is not None
