from __future__ import annotations

import codecs
import sys
from unittest import mock

import pytest

from crawlee import HttpHeaders
from crawlee.crawlers import ParselCrawler
from crawlee.crawlers._parsel._parsel_parser import ParselParser

_CZECH = 'Test dekódování znaků českého jazyka'
_UKRAINIAN = 'Тест декодування символів української мови'
_CHINESE = '中文字符解码测试'
_XHTML = b'<?xml version="1.0"?><html xmlns="http://www.w3.org/1999/xhtml"><body><a href="/next">Next</a></body></html>'


@pytest.mark.usefixtures('_restore_crawlers_binding')
def test_import_error_handled() -> None:
    # Simulate ImportError for parsel
    with mock.patch.dict('sys.modules', {'parsel': None}):
        # Invalidate ParselCrawler import
        for mod_name in list(sys.modules):
            if mod_name == 'crawlee.crawlers' or mod_name.startswith('crawlee.crawlers._parsel'):
                sys.modules.pop(mod_name, None)
        with pytest.raises(ImportError):
            from crawlee.crawlers import ParselCrawler  # noqa: F401 PLC0415


def test_default_logger() -> None:
    assert ParselCrawler().log.name == 'ParselCrawler'


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

    selector = await ParselParser().parse(response)

    assert selector.css('p::text').getall() == texts


@pytest.mark.parametrize(
    ('body', 'content_type', 'expected_type'),
    [
        pytest.param(_XHTML, 'text/html', 'html', id='xhtml-as-html'),
        pytest.param(_XHTML, 'application/xhtml+xml; charset=utf-8', 'html', id='xhtml-as-xhtml'),
        pytest.param(_XHTML, 'application/xml', 'xml', id='xhtml-as-xml'),
        pytest.param(b'{"hello": "world"}', 'text/html', 'json', id='json-as-html'),
        pytest.param(b'{"hello": "world"}', 'text/html; charset=utf-8', 'json', id='json-as-html-charset'),
        pytest.param(b' \n[1, 2]', 'text/html', 'json', id='json-after-whitespace'),
        pytest.param(b'\n{"hello": "world"}', 'text/html; charset=utf-8', 'json', id='json-after-whitespace-charset'),
        pytest.param(b'[<a href="/next">Next</a>]', 'text/html', 'html', id='bracket-not-json'),
        pytest.param(codecs.BOM_UTF8 + b'{"hello": "world"}', 'text/html', 'json', id='json-with-bom'),
        pytest.param(codecs.BOM_UTF8 + _XHTML, 'text/html', 'html', id='xhtml-with-bom'),
        pytest.param(_UKRAINIAN.encode('cp1251'), 'text/html', 'html', id='non-ascii-start'),
    ],
)
async def test_parse_selector_type(body: bytes, content_type: str, expected_type: str) -> None:
    """An HTML response starting with an XML declaration is read as HTML, while JSON and XML keep their type."""
    response = mock.Mock(headers=HttpHeaders({'Content-Type': content_type}), read=mock.AsyncMock(return_value=body))

    selector = await ParselParser().parse(response)

    assert selector.type == expected_type


async def test_parse_empty_body() -> None:
    """An empty body is read as an empty HTML document."""
    response = mock.Mock(headers=HttpHeaders({'Content-Type': 'text/html'}), read=mock.AsyncMock(return_value=b''))

    selector = await ParselParser().parse(response)

    assert selector.type == 'html'
    assert selector.css('a').getall() == []
