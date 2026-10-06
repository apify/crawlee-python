from __future__ import annotations

import codecs
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest

from crawlee import HttpHeaders, Request
from crawlee._request import RequestOptions
from crawlee._utils.html import (
    _ENCODING_BY_LABEL,
    _PRESCAN_BYTES,
    FormRequestOptions,
    decode_html_body,
    get_declared_html_encoding,
)
from crawlee.crawlers import BeautifulSoupCrawlingContext, ParselCrawlingContext
from crawlee.crawlers._beautifulsoup._beautifulsoup_parser import BeautifulSoupParser
from crawlee.crawlers._parsel._parsel_parser import ParselParser

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from crawlee.crawlers import BeautifulSoupParserType

    ExtractFormRequests = Callable[..., Awaitable[list[Request]]]

_CZECH = 'Test dekódování znaků českého jazyka'
_JAPANESE = '東京で文字コードのテストを行います。'
_FRENCH = 'Test de décodage des caractères de la langue française dans une œuvre'
_PAGE_URL = 'https://example.com/page/index.html'
_REFERRER_PAGE_URL = 'https://user:pass@example.com/page?token=1#top'


def _mock_context(html: str, page_request: Request | None, content_type: str | None, encoding: str | None) -> Mock:
    body = html.encode(encoding or 'utf-8', 'xmlcharrefreplace')
    return Mock(
        request=page_request or Request.from_url(_PAGE_URL),
        http_response=Mock(
            headers=HttpHeaders({'Content-Type': content_type} if content_type else {}),
            read=AsyncMock(return_value=body),
        ),
    )


async def _extract_with_parsel(
    html: str,
    page_request: Request | None = None,
    content_type: str | None = None,
    encoding: str | None = None,
    **kwargs: Any,
) -> list[Request]:
    context = _mock_context(html, page_request, content_type, encoding)
    context.selector = await ParselParser().parse(context.http_response)
    return await ParselCrawlingContext.extract_form_requests(context, **kwargs)


async def _extract_with_beautifulsoup(
    html: str,
    page_request: Request | None = None,
    content_type: str | None = None,
    encoding: str | None = None,
    parser: BeautifulSoupParserType = 'lxml',
    **kwargs: Any,
) -> list[Request]:
    context = _mock_context(html, page_request, content_type, encoding)
    context.soup = await BeautifulSoupParser(parser).parse(context.http_response)
    return await BeautifulSoupCrawlingContext.extract_form_requests(context, **kwargs)


@pytest.fixture(
    params=[
        pytest.param(_extract_with_parsel, id='parsel'),
        pytest.param(_extract_with_beautifulsoup, id='beautifulsoup'),
    ]
)
def extract_form_requests(request: pytest.FixtureRequest) -> ExtractFormRequests:
    return request.param


def _paths(requests: list[Request]) -> list[str]:
    return [urlsplit(request.url).path for request in requests]


def _submitted_fields(request: Request) -> dict[str, str | list[str]]:
    """Get the fields a request submits, in the shape of the `fields` argument."""
    query = urlsplit(request.url).query if request.method == 'GET' else (request.payload or b'').decode()
    parsed = parse_qs(query, keep_blank_values=True)
    return {name: values[0] if len(values) == 1 else values for name, values in parsed.items()}


@pytest.mark.parametrize(
    ('body', 'content_type', 'expected'),
    [
        pytest.param(_CZECH.encode(), None, None, id='undeclared'),
        pytest.param(_CZECH.encode('cp1250'), 'text/html; charset="windows-1250"', _CZECH, id='quoted-header'),
        pytest.param(_FRENCH.encode('cp1252'), 'text/html; charset=ISO-8859-1', _FRENCH, id='latin-1-superset'),
        pytest.param(_FRENCH.encode('cp1252'), 'text/html; charset=us-ascii', _FRENCH, id='ascii-superset'),
        pytest.param(codecs.BOM_UTF8 + _CZECH.encode(), 'text/html; charset=ISO-8859-1', _CZECH, id='bom-beats-header'),
        pytest.param(codecs.BOM_UTF16_LE + _CZECH.encode('utf-16-le'), None, _CZECH, id='utf-16-le-bom'),
        pytest.param(codecs.BOM_UTF16_BE + _CZECH.encode('utf-16-be'), None, _CZECH, id='utf-16-be-bom'),
        pytest.param(
            f'<meta charset="utf-8"><p>{_CZECH}</p>'.encode('cp1250'),
            'text/html; charset=windows-1250',
            f'<meta charset="utf-8"><p>{_CZECH}</p>',
            id='header-beats-meta',
        ),
        pytest.param(
            f"<META HTTP-EQUIV='Content-Type' CONTENT='text/html; charset=windows-1250'>{_CZECH}".encode('cp1250'),
            None,
            f"<META HTTP-EQUIV='Content-Type' CONTENT='text/html; charset=windows-1250'>{_CZECH}",
            id='meta-http-equiv-uppercase',
        ),
        pytest.param(
            f'<meta charset="utf-16"><p>{_CZECH}</p>'.encode(),
            None,
            f'<meta charset="utf-16"><p>{_CZECH}</p>',
            id='meta-utf-16-as-utf-8',
        ),
        pytest.param(_CZECH.encode(), 'text/html; charset=base64', None, id='non-text-codec'),
        pytest.param('中文𠀀'.encode('gb18030'), 'text/html; charset=gb2312', '中文𠀀', id='gbk-as-gb18030'),
        pytest.param(b'<p>a+b</p>', 'text/html; charset=utf-7', None, id='utf-7-not-a-web-encoding'),
        pytest.param(_CZECH.encode(), 'text/html; charset=unicode_escape', None, id='python-only-codec'),
        pytest.param(
            _CZECH.encode('cp1250'), 'text/html; charset=" Windows-1250 "', _CZECH, id='label-case-and-spaces'
        ),
        pytest.param(_CZECH.encode(), 'text/html; charset=bogus', None, id='unknown-label'),
        pytest.param(
            f'<meta charset="windows-1250"><p>{_CZECH}</p>'.encode('cp1250'),
            'text/html; charset=bogus',
            f'<meta charset="windows-1250"><p>{_CZECH}</p>',
            id='unknown-header-label-falls-back-to-meta',
        ),
        pytest.param(
            f'<!-- <meta charset="windows-1250"> --><p>{_CZECH}</p>'.encode(),
            None,
            None,
            id='commented-out-meta',
        ),
        pytest.param(f'<!-- <meta charset="windows-1250"> <p>{_CZECH}</p>'.encode(), None, None, id='unclosed-comment'),
        pytest.param(b'<metadata charset="windows-1250">', None, None, id='not-a-meta-tag'),
        pytest.param(b'<meta name="x" data-charset="windows-1250">', None, None, id='charset-suffixed-attribute'),
        pytest.param(
            b'<meta name="description" content="Set charset=windows-1250 in the header">',
            None,
            None,
            id='charset-in-other-meta-content',
        ),
        pytest.param(
            f'<meta charset="bogus"><meta charset=windows-1250><p>{_CZECH}</p>'.encode('cp1250'),
            None,
            f'<meta charset="bogus"><meta charset=windows-1250><p>{_CZECH}</p>',
            id='unknown-meta-label-skipped',
        ),
        pytest.param(
            f'<meta content="a>b" charset="windows-1250"><p>{_CZECH}</p>'.encode('cp1250'),
            None,
            f'<meta content="a>b" charset="windows-1250"><p>{_CZECH}</p>',
            id='quoted-gt-in-meta',
        ),
        pytest.param(b'', 'text/html; charset=utf-8', '', id='empty-body'),
        pytest.param(
            b'<p>' + b'a' * (_PRESCAN_BYTES - len(b'<p><meta charset=iso-8859-1')) + b'<meta charset=iso-8859-15>',
            None,
            None,
            id='meta-cut-off-by-prescan-end',
        ),
        pytest.param(
            f'<meta http-equiv="Content-Type" content="text/html; charset=\'windows-1250\'"><p>{_CZECH}</p>'.encode(
                'cp1250'
            ),
            None,
            f'<meta http-equiv="Content-Type" content="text/html; charset=\'windows-1250\'"><p>{_CZECH}</p>',
            id='single-quoted-charset-in-meta-content',
        ),
        pytest.param(
            f'<?xml version="1.0" encoding="windows-1250"?><p>{_CZECH}</p>'.encode('cp1250'),
            None,
            f'<?xml version="1.0" encoding="windows-1250"?><p>{_CZECH}</p>',
            id='xml-declaration',
        ),
        pytest.param(
            f'<?xml version="1.0" encoding="bogus"?><meta charset="windows-1250"><p>{_CZECH}</p>'.encode('cp1250'),
            None,
            f'<?xml version="1.0" encoding="bogus"?><meta charset="windows-1250"><p>{_CZECH}</p>',
            id='unknown-xml-label-falls-back-to-meta',
        ),
        pytest.param(b'\xffok', 'text/html; charset=utf-8', '\ufffdok', id='invalid-bytes'),
    ],
)
def test_detect_and_decode(body: bytes, content_type: str | None, expected: str | None) -> None:
    """The body is decoded with the encoding the page declares, or not at all if it declares none."""
    encoding = get_declared_html_encoding(body, content_type)

    text = None if encoding is None else decode_html_body(body, encoding)

    assert text == expected


@pytest.mark.parametrize(
    ('body', 'content_type', 'expected'),
    [
        pytest.param(codecs.BOM_UTF16_LE + b'x', 'text/html; charset=utf-8', 'utf-16-le', id='bom'),
        pytest.param(b'<p>x</p>', 'text/html; charset=ISO-8859-1', 'windows-1252', id='header-superset'),
    ],
)
def test_get_declared_html_encoding(body: bytes, content_type: str, expected: str) -> None:
    """The Python codec of the encoding the page declares is detected."""
    assert get_declared_html_encoding(body, content_type) == expected


def test_encoding_labels_resolve_to_python_codecs() -> None:
    """Every encoding in the label table is a Python codec."""
    for codec in set(_ENCODING_BY_LABEL.values()):
        codecs.lookup(codec)


def test_form_request_options() -> None:
    """`FormRequestOptions` has every request option except those the form or enqueuing sets."""
    options = RequestOptions.__required_keys__ | RequestOptions.__optional_keys__
    expected = options - {'url', 'method', 'payload', 'unique_key', 'id', 'enqueue_strategy'}

    assert FormRequestOptions.__required_keys__ | FormRequestOptions.__optional_keys__ == expected


async def test_get_form(extract_form_requests: ExtractFormRequests) -> None:
    """A GET form replaces the query of its action with the fields and keeps the fragment."""
    html = '<form action="?old=1#top"><input name="q" value="shoes"><input name="page" value="2"></form>'

    [request] = await extract_form_requests(html)

    assert request.method == 'GET'
    assert request.payload is None
    assert request.url == 'https://example.com/page/index.html?q=shoes&page=2#top'


async def test_post_form(extract_form_requests: ExtractFormRequests) -> None:
    """A POST form sends the fields as an urlencoded body, which counts towards the unique key."""
    html = '<form method="post" action="login"><input name="user" value="a b"><input name="pass" value="&"></form>'

    [request] = await extract_form_requests(html)
    [other] = await extract_form_requests(html, fields={'user': 'c'})

    assert request.method == 'POST'
    assert request.url == 'https://example.com/page/login'
    assert request.headers['content-type'] == 'application/x-www-form-urlencoded'
    assert request.payload == b'user=a+b&pass=%26'
    assert request.unique_key != other.unique_key


async def test_multipart_form(extract_form_requests: ExtractFormRequests) -> None:
    """A multipart form is encoded with a deterministic boundary and empty file parts."""
    html = (
        '<form method="post" enctype="multipart/form-data">'
        '<input name="title" value="hi"><input type="file" name="doc">'
        '</form>'
    )

    [request] = await extract_form_requests(html)
    [again] = await extract_form_requests(html)

    content_type = request.headers['content-type']
    assert content_type.startswith('multipart/form-data; boundary=')
    boundary = content_type.split('boundary=')[1]
    assert (
        request.payload
        == (
            f'--{boundary}\r\n'
            'Content-Disposition: form-data; name="title"\r\n\r\nhi\r\n'
            f'--{boundary}\r\n'
            'Content-Disposition: form-data; name="doc"; filename=""\r\n'
            'Content-Type: application/octet-stream\r\n\r\n\r\n'
            f'--{boundary}--\r\n'
        ).encode()
    )
    assert request.unique_key == again.unique_key


async def test_multipart_escapes(extract_form_requests: ExtractFormRequests) -> None:
    """Multipart names escape quotes and line breaks, and values submit line breaks as CRLF."""
    html = '<form method="post" enctype="multipart/form-data"><textarea name="a&quot;b">x\ny</textarea></form>'

    [request] = await extract_form_requests(html, fields={'c\nd': 'v'})

    assert b'name="a%22b"\r\n\r\nx\r\ny\r\n' in (request.payload or b'')
    assert b'name="c%0D%0Ad"\r\n\r\nv\r\n' in (request.payload or b'')


async def test_text_plain_form(extract_form_requests: ExtractFormRequests) -> None:
    """A `text/plain` form sends one field per line."""
    html = '<form method="post" enctype="TEXT/PLAIN"><input name="a" value="1"><input name="b" value="2"></form>'

    [request] = await extract_form_requests(html)

    assert request.headers['content-type'] == 'text/plain'
    assert request.payload == b'a=1\r\nb=2\r\n'


async def test_submitted_fields(extract_form_requests: ExtractFormRequests) -> None:
    """Only the fields a browser would submit are collected."""
    html = """
    <form>
        <input name="text" value="t">
        <input type="checkbox" name="checked" value="c1" checked>
        <input type="checkbox" name="unchecked" value="c2">
        <input type="checkbox" name="no-value" checked>
        <input type="radio" name="radio" value="r1" checked>
        <input type="radio" name="radio" value="r2" checked>
        <input type="radio" name="radio" value="r3">
        <select name="first"><option value="o1">1</option><option value="o2">2</option></select>
        <select name="last"><option value="a" selected>A</option><option value="b" selected>B</option></select>
        <select name="first-enabled"><option value="off" disabled>Off</option><option value="on">On</option></select>
        <select name="list-box" size="2"><option value="l1">1</option><option value="l2">2</option></select>
        <select name="placeholder"><option value="" disabled selected>Pick</option><option value="p">P</option></select>
        <select name="multi-disabled" multiple>
            <option value="d1" disabled selected>1</option>
            <optgroup disabled><option value="d2" selected>2</option></optgroup>
            <option value="e" selected>3</option>
        </select>
        <select name="text-value"><option> spaced\n  text </option></select>
        <select name="no-options"></select>
        <select name="multi" multiple>
            <option value="m1" selected>1</option><option value="m2">2</option><option value="m3" selected>3</option>
        </select>
        <textarea name="area">\n\nlong text</textarea>
        <input name="disabled" value="d" disabled>
        <fieldset disabled><input name="in-fieldset" value="f"></fieldset>
        <input type="reset" name="reset" value="r">
        <input type="button" name="button" value="b">
        <input type="file" name="file">
        <input name="empty">
        <input type="hidden" name="hidden">
        <input value="no name">
    </form>
    """

    [request] = await extract_form_requests(html)

    assert _submitted_fields(request) == {
        'text': 't',
        'checked': 'c1',
        'no-value': 'on',
        'radio': 'r2',
        'first': 'o1',
        'last': 'b',
        'first-enabled': 'on',
        'multi-disabled': 'e',
        'text-value': 'spaced text',
        'multi': ['m1', 'm3'],
        'area': '\r\nlong text',
        'file': '',
        'empty': '',
        'hidden': '',
    }


async def test_form_attribute(extract_form_requests: ExtractFormRequests) -> None:
    """Fields are assigned to forms by their `form` attribute, and `fields` fill in only the fields a form has."""
    html = """
    <form id="search" action="/search"><input name="q" value="x"><input name="other" value="o" form="login">
        <input name="orphan" value="z" form="missing"></form>
    <select name="lang" form="search"><option value="en" selected>EN</option></select>
    <button form="search" name="go">Go</button>
    <form id="login" action="/login"></form>
    <form action="/empty"></form>
    """

    search, login, empty = await extract_form_requests(html, fields={'q': 'y', 'other': 'p'}, all_forms=True)

    assert _submitted_fields(search) == {'q': 'y', 'lang': 'en', 'go': ''}
    assert _submitted_fields(login) == {'other': 'p'}
    assert _submitted_fields(empty) == {}


async def test_option_text_keeps_nbsp(extract_form_requests: ExtractFormRequests) -> None:
    """An `<option>` without a value collapses only ASCII whitespace in its text."""
    html = '<form><select name="s"><option>&nbsp;a&nbsp;\n b </option></select></form>'

    [request] = await extract_form_requests(html, content_type='text/html; charset=utf-8')

    assert _submitted_fields(request) == {'s': '\xa0a\xa0 b'}


async def test_disabled_fieldset(extract_form_requests: ExtractFormRequests) -> None:
    """A disabled `<fieldset>` disables all fields inside it except its first `<legend>`, and an enabled one none."""
    html = """
    <form>
        <fieldset disabled><legend><input name="first-legend"></legend><legend><input name="second-legend"></legend>
        </fieldset>
        <fieldset disabled><fieldset disabled></fieldset><label><input name="a"></label><input name="b"></fieldset>
        <fieldset><label><input name="c"></label></fieldset>
    </form>
    """

    [request] = await extract_form_requests(html)

    assert _submitted_fields(request) == {'first-legend': '', 'c': ''}


async def test_fields_argument(extract_form_requests: ExtractFormRequests) -> None:
    """Values from `fields` replace, drop and add fields."""
    html = '<form><input name="keep" value="k"><input name="replace" value="old"><input name="drop" value="d"></form>'

    [request] = await extract_form_requests(html, fields={'replace': 'new', 'drop': None, 'tags': ['a', 'b']})

    assert _submitted_fields(request) == {'keep': 'k', 'replace': 'new', 'tags': ['a', 'b']}


@pytest.mark.parametrize(
    ('html', 'expected_query'),
    [
        pytest.param('<form method="put"><input name="q" value="x"></form>', 'q=x', id='unknown-method-is-get'),
        pytest.param(
            '<form accept-charset="utf-16"><input name="q" value="é"></form>', 'q=%C3%A9', id='utf-16-submits-utf-8'
        ),
        pytest.param(
            '<form id="f"></form><form id="f"></form><input name="q" value="x" form="f">',
            'q=x',
            id='first-duplicate-id-wins',
        ),
        pytest.param(
            '<div id="f"></div><form id="f"><input name="q" value="x"></form><input name="z" value="1" form="f">',
            'q=x',
            id='first-id-not-a-form',
        ),
        pytest.param('<FORM><INPUT TYPE="IMAGE" NAME="q"></FORM>', 'q.x=0&q.y=0', id='uppercase-tags'),
        pytest.param(
            '<form id=""><input name="q" value="x"></form><input name="z" value="1" form="">',
            'q=x',
            id='empty-form-attribute',
        ),
        pytest.param('<form><input name="q" value="x"><button>Go</button></form>', 'q=x', id='unnamed-button'),
        pytest.param('<form><input type="image" name="map" src="go.png"></form>', 'map.x=0&map.y=0', id='image-button'),
        pytest.param('<form><input type="image" src="go.png"></form>', 'x=0&y=0', id='unnamed-image-button'),
    ],
)
async def test_browser_rules(extract_form_requests: ExtractFormRequests, html: str, expected_query: str) -> None:
    """Less obvious browser rules are followed."""
    [request] = await extract_form_requests(html)

    assert request.method == 'GET'
    assert urlsplit(request.url).query == expected_query


@pytest.mark.parametrize(
    ('html', 'expected_url'),
    [
        pytest.param('<form></form>', 'https://example.com/page/index.html', id='no-action'),
        pytest.param('<form action="../up"></form>', 'https://example.com/up', id='relative'),
        pytest.param('<form action=" /go "></form>', 'https://example.com/go', id='whitespace-around-action'),
        pytest.param(
            '<head><base href="https://other.com/dir/"></head><form action="go"></form>',
            'https://other.com/dir/go',
            id='base-href',
        ),
        pytest.param(
            '<head><base href="https://other.com/dir/"></head><form></form>',
            'https://example.com/page/index.html',
            id='base-href-no-action',
        ),
        pytest.param(
            '<head><base href="http://[bad"></head><form action="go"></form>',
            'https://example.com/page/go',
            id='invalid-base-href',
        ),
        pytest.param(
            '<head><base target="_blank"></head><form action="go"></form>',
            'https://example.com/page/go',
            id='base-without-href',
        ),
    ],
)
async def test_action_resolution(extract_form_requests: ExtractFormRequests, html: str, expected_url: str) -> None:
    """The action is resolved against `<base href>`, and a missing action submits to the page URL."""
    [request] = await extract_form_requests(html)

    assert request.url == expected_url


async def test_action_uses_loaded_url(extract_form_requests: ExtractFormRequests) -> None:
    """The action is resolved against the URL the page was loaded from after redirects."""
    page_request = Request.from_url(_PAGE_URL, loaded_url='https://example.com/moved/index.html')

    [request] = await extract_form_requests('<form action="go"></form>', page_request=page_request)

    assert request.url == 'https://example.com/moved/go'


async def test_unsubmittable_forms_skipped(extract_form_requests: ExtractFormRequests) -> None:
    """Dialog forms and forms whose action isn't a valid HTTP(S) URL are skipped."""
    html = (
        '<form action="javascript:void(0)"></form><form action="mailto:a@b.c"></form>'
        '<form action="http://[bad"></form><form action="https://exa mple.com/"></form>'
        '<form action="http:x"></form><form method="dialog"><button>Close</button></form><form action="/ok"></form>'
    )

    assert _paths(await extract_form_requests(html, all_forms=True)) == ['/ok']


async def test_request_options(extract_form_requests: ExtractFormRequests) -> None:
    """Request options are applied, and `headers` are merged over the ones the form sets."""
    html = '<form method="post" action="/login"><input name="q" value="x"></form>'
    headers = {'X-Custom': '1', 'referer': 'https://example.com/'}

    [request] = await extract_form_requests(html, headers=headers, label='detail', use_extended_unique_key=False)
    [get_request] = await extract_form_requests(html.replace('post', 'get'), headers=headers)

    assert request.label == 'detail'
    assert request.unique_key == 'https://example.com/login'
    assert request.headers['x-custom'] == '1'
    assert request.headers['content-type'] == 'application/x-www-form-urlencoded'
    assert request.headers['referer'] == 'https://example.com/'
    assert get_request.headers['referer'] == 'https://example.com/'


@pytest.mark.parametrize(
    ('page_url', 'html', 'expected_referer', 'expected_origin'),
    [
        pytest.param(
            _REFERRER_PAGE_URL,
            '<form method="post" action="/login">',
            'https://example.com/page?token=1',
            'https://example.com',
            id='same-origin',
        ),
        pytest.param(
            _REFERRER_PAGE_URL,
            '<form method="post" action="https://other.com/">',
            'https://example.com/',
            'https://example.com',
            id='other-host',
        ),
        pytest.param(
            _REFERRER_PAGE_URL,
            '<form method="post" action="https://example.com:8443/">',
            'https://example.com/',
            'https://example.com',
            id='other-port',
        ),
        pytest.param(
            'https://example.com:443/page',
            '<form method="post" action="/login">',
            'https://example.com/page',
            'https://example.com',
            id='default-port',
        ),
        pytest.param(
            'https://example.com',
            '<form method="post" action="/login">',
            'https://example.com/',
            'https://example.com',
            id='empty-path',
        ),
        pytest.param(
            _REFERRER_PAGE_URL,
            '<base href="https://other.com/"><form method="post" action="/login">',
            'https://example.com/',
            'https://example.com',
            id='base-href-ignored',
        ),
        pytest.param(
            _REFERRER_PAGE_URL, '<form method="post" action="http://example.com/">', None, 'null', id='https-to-http'
        ),
        pytest.param(
            'http://example.com/page',
            '<form method="post" action="https://example.com/">',
            'http://example.com/',
            'http://example.com',
            id='http-to-https',
        ),
        pytest.param(
            _REFERRER_PAGE_URL,
            '<form action="/search">',
            'https://example.com/page?token=1',
            None,
            id='get-without-origin',
        ),
        pytest.param(
            f'https://example.com/page?q={"a" * 4096}',
            '<form action="/search">',
            'https://example.com/',
            None,
            id='long-referrer-cut',
        ),
    ],
)
async def test_referrer_headers(
    extract_form_requests: ExtractFormRequests,
    page_url: str,
    html: str,
    expected_referer: str | None,
    expected_origin: str | None,
) -> None:
    """The `Referer` and `Origin` headers come from the page URL under the default referrer policy of browsers."""
    [request] = await extract_form_requests(f'{html}</form>', page_request=Request.from_url(page_url))

    assert request.headers.get('referer') == expected_referer
    assert request.headers.get('origin') == expected_origin


async def test_click_first_button(extract_form_requests: ExtractFormRequests) -> None:
    """The first enabled submit button is submitted in document order by default, and none with `click=False`."""
    html = """
    <form>
        <button type="button" name="plain" value="p">Plain</button>
        <input type="submit" name="off" value="o" disabled>
        <fieldset disabled><input type="submit" name="off" value="f"></fieldset>
        <button type="submit" name="go" value="first">Go</button>
        <input type="image" name="map" src="go.png">
        <input name="q" value="x">
    </form>
    """

    [request] = await extract_form_requests(html)
    [unclicked] = await extract_form_requests(html, click=False)

    assert urlsplit(request.url).query == 'go=first&q=x'
    assert urlsplit(unclicked.url).query == 'q=x'


async def test_click_button_overrides(extract_form_requests: ExtractFormRequests) -> None:
    """`click` picks the button by its attributes and the button overrides apply."""
    html = """
    <form action="/save" method="get">
        <input name="q" value="x">
        <input type="submit" name="action" value="save">
        <button name="action" value="delete" formaction="/delete" formmethod="post">Delete</button>
    </form>
    """

    [request] = await extract_form_requests(html, click={'value': 'delete'})

    assert request.method == 'POST'
    assert request.url == 'https://example.com/delete'
    assert _submitted_fields(request) == {'q': 'x', 'action': 'delete'}


async def test_click_disabled_button(extract_form_requests: ExtractFormRequests) -> None:
    """`click` picks a disabled button only if no enabled one matches."""
    html = (
        '<form><button name="go" value="back" disabled>Back</button><button name="go" value="next">Next</button></form>'
    )

    [enabled] = await extract_form_requests(html, click={'name': 'go'})
    [disabled] = await extract_form_requests(html, click={'value': 'back'})

    assert _submitted_fields(enabled) == {'go': 'next'}
    assert _submitted_fields(disabled) == {'go': 'back'}


@pytest.mark.parametrize(
    ('click', 'expected_paths'),
    [
        pytest.param({'name': 'log-in'}, ['/login'], id='by-name'),
        pytest.param({'id': 'find-button'}, ['/search'], id='by-id'),
        pytest.param({'name': 'missing'}, [], id='no-match'),
        pytest.param({}, ['/search'], id='any-button'),
    ],
)
async def test_click_picks_form(
    extract_form_requests: ExtractFormRequests, click: dict[str, str], expected_paths: list[str]
) -> None:
    """A `click` mapping submits the form with a matching button."""
    html = """
    <form action="/plain"><input name="q" value="y"></form>
    <form action="/search"><input name="q" value="x"><input type="submit" id="find-button" name="find"></form>
    <form action="/login"><input name="user" value="me"><button name="log-in">Log in</button></form>
    """

    assert _paths(await extract_form_requests(html, click=click)) == expected_paths


async def test_selector(extract_form_requests: ExtractFormRequests) -> None:
    """`selector` narrows the forms, skipping other elements it matches."""
    html = '<form id="a" action="/a"></form><div id="b"></div><form id="c" action="/c"></form>'

    assert _paths(await extract_form_requests(html, selector='#c, #b')) == ['/c']


@pytest.mark.parametrize(
    'selector',
    [
        pytest.param('#missing', id='no-match'),
        pytest.param('form::text, form::attr(action)', id='text-and-attributes'),
        pytest.param('form[', id='syntax-error'),
        pytest.param('form:unknown', id='unknown-pseudo-class'),
        pytest.param('ns|form', id='unknown-namespace'),
        pytest.param('#\\110000', id='escape-past-unicode'),
    ],
)
async def test_selector_without_forms(extract_form_requests: ExtractFormRequests, selector: str) -> None:
    """A selector that is invalid or matches no form yields no requests."""
    assert await extract_form_requests('<form action="/a">text</form>', selector=selector) == []


@pytest.mark.parametrize(
    ('html', 'fields', 'expected_paths'),
    [
        pytest.param(
            '<form action="/subscribe"><input name="email"><input name="name"><input name="city"></form>'
            '<form action="/login"><input name="email"><input name="password"></form>',
            {'email': 'me@example.com', 'password': 'secret'},
            ['/login'],
            id='most-shared-names',
        ),
        pytest.param(
            '<form action="/header"><input name="q"></form><form action="/footer"><input name="q"></form>',
            {'q': 'shoes'},
            ['/header'],
            id='tie-keeps-document-order',
        ),
        pytest.param(
            '<form action="/a"><input name="q"></form><form action="/b"><input name="user"></form>',
            {'token': 'x'},
            ['/a'],
            id='no-shared-name-first-form',
        ),
        pytest.param(
            '<form method="dialog"></form><form action="javascript:void(0)"></form><form action="/ok"></form>',
            {},
            ['/ok'],
            id='unsubmittable-forms-passed-over',
        ),
        pytest.param(
            '<form action="/search"><input name="q"></form>'
            '<form action="javascript:login()"><input name="username"><input name="password"></form>',
            {'username': 'me', 'password': 'secret'},
            [],
            id='best-form-unsubmittable',
        ),
    ],
)
async def test_form_choice(
    extract_form_requests: ExtractFormRequests, html: str, fields: dict[str, str], expected_paths: list[str]
) -> None:
    """The first submittable form among those sharing the most field names with `fields` is submitted."""
    assert _paths(await extract_form_requests(html, fields=fields)) == expected_paths


@pytest.mark.parametrize(
    'html',
    [
        pytest.param('<p>No forms here.</p>', id='no-form'),
        pytest.param('<!-- <form action="/x"><input name="q"></form> -->', id='commented-out-form'),
    ],
)
async def test_page_without_forms(extract_form_requests: ExtractFormRequests, html: str) -> None:
    """A page without forms yields no requests."""
    assert await extract_form_requests(html) == []


async def test_xml_declaration_page(extract_form_requests: ExtractFormRequests) -> None:
    """An HTML page starting with an XML declaration is read as HTML."""
    html = '<?xml version="1.0" encoding="UTF-8"?><html><body><form action="/ok"></form></body></html>'

    assert _paths(await extract_form_requests(html, content_type='text/html')) == ['/ok']


async def test_form_after_html_end(extract_form_requests: ExtractFormRequests) -> None:
    """A form after `</html>` doesn't shift the forms before it."""
    html = '<html><body><form id="ok" action="/ok"></form></body></html><form action="/after"></form>'

    # Newer libxml2 puts content after `</html>` into a second root, which isn't searched, older versions keep it.
    assert _paths(await extract_form_requests(html, all_forms=True)) in (['/ok'], ['/ok', '/after'])
    assert _paths(await extract_form_requests(html, selector='#ok')) == ['/ok']
    assert _paths(await extract_form_requests(html, selector='[action="/after"]')) in ([], ['/after'])


@pytest.mark.parametrize('parser', [pytest.param('lxml', id='lxml'), pytest.param('html5lib', id='html5lib')])
async def test_beautifulsoup_form_inside_select(parser: BeautifulSoupParserType) -> None:
    """Soup forms are matched to the right ones when the parser moves a form out of a `<select>`."""
    html = '<select><form action="/x"></form></select><form id="y" action="/y"></form>'

    # With a declared encoding, a soup built by lxml is reused.
    content_type = 'text/html; charset=utf-8'
    requests = await _extract_with_beautifulsoup(html, content_type=content_type, parser=parser, selector='#y')

    assert _paths(requests) == ['/y']


async def test_deeply_nested_form(extract_form_requests: ExtractFormRequests) -> None:
    """Fields of a form nested deeper than the default libxml2 limit are collected."""
    html = '<div>' * 300 + '<form><input name="q" value="x"></form>' + '</div>' * 300

    [request] = await extract_form_requests(html)

    assert _submitted_fields(request) == {'q': 'x'}


@pytest.mark.parametrize(
    ('attributes', 'content_type'),
    [
        pytest.param('accept-charset="bogus,windows-1250"', None, id='accept-charset'),
        pytest.param('', 'text/html; charset=windows-1250', id='header'),
    ],
)
@pytest.mark.parametrize('method', [pytest.param('get', id='get'), pytest.param('post', id='post')])
async def test_submission_encoding(
    extract_form_requests: ExtractFormRequests, attributes: str, content_type: str | None, method: str
) -> None:
    """Values are encoded in the form or page charset, with unsupported characters as character references."""
    html = f'<form method="{method}" {attributes}><input name="q"></form>'

    [request] = await extract_form_requests(html, content_type=content_type, fields={'q': 'Příliš ✓'})

    encoded = urlsplit(request.url).query if method == 'get' else (request.payload or b'').decode()
    assert encoded == 'q=P%F8%EDli%9A+%26%2310003%3B'


@pytest.mark.parametrize(
    ('html', 'value', 'encoding'),
    [
        pytest.param(f'<p>{_CZECH * 5}</p>', _CZECH, 'cp1250', id='guess'),
        # `BeautifulSoup` names this encoding `CP932`, which isn't a WHATWG label.
        pytest.param(f'<p>{_JAPANESE * 5}</p>', _JAPANESE, 'shift_jis', id='guess-outside-whatwg-labels'),
        pytest.param(f'<meta charset="base64"><p>{_CZECH * 5}</p>', _CZECH, 'cp1250', id='unknown-label'),
        pytest.param(f'<!-- <meta charset="utf-8"> --><p>{_CZECH * 5}</p>', _CZECH, 'cp1250', id='commented-out-meta'),
        pytest.param(
            f'<!-- <meta charset="utf-8"> --><p>{_CZECH * 5}</p>',
            _CZECH,
            'utf-8',
            id='commented-out-meta-same-as-guess',
        ),
        # `BeautifulSoup` looks for a `<meta>` in the first 5% of a page, so it takes a large one.
        pytest.param(
            f'<style>{"a" * 10_000}</style><meta charset="utf-8">{"<p>text</p>" * 20_000}',
            _CZECH,
            'utf-8',
            id='late-meta',
        ),
        pytest.param('', _CZECH, 'cp1252', id='ascii'),
    ],
)
async def test_beautifulsoup_undeclared_encoding(html: str, value: str, encoding: str) -> None:
    """A page declaring no encoding in the prescan submits in the one browsers would pick."""
    page = f'{html}<form><input name="q"></form>'

    [request] = await _extract_with_beautifulsoup(
        page, content_type='text/html', encoding=encoding, fields={'q': value}
    )

    assert urlsplit(request.url).query == urlencode({'q': value}, encoding=encoding, errors='xmlcharrefreplace')


async def test_beautifulsoup_guess_fallback() -> None:
    """A page whose guessed encoding Python can't decode submits in UTF-8."""
    detector = Mock(encodings=['EUC-TW'], declared_encoding=None)
    target = 'crawlee.crawlers._beautifulsoup._beautifulsoup_crawling_context.EncodingDetector'

    with patch(target, return_value=detector):
        [request] = await _extract_with_beautifulsoup(
            f'<form><input name="q" value="{_CZECH}"></form>', content_type='text/html'
        )

    assert urlsplit(request.url).query == urlencode({'q': _CZECH})


async def test_beautifulsoup_undeclared_selector() -> None:
    """A selector is matched on the page decoded as for the forms, not as `BeautifulSoup` decoded it."""
    # `BeautifulSoup` reads the page as UTF-7, which browsers don't know, and finds a form with the `first` ID.
    html = '<meta charset="utf-7">+ADw-form id=first+AD4-+ADw-/form+AD4-<form action="/login"></form>'

    assert await _extract_with_beautifulsoup(html, content_type='text/html', selector='#first') == []


async def test_parsel_undeclared_encoding() -> None:
    """A page declaring no encoding submits in UTF-8 with Parsel."""
    html = '<form><input name="q"></form>'

    [request] = await _extract_with_parsel(html, content_type='text/html', fields={'q': _CZECH})

    assert urlsplit(request.url).query == urlencode({'q': _CZECH})


async def test_parsel_json_response() -> None:
    """A JSON response yields no requests with Parsel."""
    body = '{"html": "<form action=\\"/a\\"></form>"}'

    assert await _extract_with_parsel(body, content_type='application/json') == []
