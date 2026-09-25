from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qsl, urlsplit

import pytest
from bs4 import BeautifulSoup
from parsel import Selector

from crawlee import HttpHeaders, Request
from crawlee._utils.forms import response_charset
from crawlee.crawlers import BeautifulSoupCrawlingContext, ParselCrawlingContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    FormRequests = Callable[..., Awaitable[list[Request]]]

_PAGE_URL = 'https://example.com/page/index.html'


def _context(html: str, page_request: Request | None, content_type: str | None, encoding: str | None) -> Mock:
    body = html.encode(encoding or response_charset(content_type) or 'utf-8', 'xmlcharrefreplace')
    return Mock(
        request=page_request or Request.from_url(_PAGE_URL),
        http_response=Mock(
            headers=HttpHeaders({'Content-Type': content_type} if content_type else {}),
            read=AsyncMock(return_value=body),
        ),
    )


async def _parsel_form_requests(
    html: str,
    page_request: Request | None = None,
    content_type: str | None = None,
    encoding: str | None = None,
    **kwargs: Any,
) -> list[Request]:
    context = _context(html, page_request, content_type, encoding)
    context.selector = Selector(text=html)
    return await ParselCrawlingContext.form_requests(context, **kwargs)


def _beautifulsoup_form_requests(parser: str) -> FormRequests:
    async def form_requests(
        html: str,
        page_request: Request | None = None,
        content_type: str | None = None,
        encoding: str | None = None,
        **kwargs: Any,
    ) -> list[Request]:
        context = _context(html, page_request, content_type, encoding)
        # Built from the raw body like `BeautifulSoupParser` does, so `original_encoding` is set.
        context.soup = BeautifulSoup(await context.http_response.read(), parser)
        return await BeautifulSoupCrawlingContext.form_requests(context, **kwargs)

    return form_requests


@pytest.fixture(
    params=[
        pytest.param(_parsel_form_requests, id='parsel'),
        pytest.param(_beautifulsoup_form_requests('lxml'), id='beautifulsoup-lxml'),
        pytest.param(_beautifulsoup_form_requests('html5lib'), id='beautifulsoup-html5lib'),
    ]
)
def form_requests(request: pytest.FixtureRequest) -> FormRequests:
    return request.param


def _form_values(request: Request) -> list[tuple[str, str]]:
    if request.method == 'GET':
        return parse_qsl(urlsplit(request.url).query, keep_blank_values=True)
    assert request.payload is not None
    return parse_qsl(request.payload.decode(), keep_blank_values=True)


async def _single(form_requests: FormRequests, html: str, **kwargs: Any) -> Request:
    [request] = await form_requests(html, **kwargs)
    return request


async def test_get_form_puts_fields_in_query(form_requests: FormRequests) -> None:
    """A GET form replaces the query of its action with the fields."""
    html = '<form action="/search?old=1"><input name="q" value="shoes"><input name="page" value="2"></form>'

    request = await _single(form_requests, html)

    assert request.method == 'GET'
    assert request.payload is None
    assert request.url == 'https://example.com/search?q=shoes&page=2'


async def test_post_form_is_urlencoded(form_requests: FormRequests) -> None:
    """A POST form sends the fields as an urlencoded body."""
    html = '<form method="post" action="login"><input name="user" value="a b"><input name="pass" value="&"></form>'

    request = await _single(form_requests, html)

    assert request.method == 'POST'
    assert request.url == 'https://example.com/page/login'
    assert request.headers['content-type'] == 'application/x-www-form-urlencoded'
    assert request.payload == b'user=a+b&pass=%26'


async def test_post_forms_with_different_data_are_distinct(form_requests: FormRequests) -> None:
    """POST requests to the same action with different data get different unique keys."""
    html = '<form method="post"><input name="page" value="1"></form>'

    first = await _single(form_requests, html)
    second = await _single(form_requests, html, form_data={'page': '2'})

    assert first.unique_key != second.unique_key


async def test_field_collection(form_requests: FormRequests) -> None:
    """Only the fields a browser would submit are collected."""
    html = """
    <form>
        <input name="text" value="t">
        <input type="checkbox" name="checked" value="c1" checked>
        <input type="checkbox" name="unchecked" value="c2">
        <input type="checkbox" name="no-value" checked>
        <input type="radio" name="radio" value="r1">
        <input type="radio" name="radio" value="r2" checked>
        <select name="first"><option value="o1">1</option><option value="o2">2</option></select>
        <select name="multi" multiple>
            <option value="m1" selected>1</option><option value="m2">2</option><option value="m3" selected>3</option>
        </select>
        <textarea name="area">long text</textarea>
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

    request = await _single(form_requests, html)

    assert _form_values(request) == [
        ('text', 't'),
        ('checked', 'c1'),
        ('no-value', 'on'),
        ('radio', 'r2'),
        ('first', 'o1'),
        ('multi', 'm1'),
        ('multi', 'm3'),
        ('area', 'long text'),
        ('file', ''),
        ('empty', ''),
        ('hidden', ''),
    ]


async def test_select_options(form_requests: FormRequests) -> None:
    """A select submits its last selected option, or the first enabled one when none is selected."""
    html = """
    <form>
        <select name="last"><option value="a" selected>A</option><option value="b" selected>B</option></select>
        <select name="first-enabled"><option value="off" disabled>Off</option><option value="on">On</option></select>
        <select name="empty"></select>
    </form>
    """

    request = await _single(form_requests, html)

    assert _form_values(request) == [('last', 'b'), ('first-enabled', 'on')]


async def test_fields_linked_by_form_attribute(form_requests: FormRequests) -> None:
    """Fields are assigned to forms by their `form` attribute."""
    html = """
    <form id="search" action="/search"><input name="q" value="x"><input name="other" value="o" form="login"></form>
    <select name="lang" form="search"><option value="en" selected>EN</option></select>
    <button form="search" name="go" value="1">Go</button>
    <input name="orphan" value="z" form="missing">
    <form id="login" action="/login"></form>
    """

    search, login = await form_requests(html)

    assert _form_values(search) == [('q', 'x'), ('lang', 'en'), ('go', '1')]
    assert _form_values(login) == [('other', 'o')]


async def test_disabled_fieldset_around_form(form_requests: FormRequests) -> None:
    """A disabled `<fieldset>` around the form disables its fields."""
    html = '<fieldset disabled><form><input name="a" value="1"></form></fieldset>'

    request = await _single(form_requests, html)

    assert _form_values(request) == []


async def test_dialog_form_is_skipped(form_requests: FormRequests) -> None:
    """Dialog forms send no request."""
    html = '<dialog open><form method="dialog"><button>Close</button></form></dialog><form action="/ok"></form>'

    assert [urlsplit(request.url).path for request in await form_requests(html)] == ['/ok']


@pytest.mark.parametrize(
    ('html', 'content_type', 'encoding'),
    [
        pytest.param(
            '<form accept-charset="bogus windows-1251"><input name="q" value="{value}"></form>',
            None,
            'utf-8',
            id='accept-charset',
        ),
        pytest.param(
            '<form><input name="q" value="{value}"></form>',
            'text/html; charset=windows-1251',
            'windows-1251',
            id='header',
        ),
        pytest.param(
            '<head><meta charset="windows-1251"></head><form><input name="q" value="{value}"></form>',
            None,
            'windows-1251',
            id='meta-charset',
        ),
        pytest.param(
            '<head><meta http-equiv="Content-Type" content="text/html; charset=windows-1251"></head>'
            '<form><input name="q" value="{value}"></form>',
            None,
            'windows-1251',
            id='meta-http-equiv',
        ),
    ],
)
@pytest.mark.parametrize('method', [pytest.param('get', id='get'), pytest.param('post', id='post')])
async def test_form_encoding(
    form_requests: FormRequests, html: str, content_type: str | None, encoding: str, method: str
) -> None:
    """Values are encoded in the form or page charset, with unsupported characters as character references."""
    html = html.format(value='привіт ✓').replace('<form', f'<form method="{method}"', 1)

    request = await _single(form_requests, html, content_type=content_type, encoding=encoding)

    encoded = request.url.split('?')[1] if method == 'get' else (request.payload or b'').decode()
    assert encoded == 'q=%EF%F0%E8%E2%B3%F2+%26%2310003%3B'


async def test_form_data_overrides_fields(form_requests: FormRequests) -> None:
    """Values from `form_data` replace, drop and add fields."""
    html = '<form><input name="keep" value="k"><input name="replace" value="old"><input name="drop" value="d"></form>'

    request = await _single(form_requests, html, form_data={'replace': 'new', 'drop': None, 'tags': ['a', 'b']})

    assert _form_values(request) == [('keep', 'k'), ('replace', 'new'), ('tags', 'a'), ('tags', 'b')]


async def test_first_submit_button_is_clicked(form_requests: FormRequests) -> None:
    """The first enabled submit button is included by default."""
    html = """
    <form>
        <input name="q" value="x">
        <button type="button" name="plain" value="p">Plain</button>
        <input type="submit" name="off" value="o" disabled>
        <fieldset disabled><input type="submit" name="off" value="f"></fieldset>
        <button name="go" value="first">Go</button>
        <input type="submit" name="go" value="second">
    </form>
    """

    request = await _single(form_requests, html)

    assert _form_values(request) == [('q', 'x'), ('go', 'first')]


async def test_click_data_selects_button(form_requests: FormRequests) -> None:
    """`click_data` picks the button to click and its form overrides apply."""
    html = """
    <form action="/save" method="get">
        <input name="q" value="x">
        <input type="submit" name="action" value="save">
        <button name="action" value="delete" formaction="/delete" formmethod="post">Delete</button>
    </form>
    """

    request = await _single(form_requests, html, click_data={'value': 'delete'})

    assert request.method == 'POST'
    assert request.url == 'https://example.com/delete'
    assert _form_values(request) == [('q', 'x'), ('action', 'delete')]


async def test_dont_click(form_requests: FormRequests) -> None:
    """`dont_click` leaves out every button."""
    html = '<form><input name="q" value="x"><input type="submit" name="go" value="1"></form>'

    request = await _single(form_requests, html, dont_click=True)

    assert _form_values(request) == [('q', 'x')]


async def test_image_button_sends_coordinates(form_requests: FormRequests) -> None:
    """A clicked image button sends its click coordinates."""
    html = '<form><input type="image" name="map" src="map.png"></form>'

    request = await _single(form_requests, html)

    assert _form_values(request) == [('map.x', '0'), ('map.y', '0')]


async def test_click_data_skips_unmatched_forms(form_requests: FormRequests) -> None:
    """Forms with no button matching `click_data` are skipped."""
    html = '<form action="/search"><input name="q"></form><form action="/save"><button name="go">Go</button></form>'

    assert [urlsplit(request.url).path for request in await form_requests(html, click_data={'name': 'go'})] == ['/save']
    assert await form_requests(html, click_data={'name': 'missing'}) == []


async def test_non_http_action_is_skipped(form_requests: FormRequests) -> None:
    """Forms whose action isn't a valid HTTP(S) URL are skipped."""
    html = (
        '<form action="javascript:void(0)"></form><form action="mailto:a@b.c"></form>'
        '<form action="http://[bad"></form><form action="/ok"></form>'
    )

    assert [urlsplit(request.url).path for request in await form_requests(html)] == ['/ok']


async def test_multipart_form(form_requests: FormRequests) -> None:
    """A multipart form is encoded with a deterministic boundary and empty file parts."""
    html = (
        '<form method="post" enctype="multipart/form-data">'
        '<input name="title" value="hi"><input type="file" name="doc">'
        '</form>'
    )

    request = await _single(form_requests, html)
    again = await _single(form_requests, html)

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


async def test_text_plain_form(form_requests: FormRequests) -> None:
    """A `text/plain` form sends one field per line."""
    html = '<form method="post" enctype="text/plain"><input name="a" value="1"><input name="b" value="2"></form>'

    request = await _single(form_requests, html)

    assert request.headers['content-type'] == 'text/plain'
    assert request.payload == b'a=1\r\nb=2\r\n'


@pytest.mark.parametrize(
    ('html', 'expected_url'),
    [
        pytest.param('<form></form>', 'https://example.com/page/index.html', id='no-action'),
        pytest.param('<form action="../up"></form>', 'https://example.com/up', id='relative'),
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
    ],
)
async def test_action_resolution(form_requests: FormRequests, html: str, expected_url: str) -> None:
    """The action is resolved against `<base href>`, and a missing action submits to the page URL."""
    request = await _single(form_requests, html)

    assert request.url == expected_url


async def test_action_resolves_against_loaded_url(form_requests: FormRequests) -> None:
    """The action is resolved against the URL the page was loaded from after redirects."""
    page_request = Request.from_url(_PAGE_URL, loaded_url='https://example.com/moved/index.html')

    request = await _single(form_requests, '<form action="go"></form>', page_request=page_request)

    assert request.url == 'https://example.com/moved/go'


async def test_textarea_leading_newline(form_requests: FormRequests) -> None:
    """The newline right after `<textarea>` is dropped."""
    request = await _single(form_requests, '<form><textarea name="a">\n\nhello</textarea></form>')

    assert _form_values(request) == [('a', '\nhello')]


async def test_selector_filters_forms(form_requests: FormRequests) -> None:
    """All forms are used by default and `selector` narrows them."""
    html = '<form id="a" action="/a"></form><div id="b"></div><form id="c" action="/c"></form>'

    assert [urlsplit(request.url).path for request in await form_requests(html)] == ['/a', '/c']
    assert [urlsplit(request.url).path for request in await form_requests(html, selector='#c, #b')] == ['/c']


async def test_request_options_are_passed(form_requests: FormRequests) -> None:
    """Headers and request options are applied to the request."""
    html = '<form method="post"><input name="q" value="x"></form>'

    request = await _single(form_requests, html, headers={'X-Custom': '1'}, label='detail')

    assert request.headers['x-custom'] == '1'
    assert request.headers['content-type'] == 'application/x-www-form-urlencoded'
    assert request.label == 'detail'


async def test_xml_declaration(form_requests: FormRequests) -> None:
    """Pages starting with an XML declaration are supported."""
    html = '<?xml version="1.0" encoding="UTF-8"?><html><body><form action="/ok"></form></body></html>'

    assert [urlsplit(request.url).path for request in await form_requests(html)] == ['/ok']


@pytest.mark.parametrize(
    ('charset', 'value', 'expected_query'),
    [
        pytest.param('us-ascii', 'é€', 'q=%E9%80', id='ascii'),
        pytest.param('iso-8859-1', 'é€', 'q=%E9%80', id='latin1'),
        pytest.param('shift_jis', '①', 'q=%87%40', id='shift-jis'),
    ],
)
async def test_legacy_charset_uses_superset(
    form_requests: FormRequests, charset: str, value: str, expected_query: str
) -> None:
    """Legacy charsets are submitted in the superset browsers use instead."""
    html = f'<form><input name="q" value="{value}"></form>'

    request = await _single(form_requests, html, content_type=f'text/html; charset={charset}')

    assert request.url.split('?')[1] == expected_query


@pytest.mark.parametrize(
    'charset',
    [
        pytest.param('base64', id='binary-codec'),
        pytest.param('undefined', id='undefined'),
        pytest.param('idna', id='idna'),
    ],
)
async def test_non_text_charset_is_ignored(form_requests: FormRequests, charset: str) -> None:
    """Charsets naming codecs that can't encode form data fall back to UTF-8."""
    html = f'<form accept-charset="{charset}"><input name="q" value="é"></form>'

    request = await _single(form_requests, html)

    assert request.url == 'https://example.com/page/index.html?q=%C3%A9'
