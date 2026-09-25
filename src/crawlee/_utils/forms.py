from __future__ import annotations

import codecs
import re
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, NamedTuple, TypedDict
from urllib.parse import urlencode

from lxml.html import HTMLParser, document_fromstring
from yarl import URL

from crawlee._request import Request
from crawlee._types import HttpHeaders
from crawlee._utils.crypto import compute_short_hash
from crawlee._utils.urls import convert_to_absolute_url

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from lxml.html import HtmlElement
    from typing_extensions import NotRequired, Unpack

    from crawlee._types import EnqueueStrategy, HttpMethod, JsonSerializable

_CHARSET_PATTERN = re.compile(r'charset\s*=\s*["\']?([^"\'\s;]+)', re.IGNORECASE)

# Legacy charsets browsers replace with a superset, per the WHATWG Encoding Standard.
_ENCODING_SUPERSETS = {
    'ascii': 'cp1252',
    'iso8859-1': 'cp1252',
    'iso8859-9': 'cp1254',
    'iso8859-11': 'cp874',
    'tis-620': 'cp874',
    'gb2312': 'gbk',
    'shift_jis': 'cp932',
    'euc_kr': 'cp949',
}

_FIELD_TAGS = ('input', 'button', 'select', 'textarea')
_BUTTON_INPUT_TYPES = ('submit', 'image', 'reset', 'button')

# Browsers percent-encode these characters in multipart field names.
_MULTIPART_NAME_ESCAPES = str.maketrans({'"': '%22', '\r': '%0D', '\n': '%0A'})


class FormRequestOptions(TypedDict):
    """Options for the `Request` created from a form, other than the URL, method and payload taken from the form."""

    label: NotRequired[str | None]
    session_id: NotRequired[str | None]
    unique_key: NotRequired[str | None]
    keep_url_fragment: NotRequired[bool]
    use_extended_unique_key: NotRequired[bool]
    always_enqueue: NotRequired[bool]
    user_data: NotRequired[Mapping[str, JsonSerializable]]
    no_retry: NotRequired[bool]
    enqueue_strategy: NotRequired[EnqueueStrategy]
    max_retries: NotRequired[int | None]


def forms_to_requests(
    forms: Iterable[HtmlElement],
    page_url: str,
    content_type: str | None,
    *,
    form_data: Mapping[str, str | Sequence[str] | None] | None = None,
    click_data: Mapping[str, str] | None = None,
    dont_click: bool = False,
    headers: HttpHeaders | dict[str, str] | None = None,
    **kwargs: Unpack[FormRequestOptions],
) -> list[Request]:
    """Create a `Request` submitting each form the way a browser does.

    Forms with no submit button matching `click_data`, dialog forms and forms not submitting over HTTP(S) are skipped.

    Args:
        forms: The form elements, each within the lxml tree of the whole page.
        page_url: The URL of the page.
        content_type: The `Content-Type` header of the page response, used to find the page encoding.
        form_data: Field values overriding those in the form. A `None` value drops the field.
        click_data: Attributes identifying the submit button to click. Defaults to the first one.
        dont_click: Submit the form without clicking any button.
        headers: The HTTP headers of the request. The `Content-Type` of the form is added to them.
        **kwargs: Additional options passed to `Request.from_url`.
    """
    forms = list(forms)
    if not forms:
        return []

    page = _analyze_page(forms[0].getroottree().getroot(), page_url, content_type)

    requests = []
    for form in forms:
        request = _form_to_request(
            form,
            page,
            form_data=form_data,
            click_data=click_data,
            dont_click=dont_click,
            headers=headers,
            **kwargs,
        )
        if request is not None:
            requests.append(request)
    return requests


def parse_html(body: bytes, encoding: str | None) -> HtmlElement:
    """Parse a page with lxml, decoding it with the given encoding, or the one lxml detects if it's `None`."""
    try:
        parser = HTMLParser(encoding=encoding)
    except LookupError:
        # A codec Python knows but libxml2 doesn't, so let lxml detect the encoding itself.
        parser = HTMLParser()
    return document_fromstring(body, parser=parser)


def response_charset(content_type: str | None) -> str | None:
    """Get the charset from a `Content-Type` header, if it names a known one."""
    charset = _find_charset(content_type)
    if charset is None:
        return None
    with suppress(LookupError):
        return codecs.lookup(charset).name
    return None


class _Field(NamedTuple):
    """A single entry the form submits."""

    name: str
    value: str
    is_file: bool = False


@dataclass
class _Page:
    """The parts of a page shared by all its forms, computed in a single pass over the document."""

    url: str
    base_url: str
    charsets: list[str]
    """The page encoding candidates, from the `Content-Type` header and then the `<meta>` tags."""
    elements_by_form: dict[HtmlElement, list[HtmlElement]]
    """The fields and buttons belonging to each form, in document order."""


def _analyze_page(root: HtmlElement, page_url: str, content_type: str | None) -> _Page:
    """Resolve the base URL, encoding candidates and form owners of all fields on the page."""
    try:
        base_url = convert_to_absolute_url(page_url, root.xpath('string(//base[@href][1]/@href)'))
    except ValueError:
        base_url = page_url

    # The first element with a given ID wins, as in `getElementById`.
    elements_by_id: dict[str, HtmlElement] = {}
    for element in root.xpath('//*[@id]'):
        elements_by_id.setdefault(element.get('id'), element)

    elements_by_form: dict[HtmlElement, list[HtmlElement]] = {}
    for element in root.iter(*_FIELD_TAGS):
        owner = _form_owner(element, elements_by_id)
        if owner is not None:
            elements_by_form.setdefault(owner, []).append(element)

    return _Page(
        url=page_url,
        base_url=base_url,
        charsets=_page_charsets(root, content_type),
        elements_by_form=elements_by_form,
    )


def _page_charsets(root: HtmlElement, content_type: str | None) -> list[str]:
    """Collect the charsets the page declares, in the order browsers trust them."""
    charsets: list[str] = []

    header_charset = _find_charset(content_type)
    if header_charset is not None:
        charsets.append(header_charset)

    charsets.extend(root.xpath('//meta[@charset]/@charset'))

    http_equiv_contents = root.xpath(
        '//meta[translate(@http-equiv, "CONTENT-TYP", "content-typ")="content-type"]/@content'
    )
    for content in http_equiv_contents:
        meta_charset = _find_charset(content)
        if meta_charset is not None:
            charsets.append(meta_charset)

    return charsets


def _find_charset(value: str | None) -> str | None:
    """Find the `charset=` parameter in a `Content-Type` value."""
    if not value:
        return None
    match = _CHARSET_PATTERN.search(value)
    return match.group(1) if match else None


def _form_to_request(
    form: HtmlElement,
    page: _Page,
    *,
    form_data: Mapping[str, str | Sequence[str] | None] | None,
    click_data: Mapping[str, str] | None,
    dont_click: bool,
    headers: HttpHeaders | dict[str, str] | None,
    **kwargs: Unpack[FormRequestOptions],
) -> Request | None:
    """Create a `Request` submitting a single form, or `None` if a browser wouldn't send one."""
    elements = page.elements_by_form.get(form, [])

    if dont_click:
        button = None
    else:
        button = _find_clickable(elements, click_data)
        if click_data and button is None:
            return None

    method = (_submission_attribute(form, button, 'method') or 'get').upper()
    enctype = _submission_attribute(form, button, 'enctype').lower()
    action = _submission_attribute(form, button, 'action').strip()

    # A dialog form only closes its `<dialog>` on the client.
    if method == 'DIALOG':
        return None

    url = _resolve_action(page, action)
    if url is None:
        return None

    fields = _collect_fields(elements, button, form_data)
    encoding = _form_encoding(form, page.charsets)
    request_headers = HttpHeaders(headers or {})

    if method != 'POST':
        get_url = _url_with_query(url, fields, encoding)
        return Request.from_url(get_url, method='GET', headers=request_headers, **kwargs)

    payload, content_type = _encode_body(fields, enctype, encoding)
    request_method: HttpMethod = 'POST'
    kwargs.setdefault('use_extended_unique_key', True)
    return Request.from_url(
        url,
        method=request_method,
        headers=HttpHeaders({'Content-Type': content_type}) | request_headers,
        payload=payload,
        **kwargs,
    )


def _submission_attribute(form: HtmlElement, button: HtmlElement | None, name: str) -> str:
    """Get a form attribute like `action`, which the clicked button can override with its `form*` counterpart."""
    if button is not None:
        button_value = button.get(f'form{name}')
        if button_value:
            return button_value
    return form.get(name) or ''


def _resolve_action(page: _Page, action: str) -> str | None:
    """Resolve the form action to an absolute URL, or `None` if it can't be submitted over HTTP(S)."""
    if not action:
        return page.url

    try:
        url = convert_to_absolute_url(page.base_url, action)
    except ValueError:
        return None

    if URL(url).scheme not in ('http', 'https'):
        return None
    return url


def _url_with_query(url: str, fields: list[_Field], encoding: str) -> str:
    """Replace the query of the URL with the fields, as a GET form does."""
    query = urlencode([(field.name, field.value) for field in fields], encoding=encoding, errors='xmlcharrefreplace')
    parsed = URL(url)
    # Build the URL from raw parts, as `with_query` would encode the query again as UTF-8.
    return str(
        URL.build(
            scheme=parsed.scheme,
            authority=parsed.raw_authority,
            path=parsed.raw_path,
            query_string=query,
            fragment=parsed.raw_fragment,
            encoded=True,
        )
    )


def _encode_body(fields: list[_Field], enctype: str, encoding: str) -> tuple[bytes, str]:
    """Encode the fields as a POST body, returning it with its `Content-Type` header value."""
    if enctype == 'multipart/form-data':
        return _encode_multipart(fields, encoding)

    if enctype == 'text/plain':
        text = ''.join(f'{field.name}={field.value}\r\n' for field in fields)
        return text.encode(encoding, 'xmlcharrefreplace'), 'text/plain'

    pairs = [(field.name, field.value) for field in fields]
    body = urlencode(pairs, encoding=encoding, errors='xmlcharrefreplace').encode()
    return body, 'application/x-www-form-urlencoded'


def _form_owner(element: HtmlElement, elements_by_id: Mapping[str, HtmlElement]) -> HtmlElement | None:
    """Get the form an element belongs to, honouring its `form` attribute."""
    form_id = element.get('form')
    if form_id is None:
        return next(element.iterancestors('form'), None)

    owner = elements_by_id.get(form_id)
    if owner is None or owner.tag != 'form':
        return None
    return owner


def _find_clickable(elements: list[HtmlElement], click_data: Mapping[str, str] | None) -> HtmlElement | None:
    """Find the submit button to click, matching all attributes in `click_data`."""
    for element in elements:
        if not _is_submit_button(element) or _is_disabled(element):
            continue
        if not click_data:
            return element
        if all(element.get(key) == value for key, value in click_data.items()):
            return element
    return None


def _is_submit_button(element: HtmlElement) -> bool:
    """Check whether the element submits the form when clicked."""
    button_type = element.get('type', '').lower()
    if element.tag == 'button':
        return button_type in ('', 'submit')
    if element.tag == 'input':
        return button_type in ('submit', 'image')
    return False


def _is_disabled(element: HtmlElement) -> bool:
    """Check whether the element, or a `<fieldset>` containing it, is disabled."""
    if 'disabled' in element.attrib:
        return True
    return any('disabled' in fieldset.attrib for fieldset in element.iterancestors('fieldset'))


def _collect_fields(
    elements: list[HtmlElement],
    button: HtmlElement | None,
    form_data: Mapping[str, str | Sequence[str] | None] | None,
) -> list[_Field]:
    """Collect the entries the form submits: its fields, the clicked button and the `form_data` overrides."""
    fields: list[_Field] = []
    for element in elements:
        if element.get('name') and not _is_disabled(element):
            fields.extend(_element_fields(element))

    if button is not None:
        fields.extend(_button_fields(button))

    if form_data:
        fields = _apply_form_data(fields, form_data)

    return fields


def _element_fields(element: HtmlElement) -> list[_Field]:
    """Get the entries a single enabled, named field submits."""
    name = element.get('name')

    if element.tag == 'select':
        values = element.value if element.multiple else [element.value]
        return [_Field(name, value) for value in values if value is not None]

    if element.tag == 'textarea':
        # Browsers drop the newline right after `<textarea>`, lxml keeps it.
        return [_Field(name, element.value.removeprefix('\n'))]

    # Buttons submit only when clicked, see `_button_fields`.
    if element.tag != 'input' or element.type in _BUTTON_INPUT_TYPES:
        return []

    if element.type == 'file':
        return [_Field(name, '', is_file=True)]

    if element.checkable and not element.checked:
        return []

    return [_Field(name, element.value or '')]


def _button_fields(button: HtmlElement) -> list[_Field]:
    """Get the entries the clicked button adds."""
    name = button.get('name')

    # An image button sends the click coordinates instead of its value.
    if button.tag == 'input' and button.get('type', '').lower() == 'image':
        prefix = f'{name}.' if name else ''
        return [_Field(f'{prefix}x', '0'), _Field(f'{prefix}y', '0')]

    if name:
        return [_Field(name, button.get('value', ''))]
    return []


def _apply_form_data(fields: list[_Field], form_data: Mapping[str, str | Sequence[str] | None]) -> list[_Field]:
    """Replace the fields named in `form_data` with its values, dropping those set to `None`."""
    result = [field for field in fields if field.name not in form_data]
    for name, value in form_data.items():
        if value is None:
            continue
        values = [value] if isinstance(value, str) else value
        result.extend(_Field(name, item) for item in values)
    return result


def _form_encoding(form: HtmlElement, page_charsets: list[str]) -> str:
    """Pick the encoding a browser submits the form in: `accept-charset`, then the page encoding, then UTF-8."""
    accept_charsets = (form.get('accept-charset') or '').replace(',', ' ').split()

    for candidate in [*accept_charsets, *page_charsets]:
        with suppress(LookupError, UnicodeError):
            name = codecs.lookup(candidate).name
            # Skip codecs that can't encode text with character references, like `base64` or `idna`.
            ''.encode(name, 'xmlcharrefreplace')
            return _ENCODING_SUPERSETS.get(name, name)

    return 'utf-8'


def _encode_multipart(fields: list[_Field], encoding: str) -> tuple[bytes, str]:
    """Encode fields as `multipart/form-data`, returning the body and the `Content-Type` header value."""
    parts: list[bytes] = []
    for field in fields:
        disposition = f'form-data; name="{field.name.translate(_MULTIPART_NAME_ESCAPES)}"'
        if field.is_file:
            head = f'Content-Disposition: {disposition}; filename=""\r\nContent-Type: application/octet-stream\r\n\r\n'
        else:
            head = f'Content-Disposition: {disposition}\r\n\r\n'
        parts.append((head + field.value).encode(encoding, 'xmlcharrefreplace'))

    # A random boundary would change the payload on every call and break deduplication.
    boundary = f'----CrawleeFormBoundary{compute_short_hash(b"".join(parts), length=16)}'
    delimiter = f'--{boundary}\r\n'.encode()
    body = b''.join(delimiter + part + b'\r\n' for part in parts) + f'--{boundary}--\r\n'.encode()
    return body, f'multipart/form-data; boundary={boundary}'
