"""HTML utility functions for Crawlee."""

from __future__ import annotations

import codecs
import re
from typing import TYPE_CHECKING, NamedTuple, TypedDict
from urllib.parse import urlencode, urlsplit

from yarl import URL

from crawlee._request import Request
from crawlee._types import HttpHeaders
from crawlee._utils.crypto import compute_short_hash
from crawlee._utils.http import parse_content_type_charset
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute, validate_http_url

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from lxml.html import HtmlElement
    from typing_extensions import NotRequired, Unpack

    from crawlee._types import JsonSerializable

# Matches the `encoding` of an XML declaration, which XHTML pages may use instead of a `<meta>` tag.
_XML_ENCODING_PATTERN = re.compile(rb'^\s*<\?xml[^>]*\sencoding\s*=\s*["\']([a-z0-9_:.+-]+)', re.IGNORECASE)

# A quoted attribute value may contain `>`, so it doesn't end the tag. A tag cut off by the prescan end doesn't match.
_META_TAG_PATTERN = re.compile(rb'<meta[\s/]((?:[^>"\']|"[^"]*"|\'[^\']*\')*)>', re.IGNORECASE)

_ATTRIBUTE_PATTERN = re.compile(rb'([^\s/>=]+)(?:\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]*)))?')

# Comments are skipped, so a commented-out `<meta>` tag doesn't count. An unclosed comment runs to the prescan end.
_HTML_COMMENT_PATTERN = re.compile(rb'<!--.*?(?:-->|\Z)', re.DOTALL)

# How much of the body is searched for a declared encoding. Browsers prescan 1024 bytes but still switch to a
# `<meta>` charset found later, so a larger window catches pages with a long `<head>` before it.
_PRESCAN_BYTES = 4096

_BOMS = (
    (codecs.BOM_UTF8, 'utf-8'),
    (codecs.BOM_UTF16_LE, 'utf-16-le'),
    (codecs.BOM_UTF16_BE, 'utf-16-be'),
)

# The encoding labels of the WHATWG Encoding Standard (https://encoding.spec.whatwg.org/#names-and-labels), grouped
# by the Python codec that decodes them. Labels outside it, like `utf-7` or Python-only codecs, are ignored as browsers
# do. The `replacement` and `x-user-defined` encodings have no Python codec and are left out.
_WHATWG_ENCODING_LABELS = {
    'big5hkscs': 'big5 big5-hkscs cn-big5 csbig5 x-x-big5',
    'cp874': 'dos-874 iso-8859-11 iso8859-11 iso885911 tis-620 windows-874',
    'cp932': 'csshiftjis ms932 ms_kanji shift-jis shift_jis sjis windows-31j x-sjis',
    'cp949': (
        'cseuckr csksc56011987 euc-kr iso-ir-149 korean ks_c_5601-1987 ks_c_5601-1989 ksc5601 ksc_5601 windows-949'
    ),
    'euc-jp': 'cseucpkdfmtjapanese euc-jp x-euc-jp',
    # WHATWG decodes GBK with the gb18030 decoder, its superset.
    'gb18030': 'chinese csgb2312 csiso58gb231280 gb18030 gb2312 gb_2312 gb_2312-80 gbk iso-ir-58 x-gbk',
    'ibm866': '866 cp866 csibm866 ibm866',
    'iso-2022-jp': 'csiso2022jp iso-2022-jp',
    'iso-8859-10': 'csisolatin6 iso-8859-10 iso-ir-157 iso8859-10 iso885910 l6 latin6',
    'iso-8859-13': 'iso-8859-13 iso8859-13 iso885913',
    'iso-8859-14': 'iso-8859-14 iso8859-14 iso885914',
    'iso-8859-15': 'csisolatin9 iso-8859-15 iso8859-15 iso885915 iso_8859-15 l9',
    'iso-8859-16': 'iso-8859-16',
    'iso-8859-2': 'csisolatin2 iso-8859-2 iso-ir-101 iso8859-2 iso88592 iso_8859-2 iso_8859-2:1987 l2 latin2',
    'iso-8859-3': 'csisolatin3 iso-8859-3 iso-ir-109 iso8859-3 iso88593 iso_8859-3 iso_8859-3:1988 l3 latin3',
    'iso-8859-4': 'csisolatin4 iso-8859-4 iso-ir-110 iso8859-4 iso88594 iso_8859-4 iso_8859-4:1988 l4 latin4',
    'iso-8859-5': 'csisolatincyrillic cyrillic iso-8859-5 iso-ir-144 iso8859-5 iso88595 iso_8859-5 iso_8859-5:1988',
    'iso-8859-6': (
        'arabic asmo-708 csiso88596e csiso88596i csisolatinarabic ecma-114 iso-8859-6 iso-8859-6-e '
        'iso-8859-6-i iso-ir-127 iso8859-6 iso88596 iso_8859-6 iso_8859-6:1987'
    ),
    'iso-8859-7': (
        'csisolatingreek ecma-118 elot_928 greek greek8 iso-8859-7 iso-ir-126 iso8859-7 iso88597 iso_8859-7 '
        'iso_8859-7:1987 sun_eu_greek'
    ),
    'iso-8859-8': (
        'csiso88598e csiso88598i csisolatinhebrew hebrew iso-8859-8 iso-8859-8-e iso-8859-8-i iso-ir-138 '
        'iso8859-8 iso88598 iso_8859-8 iso_8859-8:1988 logical visual'
    ),
    'koi8-r': 'cskoi8r koi koi8 koi8-r koi8_r',
    'koi8-u': 'koi8-ru koi8-u',
    'mac-cyrillic': 'x-mac-cyrillic x-mac-ukrainian',
    'mac-roman': 'csmacintosh mac macintosh x-mac-roman',
    'utf-16be': 'unicodefffe utf-16be',
    'utf-16le': 'csunicode iso-10646-ucs-2 ucs-2 unicode unicodefeff utf-16 utf-16le',
    'utf-8': 'unicode-1-1-utf-8 unicode11utf8 unicode20utf8 utf-8 utf8 x-unicode20utf8',
    'windows-1250': 'cp1250 windows-1250 x-cp1250',
    'windows-1251': 'cp1251 windows-1251 x-cp1251',
    'windows-1252': (
        'ansi_x3.4-1968 ascii cp1252 cp819 csisolatin1 ibm819 iso-8859-1 iso-ir-100 iso8859-1 iso88591 '
        'iso_8859-1 iso_8859-1:1987 l1 latin1 us-ascii windows-1252 x-cp1252'
    ),
    'windows-1253': 'cp1253 windows-1253 x-cp1253',
    'windows-1254': (
        'cp1254 csisolatin5 iso-8859-9 iso-ir-148 iso8859-9 iso88599 iso_8859-9 iso_8859-9:1989 l5 latin5 '
        'windows-1254 x-cp1254'
    ),
    'windows-1255': 'cp1255 windows-1255 x-cp1255',
    'windows-1256': 'cp1256 windows-1256 x-cp1256',
    'windows-1257': 'cp1257 windows-1257 x-cp1257',
    'windows-1258': 'cp1258 windows-1258 x-cp1258',
}

_ENCODING_BY_LABEL = {label: codec for codec, labels in _WHATWG_ENCODING_LABELS.items() for label in labels.split()}

_FIELD_TAGS = ('input', 'button', 'select', 'textarea')
_BUTTON_INPUT_TYPES = ('submit', 'image', 'reset', 'button')

# Browsers cut a longer referrer down to the origin.
_MAX_REFERRER_LENGTH = 4096


class FormRequestOptions(TypedDict):
    """Options for the `Request` created from a form.

    Mirrors `RequestOptions` without the URL, method and payload, which come from the form, without `id` and
    `unique_key`, which can't be shared by several forms, and without `enqueue_strategy`, which enqueuing sets.
    """

    label: NotRequired[str | None]
    """A label routing the request to a specific handler."""

    headers: NotRequired[HttpHeaders | dict[str, str] | None]
    """HTTP headers of the request, replacing those the form sets, like `Content-Type` or `Referer`."""

    session_id: NotRequired[str | None]
    """ID of the `Session` the request is bound to."""

    keep_url_fragment: NotRequired[bool]
    """Whether the URL fragment counts towards the unique key of the request."""

    use_extended_unique_key: NotRequired[bool]
    """Whether the method and payload count towards the unique key. Defaults to `True` for POST forms."""

    always_enqueue: NotRequired[bool]
    """Whether to enqueue the request even if it's already in the queue."""

    user_data: NotRequired[Mapping[str, JsonSerializable]]
    """Custom data stored with the request."""

    no_retry: NotRequired[bool]
    """Whether to skip retrying the request if it fails."""

    max_retries: NotRequired[int | None]
    """The maximum number of retries of the request."""


class _Field(NamedTuple):
    """A single entry the form submits."""

    name: str
    value: str
    is_file: bool = False


def get_declared_html_encoding(body: bytes, content_type: str | None) -> str | None:
    """Get the Python codec for the encoding an HTML response body declares.

    The encoding comes from the BOM, then the `charset` of the `Content-Type` header, then an XML declaration or the
    `<meta>` tags. Labels are resolved as in the WHATWG Encoding Standard, so legacy charsets map to the supersets
    browsers use and unknown labels are ignored.

    Args:
        body: The raw response body.
        content_type: The `Content-Type` header of the response.

    Returns:
        The codec, or `None` if the body declares no encoding browsers know.
    """
    for bom, encoding in _BOMS:
        if body.startswith(bom):
            return encoding

    header_charset = parse_content_type_charset(content_type)
    return resolve_encoding(header_charset) or _find_declared_encoding(body)


def decode_html_body(body: bytes, encoding: str) -> str:
    """Decode an HTML response body with the codec from `get_declared_html_encoding`, dropping a leading U+FEFF.

    Undecodable bytes are replaced with U+FFFD.

    Args:
        body: The raw response body.
        encoding: The Python codec to decode the body with.
    """
    return body.decode(encoding, 'replace').removeprefix('\ufeff')


def resolve_encoding(label: str | None) -> str | None:
    """Get the Python codec for a WHATWG encoding label, or `None` if browsers don't know the label."""
    if not label:
        return None
    return _ENCODING_BY_LABEL.get(label.lower())


def strip_html_comments(body: bytes) -> bytes:
    """Remove the HTML comments from the body, so a commented-out declaration doesn't count."""
    return _HTML_COMMENT_PATTERN.sub(b'', body)


def forms_to_requests(
    forms: Sequence[HtmlElement],
    page_url: str,
    page_encoding: str,
    *,
    fields: Mapping[str, str | Sequence[str] | None] | None = None,
    click: bool | Mapping[str, str] = True,
    all_forms: bool = False,
    **kwargs: Unpack[FormRequestOptions],
) -> list[Request]:
    """Create a `Request` submitting one of the forms, or each of them, the way a browser does.

    Args:
        forms: The form elements, each within the lxml tree of the whole page.
        page_url: The URL of the page, which forms without an action submit to and the `Referer` comes from.
        page_encoding: The Python codec the page was decoded with, which forms submit in by default.
        fields: Field values to submit, see `extract_form_requests` of the crawling contexts.
        click: The submit button to click, see `extract_form_requests`.
        all_forms: Whether to submit each form, filling in only the fields it has, see `extract_form_requests`.
        **kwargs: Additional options passed to `Request.from_url`.
    """
    if not forms:
        return []

    root = forms[0].getroottree().getroot()
    base = root.find('.//base[@href]')
    try:
        base_url = convert_to_absolute_url(page_url, '' if base is None else base.get('href'))
    except ValueError:
        base_url = page_url

    page_elements = list(root.iter(*_FIELD_TAGS))
    elements_by_form = _elements_by_form(root, page_elements)
    disabled = _disabled_elements(root, page_elements)

    if not all_forms:
        # The form sharing the most field names with `fields` is the one to fill in, and ties keep document order.
        # Forms sharing fewer names never get the values, even if the best one can't be submitted.
        wanted = set(fields or ())
        shared = {form: len(wanted & _field_names(elements_by_form.get(form, []))) for form in forms}
        most_shared = max(shared.values())
        forms = [form for form in forms if shared[form] == most_shared]

    requests = []
    for form in forms:
        elements = elements_by_form.get(form, [])
        form_fields = fields
        if all_forms and fields:
            # Values meant for one form don't spread to the others, like credentials into a search form.
            names = _field_names(elements)
            form_fields = {name: value for name, value in fields.items() if name in names}

        request = _form_to_request(
            form,
            elements,
            page_url=page_url,
            base_url=base_url,
            page_encoding=page_encoding,
            disabled=disabled,
            fields=form_fields,
            click=click,
            **kwargs,
        )
        if request is None:
            continue
        requests.append(request)
        if not all_forms:
            break
    return requests


def _find_declared_encoding(body: bytes) -> str | None:
    """Find the encoding declared by an XML declaration or a `<meta>` tag near the start of the body."""
    prescan = strip_html_comments(body[:_PRESCAN_BYTES])
    xml_match = _XML_ENCODING_PATTERN.match(prescan)
    xml_encoding = resolve_encoding(xml_match.group(1).decode('ascii')) if xml_match else None
    encoding = xml_encoding or _find_meta_encoding(prescan)
    # A declaration readable as ASCII rules out UTF-16, so browsers read such pages as UTF-8.
    return 'utf-8' if encoding and encoding.startswith('utf-16') else encoding


def _find_meta_encoding(prescan: bytes) -> str | None:
    """Find the encoding of the first `<meta>` tag that declares one browsers know.

    Both `<meta charset="...">` and `<meta http-equiv="Content-Type" content="...; charset=...">` count; a `charset=`
    in the `content` of any other `<meta>` tag doesn't.
    """
    for tag in _META_TAG_PATTERN.finditer(prescan):
        attributes: dict[str, str] = {}
        for match in _ATTRIBUTE_PATTERN.finditer(tag.group(1)):
            value = match.group(2) or match.group(3) or match.group(4) or b''
            attributes.setdefault(match.group(1).decode('latin-1').lower(), value.decode('latin-1'))

        if 'charset' in attributes:
            label = attributes['charset'].strip()
        elif attributes.get('http-equiv', '').strip().lower() == 'content-type':
            label = (parse_content_type_charset(attributes.get('content')) or '').strip('\'"')
        else:
            continue

        encoding = resolve_encoding(label)
        if encoding:
            return encoding
    return None


def _elements_by_form(root: HtmlElement, elements: list[HtmlElement]) -> dict[HtmlElement, list[HtmlElement]]:
    """Group the fields and buttons of the page by the form they belong to, in document order."""
    # Walking each form avoids an ancestor walk per field.
    enclosing_form: dict[HtmlElement, HtmlElement] = {}
    for form in root.iter('form'):
        enclosing_form.update(dict.fromkeys(form.iter(*_FIELD_TAGS), form))

    # The first element with a given ID wins, as in `getElementById`. Only the `form` attribute needs them.
    elements_by_id: dict[str, HtmlElement] = {}
    if any(element.get('form') is not None for element in elements):
        for element in root.xpath('//*[@id!=""]'):
            elements_by_id.setdefault(element.get('id'), element)

    elements_by_form: dict[HtmlElement, list[HtmlElement]] = {}
    for element in elements:
        form_id = element.get('form')
        owner = enclosing_form.get(element) if form_id is None else elements_by_id.get(form_id)
        if owner is not None and owner.tag == 'form':
            elements_by_form.setdefault(owner, []).append(element)
    return elements_by_form


def _disabled_elements(root: HtmlElement, elements: list[HtmlElement]) -> set[HtmlElement]:
    """Find the disabled fields and buttons, including those inside a disabled `<fieldset>`."""
    disabled = {element for element in elements if 'disabled' in element.attrib}
    for fieldset in root.iter('fieldset'):
        # A fieldset inside a disabled one is already covered, so each element is visited once.
        if 'disabled' in fieldset.attrib and fieldset not in disabled:
            disabled.update(fieldset.iter('fieldset', *_FIELD_TAGS))
    return disabled


def _field_names(elements: list[HtmlElement]) -> set[str]:
    """Get the names of the given fields and buttons."""
    return {name for element in elements if (name := element.get('name'))}


def _form_to_request(
    form: HtmlElement,
    elements: list[HtmlElement],
    *,
    page_url: str,
    base_url: str,
    page_encoding: str,
    disabled: set[HtmlElement],
    fields: Mapping[str, str | Sequence[str] | None] | None,
    click: bool | Mapping[str, str],
    **kwargs: Unpack[FormRequestOptions],
) -> Request | None:
    """Create a `Request` submitting a single form, or `None` if a browser wouldn't send one."""
    button = None
    if click is not False:
        button = _find_clickable(elements, {} if click is True else click, disabled)
        if click is not True and button is None:
            # Pages often enable a button with JavaScript, so a disabled one matching `click` is clicked too.
            button = _find_clickable(elements, click, set())
            if button is None:
                return None

    method = (_submission_attribute(form, button, 'method') or 'get').upper()
    enctype = _submission_attribute(form, button, 'enctype').lower()
    action = _submission_attribute(form, button, 'action').strip()

    # A dialog form only closes its `<dialog>` on the client.
    if method == 'DIALOG':
        return None

    url = _resolve_action(page_url, base_url, action)
    if url is None:
        return None

    entries = _collect_fields(elements, button, fields, disabled)
    encoding = _form_encoding(form, page_encoding)
    # CSRF checks may require the `Referer` or `Origin` a browser sends. Headers passed in `headers` win.
    form_headers = _referrer_headers(page_url, url, method)

    if method != 'POST':
        query = urlencode(
            [(entry.name, entry.value) for entry in entries], encoding=encoding, errors='xmlcharrefreplace'
        )
        kwargs['headers'] = HttpHeaders(form_headers) | HttpHeaders(kwargs.get('headers') or {})
        return Request.from_url(urlsplit(url)._replace(query=query).geturl(), method='GET', **kwargs)

    payload, form_headers['Content-Type'] = _encode_body(entries, enctype, encoding)
    kwargs['headers'] = HttpHeaders(form_headers) | HttpHeaders(kwargs.get('headers') or {})
    kwargs.setdefault('use_extended_unique_key', True)
    return Request.from_url(url, method='POST', payload=payload, **kwargs)


def _find_clickable(
    elements: list[HtmlElement], attributes: Mapping[str, str], disabled: set[HtmlElement]
) -> HtmlElement | None:
    """Find the first submit button having all the given attributes, skipping the `disabled` ones."""
    for element in elements:
        if not _is_submit_button(element) or element in disabled:
            continue
        if all(element.get(key) == value for key, value in attributes.items()):
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


def _submission_attribute(form: HtmlElement, button: HtmlElement | None, name: str) -> str:
    """Get a form attribute like `action`, which the clicked button can override with its `form*` counterpart."""
    if button is not None:
        button_value = button.get(f'form{name}')
        if button_value:
            return button_value
    return form.get(name) or ''


def _resolve_action(page_url: str, base_url: str, action: str) -> str | None:
    """Resolve the form action to an absolute URL, or `None` if it isn't a valid HTTP(S) URL."""
    if not action:
        return page_url

    try:
        url = convert_to_absolute_url(base_url, action)
        validate_http_url(url)
    except ValueError:
        return None
    # `validate_http_url` accepts a URL without a host, like `http:x` resolved against an HTTPS page.
    return url if is_url_absolute(url) else None


def _collect_fields(
    elements: list[HtmlElement],
    button: HtmlElement | None,
    fields: Mapping[str, str | Sequence[str] | None] | None,
    disabled: set[HtmlElement],
) -> list[_Field]:
    """Collect the entries the form submits: its fields, the clicked button and the `fields` overrides."""
    # Only one radio button of a group is checked, the last one in the markup.
    checked_radios = {element.get('name'): element for element in elements if _is_radio(element) and element.checked}

    entries: list[_Field] = []
    for element in elements:
        if element is button:
            entries.extend(_button_fields(button))
            continue
        if not element.get('name') or element in disabled:
            continue
        if _is_radio(element) and checked_radios.get(element.get('name')) is not element:
            continue
        entries.extend(_element_fields(element))

    return _apply_fields(entries, fields or {})


def _is_radio(element: HtmlElement) -> bool:
    """Check whether the element is a radio button."""
    return element.tag == 'input' and element.type == 'radio'


def _button_fields(button: HtmlElement) -> list[_Field]:
    """Get the entries the clicked button adds."""
    name = button.get('name')

    # An image button sends the click coordinates instead of its value.
    if button.get('type', '').lower() == 'image':
        prefix = f'{name}.' if name else ''
        return [_Field(f'{prefix}x', '0'), _Field(f'{prefix}y', '0')]

    if name:
        return [_Field(name, button.get('value', ''))]
    return []


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


def _apply_fields(entries: list[_Field], fields: Mapping[str, str | Sequence[str] | None]) -> list[_Field]:
    """Replace the entries named in `fields` with its values, dropping those set to `None`."""
    result = [entry for entry in entries if entry.name not in fields]
    for name, value in fields.items():
        if value is None:
            continue
        values = [value] if isinstance(value, str) else value
        result.extend(_Field(name, item) for item in values)
    return result


def _form_encoding(form: HtmlElement, page_encoding: str) -> str:
    """Pick the encoding a browser submits the form in: the first known `accept-charset` label, else the page one."""
    labels = (form.get('accept-charset') or '').replace(',', ' ').split()
    encoding = next(filter(None, map(resolve_encoding, labels)), page_encoding)
    # Browsers never submit in UTF-16, as it isn't ASCII-compatible.
    return 'utf-8' if encoding.startswith('utf-16') else encoding


def _referrer_headers(page_url: str, url: str, method: str) -> dict[str, str]:
    """Get the `Referer` and `Origin` a browser sends under its default `strict-origin-when-cross-origin` policy."""
    page = URL(page_url)
    target = URL(url)
    page_origin = str(page.origin())
    # Nothing about an HTTPS page is revealed to a plain HTTP URL.
    downgrade = page.scheme == 'https' and target.scheme != 'https'

    headers = {}
    if not downgrade:
        same_origin = page_origin == str(target.origin())
        referrer = f'{page_origin}{page.raw_path_qs}'
        headers['Referer'] = referrer if same_origin and len(referrer) <= _MAX_REFERRER_LENGTH else f'{page_origin}/'
    if method == 'POST':
        headers['Origin'] = 'null' if downgrade else page_origin
    return headers


def _encode_body(entries: list[_Field], enctype: str, encoding: str) -> tuple[bytes, str]:
    """Encode the fields as a POST body, returning it with its `Content-Type` header value."""
    if enctype == 'multipart/form-data':
        return _encode_multipart(entries, encoding)

    if enctype == 'text/plain':
        text = ''.join(f'{entry.name}={entry.value}\r\n' for entry in entries)
        return text.encode(encoding, 'xmlcharrefreplace'), 'text/plain'

    pairs = [(entry.name, entry.value) for entry in entries]
    body = urlencode(pairs, encoding=encoding, errors='xmlcharrefreplace').encode()
    return body, 'application/x-www-form-urlencoded'


def _encode_multipart(entries: list[_Field], encoding: str) -> tuple[bytes, str]:
    """Encode fields as `multipart/form-data`, returning the body and the `Content-Type` header value."""
    parts: list[bytes] = []
    for entry in entries:
        disposition = f'form-data; name="{entry.name}"'
        if entry.is_file:
            head = f'Content-Disposition: {disposition}; filename=""\r\nContent-Type: application/octet-stream\r\n\r\n'
        else:
            head = f'Content-Disposition: {disposition}\r\n\r\n'
        parts.append((head + entry.value).encode(encoding, 'xmlcharrefreplace'))

    # A random boundary would change the payload on every call and break deduplication.
    boundary = f'----CrawleeFormBoundary{compute_short_hash(b"".join(parts), length=16)}'
    delimiter = f'--{boundary}\r\n'.encode()
    body = b''.join(delimiter + part + b'\r\n' for part in parts) + f'--{boundary}--\r\n'.encode()
    return body, f'multipart/form-data; boundary={boundary}'
