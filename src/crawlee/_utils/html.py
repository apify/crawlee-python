"""HTML utility functions for Crawlee."""

from __future__ import annotations

import codecs
import re

from crawlee._utils.http import parse_content_type_charset

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


def declared_html_encoding(body: bytes, content_type: str | None) -> str | None:
    """Get the Python codec for the encoding an HTML response body declares.

    The encoding comes from the BOM, then the `charset` of the `Content-Type` header, then an XML declaration or the
    `<meta>` tags. Labels are resolved as in the WHATWG Encoding Standard, so legacy charsets map to the supersets
    browsers use and unknown labels are ignored.

    Args:
        body: The raw response body.
        content_type: The `Content-Type` header of the response.

    Returns:
        The codec, or `None` if the body is empty or declares no encoding browsers know.
    """
    if not body:
        return None

    for bom, encoding in _BOMS:
        if body.startswith(bom):
            return encoding

    header_charset = parse_content_type_charset(content_type)
    return _resolve_encoding(header_charset) or _find_declared_encoding(body)


def decode_html_body(body: bytes, encoding: str) -> str:
    """Decode an HTML response body with the codec from `declared_html_encoding`, dropping a leading U+FEFF.

    Undecodable bytes are replaced with U+FFFD.

    Args:
        body: The raw response body.
        encoding: The Python codec to decode the body with.
    """
    return body.decode(encoding, 'replace').removeprefix('\ufeff')


def _resolve_encoding(label: str | None) -> str | None:
    """Get the Python codec for a WHATWG encoding label, or `None` if browsers don't know the label."""
    if not label:
        return None
    return _ENCODING_BY_LABEL.get(label.lower())


def _find_declared_encoding(body: bytes) -> str | None:
    """Find the encoding declared by an XML declaration or a `<meta>` tag near the start of the body."""
    prescan = _HTML_COMMENT_PATTERN.sub(b'', body[:_PRESCAN_BYTES])
    xml_match = _XML_ENCODING_PATTERN.match(prescan)
    encoding = _resolve_encoding(xml_match.group(1).decode('ascii')) if xml_match else _find_meta_encoding(prescan)
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

        encoding = _resolve_encoding(label)
        if encoding:
            return encoding
    return None
