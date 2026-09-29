from __future__ import annotations

import codecs

import pytest

from crawlee._utils.html import _ENCODING_BY_LABEL, _PRESCAN_BYTES, declared_html_encoding, decode_html_body

_CZECH = 'Test dekódování znaků českého jazyka'
_FRENCH = 'Test de décodage des caractères de la langue française dans une œuvre'


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
        pytest.param(b'', 'text/html; charset=utf-8', None, id='empty-body'),
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
        pytest.param(b'\xffok', 'text/html; charset=utf-8', '\ufffdok', id='invalid-bytes'),
    ],
)
def test_detect_and_decode(body: bytes, content_type: str | None, expected: str | None) -> None:
    """The body is decoded with the encoding the page declares, or not at all if it declares none."""
    encoding = declared_html_encoding(body, content_type)

    text = None if encoding is None else decode_html_body(body, encoding)

    assert text == expected


@pytest.mark.parametrize(
    ('body', 'content_type', 'expected'),
    [
        pytest.param(codecs.BOM_UTF16_LE + b'x', 'text/html; charset=utf-8', 'utf-16-le', id='bom'),
        pytest.param(b'<p>x</p>', 'text/html; charset=ISO-8859-1', 'windows-1252', id='header-superset'),
    ],
)
def test_declared_html_encoding(body: bytes, content_type: str, expected: str) -> None:
    """The Python codec of the encoding the page declares is detected."""
    assert declared_html_encoding(body, content_type) == expected


def test_encoding_labels_resolve_to_python_codecs() -> None:
    """Every encoding in the label table is a Python codec."""
    for codec in set(_ENCODING_BY_LABEL.values()):
        codecs.lookup(codec)
