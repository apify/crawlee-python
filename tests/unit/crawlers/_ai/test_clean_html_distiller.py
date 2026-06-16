from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from crawlee.crawlers import AiCleanHtmlDistiller
from crawlee.crawlers._ai._prompts import _CLEAN_HTML_PROMPT_NOTES, _TRUNCATION_MARKER

if TYPE_CHECKING:
    import pytest


def test_empty_html_input() -> None:
    distiller = AiCleanHtmlDistiller()
    assert distiller.distill('') == ''
    assert distiller.distill('   \n\t ') == ''


def test_prompt_notes() -> None:
    assert AiCleanHtmlDistiller().get_prompt_notes() == _CLEAN_HTML_PROMPT_NOTES
    assert AiCleanHtmlDistiller(prompt_notes=None).get_prompt_notes() is None
    assert AiCleanHtmlDistiller(prompt_notes='custom').get_prompt_notes() == 'custom'


def test_keeps_text_and_semantic_attributes() -> None:
    html = '<div class="card" itemprop="product" data-id="7"><a href="/p">Item</a></div>'
    distilled_html = AiCleanHtmlDistiller().distill(html)

    assert html == distilled_html


def test_drops_non_semantic_attributes() -> None:
    distilled_html = AiCleanHtmlDistiller().distill(
        '<div style="color:red" onclick="hack()" tabindex="2"><p>Item</p></div>'
    )

    assert distilled_html == '<div><p>Item</p></div>'


def test_drops_scripts_and_styles() -> None:
    distilled_html = AiCleanHtmlDistiller().distill(
        '<body><script>evil()</script><style>.a{color:red}</style><p>Item</p></body>'
    )

    assert distilled_html == '<div><p>Item</p></div>'


def test_drops_noise_tags() -> None:
    distilled_html = AiCleanHtmlDistiller().distill('<div><svg><path/></svg><noscript>x</noscript><p>Item</p></div>')

    assert distilled_html == '<div><p>Item</p></div>'


def test_saves_json_ld_script() -> None:
    html = '<div><script type="application/ld+json">{"name": "Phone"}</script></div>'
    distilled_html = AiCleanHtmlDistiller().distill(html)

    assert distilled_html == html


def test_drops_data_uri_attribute() -> None:
    distilled_html = AiCleanHtmlDistiller().distill('<img src="data:image/png;base64,AAAABBBB" alt="logo">')

    assert distilled_html == '<img alt="logo">'


def test_limited_class_attribute() -> None:
    distilled_html = AiCleanHtmlDistiller(max_classes=2).distill('<div class="a b c d e">x</div>')

    assert distilled_html == '<div class="a b">x</div>'


def test_drops_empty_class_attribute() -> None:
    distilled_html = AiCleanHtmlDistiller().distill('<div class="   ">x</div>')

    assert distilled_html == '<div>x</div>'


def test_truncates_long_attribute_values() -> None:
    distilled_html = AiCleanHtmlDistiller(max_attr_len=5).distill(f'<a href="{"x" * 50}">link</a>')

    assert distilled_html == f'<a href="xxxxx{_TRUNCATION_MARKER}">link</a>'


def test_truncates_json_payload() -> None:
    distilled_html = AiCleanHtmlDistiller(max_json_len=5).distill(
        '<div><script type="application/json">{"long": "value here"}</script></div>'
    )

    assert distilled_html == f'<div><script type="application/json">{{"lon{_TRUNCATION_MARKER}</script></div>'


def test_keep_head_useful_tags() -> None:
    html = (
        '<html><head>'
        '<title>Page</title>'
        '<meta name="description" content="desc">'
        '<meta charset="utf-8">'
        '<link rel="stylesheet" href="/a.css">'
        '<script type="application/ld+json">{"k": 1}</script>'
        '</head><body><p>Body</p></body></html>'
    )
    distilled_html = AiCleanHtmlDistiller().distill(html)

    assert distilled_html == (
        '<html><head>'
        '<title>Page</title>'
        '<meta name="description" content="desc">'
        '<script type="application/ld+json">{"k": 1}</script>'
        '</head><body><p>Body</p></body></html>'
    )


def test_drops_head() -> None:
    distilled_html = AiCleanHtmlDistiller(keep_head=False).distill(
        '<html><head><title>Page</title></head><body><p>Body</p></body></html>'
    )

    assert distilled_html == '<html><body><p>Body</p></body></html>'


def test_normalizes_whitespace() -> None:
    distilled_html = AiCleanHtmlDistiller().distill('<p>a    b\n\n\tc</p>')

    assert distilled_html == '<p>a b c</p>'


def test_enforces_max_size(caplog: pytest.LogCaptureFixture) -> None:
    html = f'<div>{"<p>x</p>" * 500}</div>'
    with caplog.at_level(logging.WARNING, logger='crawlee.crawlers._ai._clean_html_distiller'):
        out = AiCleanHtmlDistiller(max_size=50).distill(html)

    assert len(out) == 50 + len(_TRUNCATION_MARKER)
    assert out.endswith(_TRUNCATION_MARKER)
    assert any('max_size' in record.message for record in caplog.records)
