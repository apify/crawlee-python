from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from crawlee.crawlers import AiSkeletonDistiller
from crawlee.crawlers._ai._prompts import _SKELETON_PROMPT_NOTES, _TRUNCATION_MARKER

if TYPE_CHECKING:
    import pytest


def test_default_prompt_notes() -> None:
    assert AiSkeletonDistiller().get_prompt_notes() == _SKELETON_PROMPT_NOTES


def test_truncates_long_text() -> None:
    distilled_html = AiSkeletonDistiller(max_text_len=5).distill(f'<p>{"a" * 20}</p>')

    assert distilled_html == f'<p>aaaaa{_TRUNCATION_MARKER}</p>'


def test_keeps_short_text() -> None:
    distilled_html = AiSkeletonDistiller(max_text_len=50).distill('<p>short text</p>')

    assert distilled_html == '<p>short text</p>'


def test_collapses_repeated_siblings() -> None:
    items = ''.join(f'<li class="item">item {index}</li>' for index in range(10))
    distilled_html = AiSkeletonDistiller(keep_siblings=3).distill(f'<ul>{items}</ul>')

    assert distilled_html == (
        '<ul>'
        '<li class="item">item 0</li>'
        '<li class="item">item 1</li>'
        '<li class="item">item 2</li>'
        '<!-- ...7 more <li.item> siblings omitted -->'
        '</ul>'
    )


def test_does_not_collapse_siblings_with_different_identity_attrs() -> None:
    # Same tag and class, but different identity attributes, not a repeating template.
    spans = ''.join(f'<span name="field-{index}">v</span>' for index in range(4))
    distilled_html = AiSkeletonDistiller(keep_siblings=2).distill(f'<div>{spans}</div>')

    assert distilled_html == (
        '<div>'
        '<span name="field-0">v</span>'
        '<span name="field-1">v</span>'
        '<span name="field-2">v</span>'
        '<span name="field-3">v</span>'
        '</div>'
    )


def test_does_not_collapse_scripts() -> None:
    scripts = '<script type="application/json">{"a":1}</script>' * 4
    distilled_html = AiSkeletonDistiller(keep_siblings=2).distill(f'<div>{scripts}</div>')

    assert distilled_html == (
        '<div>'
        '<script type="application/json">{"a":1}</script>'
        '<script type="application/json">{"a":1}</script>'
        '<script type="application/json">{"a":1}</script>'
        '<script type="application/json">{"a":1}</script>'
        '</div>'
    )


def test_does_not_collapse_layout_markers() -> None:
    distilled_html = AiSkeletonDistiller(keep_siblings=2).distill(f'<div>{"<br>" * 5}</div>')

    assert distilled_html == '<div><br><br><br><br><br></div>'


def test_redistills_for_oversize_without_cutting(caplog: pytest.LogCaptureFixture) -> None:
    text = 'a' * 50
    html = f'<div><p name="a">{text}</p><p name="b">{text}</p><p name="c">{text}</p></div>'

    with caplog.at_level(logging.WARNING, logger='crawlee.crawlers._ai._skeleton_distiller'):
        distilled_html = AiSkeletonDistiller(max_text_len=20, max_size=120).distill(html)

    # The first distillation produces a skeleton of 134 chars, but the limit is 120 chars.
    # The second distillation uses more aggressive text truncation to 15 chars, so the result is 119 chars.
    assert distilled_html == (
        f'<div>'
        f'<p name="a">aaaaaaaaaaaaaaa{_TRUNCATION_MARKER}</p>'
        f'<p name="b">aaaaaaaaaaaaaaa{_TRUNCATION_MARKER}</p>'
        f'<p name="c">aaaaaaaaaaaaaaa{_TRUNCATION_MARKER}</p>'
        f'</div>'
    )
    assert not caplog.records


def test_cutting_for_oversize(caplog: pytest.LogCaptureFixture) -> None:
    text = 'a' * 50
    html = f'<div><p name="a">{text}</p><p name="b">{text}</p><p name="c">{text}</p></div>'

    with caplog.at_level(logging.WARNING, logger='crawlee.crawlers._ai._skeleton_distiller'):
        distilled_html = AiSkeletonDistiller(max_text_len=20, max_size=32).distill(html)

    assert distilled_html == f'<div><p name="a">aaaaaaaaaaaaaaa{_TRUNCATION_MARKER}'
    assert any('max_size' in record.message for record in caplog.records)
