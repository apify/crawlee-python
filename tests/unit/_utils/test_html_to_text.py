from __future__ import annotations

from typing import Callable

import pytest
from bs4 import BeautifulSoup
from parsel import Selector

from crawlee.crawlers._beautifulsoup._utils import html_to_text as html_to_text_beautifulsoup
from crawlee.crawlers._parsel._utils import html_to_text as html_to_text_parsel

_EXPECTED_TEXT = (
    "Let's start with a simple text. \n"
    "The ships hung in the sky, much the way that bricks don't. \n"
    "These aren't the Droids you're looking for\n"
    "I'm sorry, Dave. I'm afraid I can't do that.\n"
    "I'm sorry, Dave. I'm afraid I can't do that.\n"
    'A1\tA2\tA3\t\n'
    'B1\tB2\tB3\tB 4\t\n'
    'This is some text with inline elements and HTML entities (>bla<) \n'
    'Test\n'
    'a\n'
    'few\n'
    'line\n'
    'breaks\n'
    'Spaces in an inline text should be completely ignored. \n'
    'But,\n'
    '    a pre-formatted\n'
    '                block  should  be  kept\n'
    '                                       pre-formatted.\n'
    'The Greatest Science Fiction Quotes Of All Time \n'
    "Don't know, I don't know such stuff. I just do eyes, ju-, ju-, just eyes... just genetic design, just eyes. You "
    'Nexus, huh? I design your eyes.'
)

_EXAMPLE_HTML = """
<html>
<head>
    <title>Title SHOULD NOT be converted</title>

    <!-- Comments SHOULD NOT be converted -->
</head>
<body with='some attributes'>
Let's start with a        simple text.
<p>
    The ships hung in the sky, much the <a class="click" href="https://example.com/a/b/first">way that</a> bricks don't.
</p>
<ul>
    <li>These aren't the Droids you're looking for</li>
    <li some="attribute"><a href="https://example.com/a/second">I'm sorry, Dave. I'm afraid I can't do that.</a></li>
    <li><a class="click" href="https://example.com/a/b/third">I'm sorry, Dave. I'm afraid I can't do that.</a></li>
</ul>

<img src="something" alt="This should be ignored" />

<!-- Comments SHOULD NOT be converted -->

<table>
    <tr class="something">
        <td>A1</td>
        <td attributes="are ignored">A2</td>
        <td>A3</td>
    </tr>
    <tr class="something">
        <td>B1</td>
        <td attributes="are ignored" even="second attribute">B2</td>
        <td>B3</td>
        <td>B     4</td>
    </tr>
</table>

<p>
    This is <b>some<i> text <b>with</b></i></b> inline <span>elements</span> and HTML&nbsp;entities (&gt;bla&lt;)
</p>

<div>
    Test<br>
    a<br />
    few<br>
    line<br>
    breaks<br>
</div>




    Spaces


    in


    an inline text                                should be


    completely ignored.



<pre>
But,
    a pre-formatted
                block  should  be  kept
                                       pre-formatted.
</pre>

<svg>
    These special elements SHOULD NOT BE CONVERTED.
</svg>

<script>
    // These special elements should be completely skipped.
    skipThis();
</script>

<style>
    /* These special elements should be completely skipped. */
    .skip_this {}
</style>

<canvas>
    This should be skipped too.
</canvas>

<a class="click" href="https://another.com/a/fifth">The Greatest Science Fiction Quotes Of All Time</a>
<p>
    Don't know, I don't know such stuff. I just do eyes, ju-, ju-, just eyes... just genetic design,
    just eyes. You Nexus, huh? I design your <a class="click" href="http://cool.com/">eyes</a>.
</p>
</body>
</html>
"""


@pytest.mark.parametrize('html_to_text', [html_to_text_parsel, html_to_text_beautifulsoup])
@pytest.mark.parametrize(
    ('source', 'expected_text'),
    [
        pytest.param(_EXAMPLE_HTML, _EXPECTED_TEXT, id='Complex html'),
        ('   Plain    text     node    ', 'Plain text node'),
        ('   \nPlain    text     node  \n  ', 'Plain text node'),
        ('<h1>Header 1</h1> <h2>Header 2</h2>', 'Header 1\nHeader 2'),
        ('<h1>Header 1</h1> <h2>Header 2</h2><br>', 'Header 1\nHeader 2'),
        ('<h1>Header 1</h1> <h2>Header 2</h2><br><br>', 'Header 1\nHeader 2'),
        ('<h1>Header 1</h1> <h2>Header 2</h2><br><br><br>', 'Header 1\nHeader 2'),
        ('<h1>Header 1</h1><br><h2>Header 2</h2><br><br><br>', 'Header 1\n\nHeader 2'),
        ('<h1>Header 1</h1> <br> <h2>Header 2</h2><br><br><br>', 'Header 1\n\nHeader 2'),
        ('<h1>Header 1</h1>  \n <br>\n<h2>Header 2</h2><br><br><br>', 'Header 1\n\nHeader 2'),
        ('<h1>Header 1</h1>  \n <br>\n<br><h2>Header 2</h2><br><br><br>', 'Header 1\n\n\nHeader 2'),
        ('<h1>Header 1</h1>  \n <br>\n<br><br><h2>Header 2</h2><br><br><br>', 'Header 1\n\n\n\nHeader 2'),
        ('<div><div>Div</div><p>Paragraph</p></div>', 'Div\nParagraph'),
        ('<div>Div1</div><!-- Some comments --><div>Div2</div>', 'Div1\nDiv2'),
        ('<div>Div1</div><style>Skip styles</style>', 'Div1'),
        ('<script>Skip_scripts();</script><div>Div1</div>', 'Div1'),
        ('<SCRIPT>Skip_scripts();</SCRIPT><div>Div1</div>', 'Div1'),
        ('<svg>Skip svg</svg><div>Div1</div>', 'Div1'),
        ('<canvas>Skip canvas</canvas><div>Div1</div>', 'Div1'),
        ('<b>A  B  C  D  E\n\nF  G</b>', 'A B C D E F G'),
        ('<pre>A  B  C  D  E\n\nF  G</pre>', 'A  B  C  D  E\n\nF  G'),
        (
            '<h1>Heading 1</h1><div><div><div><div>Deep  Div</div></div></div></div><h2>Heading       2</h2>',
            'Heading 1\nDeep Div\nHeading 2',
        ),
        ('<a>this_word</a>_should_<b></b>be_<span>one</span>', 'this_word_should_be_one'),
        ('<span attributes="should" be="ignored">some <span>text</span></span>', 'some text'),
        pytest.param(
            (
                """<table>
    <tr>
        <td>Cell    A1</td><td>Cell A2</td>
        <td>    Cell A3    </td>
    </tr>
    <tr>
        <td>Cell    B1</td><td>Cell B2</td>
    </tr>
</table>"""
            ),
            'Cell A1\tCell A2\tCell A3 \t\nCell B1\tCell B2',
            id='Table',
        ),
        ('<span>&aacute; &eacute;</span>', 'á é'),
    ],
)
def test_html_to_text(source: str, expected_text: str, html_to_text: Callable[[str], str]) -> None:
    assert html_to_text(source) == expected_text


@pytest.mark.parametrize('html_to_text', [html_to_text_parsel, html_to_text_beautifulsoup])
def test_html_to_text_raises_on_wrong_input_type(html_to_text: Callable[[str], str]) -> None:
    with pytest.raises(TypeError):
        html_to_text(1)  # type: ignore[arg-type]  # Intentional wrong type test.


def test_html_to_text_parsel() -> None:
    assert html_to_text_parsel(Selector(_EXAMPLE_HTML)) == _EXPECTED_TEXT


def test_html_to_text_beautifulsoup() -> None:
    assert html_to_text_beautifulsoup(BeautifulSoup(_EXAMPLE_HTML)) == _EXPECTED_TEXT
