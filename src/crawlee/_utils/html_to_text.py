# This file contains shared constants used by different implementations of html_to_text function.
from __future__ import annotations

import re

# Tags based on Javascript implementation of htmlToText from:
# https://github.com/apify/crawlee/blob/master/packages/utils/src/internals/cheerio.ts#L11
# Originally added here: https://github.com/apify/apify-ts/commit/4c0e5e3e7377536a449bb7b205132348ad3b0fe9
SKIP_TAGS = {'script', 'style', 'canvas', 'svg', 'noscript', 'title'}
BLOCK_TAGS = {
    'p',
    'h1',
    'h2',
    'h3',
    'h4',
    'h5',
    'h6',
    'ol',
    'ul',
    'li',
    'pre',
    'address',
    'blockquote',
    'dl',
    'div',
    'fieldset',
    'form',
    'table',
    'tr',
    'select',
    'option',
}

_EMPTY_OR_ENDS_WITH_ANY_WHITE_SPACE = re.compile(r'(^|\s)$')
_EMPTY_OR_ENDS_WITH_NEW_LINE = re.compile(r'(^|\n)$')
_ANY_CONSECUTIVE_WHITE_SPACES = re.compile(r'\s+')
