# Marker for truncated values in distillate documents and prompt instructions.
_TRUNCATION_MARKER = '[...]'

# Default prompt instructions for direct extraction from HTML.
_DIRECT_INSTRUCTIONS = (
    'You are a precise web data extraction engine. Extract the requested fields strictly from the provided '
    'document. Follow these rules:\n'
    '- A field value is content copied verbatim from the document - never a description, summary, or '
    'commentary about the document or its elements.\n'
    '- Add no formatting of your own: no bullets, no "Label: value" prefixes, no markdown.\n'
    '- If a field spans several elements (e.g. paragraphs of an article body), join their text in document '
    'order with newlines, adding nothing.\n'
    '- Use only what is present in the document; never invent or infer missing values. Leave absent fields '
    'empty (null for optional fields).\n'
    '- With several similar items on the page, return one entry per item; do not merge them.\n'
    '- Do not reformat, translate, or normalize values unless the field definition asks for it.\n'
    '- Return URLs exactly as they appear in `href`/`src`, without resolving or rewriting.'
)

# Instructions for the selector-generating prompt.
_SELECTOR_INSTRUCTIONS = (
    'You are an expert in CSS selectors. Given an HTML document, produce one Parsel CSS selector per '
    'requested field. Every leaf (data) selector MUST end with `::text` or `::attr(name)` so it yields a '
    'value rather than an element. For list fields the selector must match every item on the page. Prefer '
    'stable anchors - semantic tags, ids, meaningful classes, `itemprop` and `data-*` attributes - and never '
    'use positional selectors such as `:nth-child` or ids of individual list items: pages vary between '
    'requests. For a field that is a list of items, provide a container selector matching every item element '
    '(no ::text/::attr on it) and sub-selectors for the item fields written RELATIVE to one item container, '
    'each ending with `::text` or `::attr(name)`. If a field has no matching content on the page, omit its selector '
    'instead of inventing one that matches nothing.'
)

# Default prompt-notes for `PydanticAiCleanHtmlDistiller`.
_CLEAN_HTML_PROMPT_NOTES = (
    'The document is distilled HTML. Scripts and styling are removed; tags, nesting, and data-bearing '
    'attributes (`href`, `src`, `id`, `class`, `data-*`, `aria-*`, `lang`, `datetime`, `content`) are '
    'preserved. JSON payloads (`application/ld+json`, `application/json`) are kept and are a reliable source '
    f'for the requested fields. Values ending with `{_TRUNCATION_MARKER}` are truncated.'
)

# Default prompt-notes for `PydanticAiSkeletonDistiller`.
_SKELETON_PROMPT_NOTES = (
    'The document is a skeleton of an HTML page. Scripts and styling are removed; tags, nesting, and '
    'data-bearing attributes are preserved; JSON payloads are truncated to their key structure. Text is '
    'truncated to short samples, so rely on structure and attributes rather than on exact text content. Runs '
    'of repeated siblings are collapsed with an HTML comment marker; selectors must match every such sibling '
    f'on the full page. Values ending with `{_TRUNCATION_MARKER}` are truncated.'
)
