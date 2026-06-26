from lxml_html_clean import Cleaner


def get_basic_http_cleaner(**kwargs: object) -> Cleaner:
    """Build the default `lxml_html_clean.Cleaner` used by built-in distillers.

    Args:
        kwargs: Overrides for individual `Cleaner` options.
    """
    options: dict = {
        'annoying_tags': True,
        'comments': True,
        'embedded': True,
        'forms': False,
        'frames': True,
        'inline_style': True,
        'javascript': True,
        'kill_tags': ('audio', 'canvas', 'noscript', 'source', 'svg', 'template', 'video'),
        'links': True,
        'meta': False,  # meta tags carry structured data, handled by the distiller
        'page_structure': False,
        'remove_unknown_tags': False,  # required: the JSON-script protect tag must survive
        'safe_attrs_only': False,  # the distiller filters attributes separately
        'scripts': True,  # JSON scripts are protected separately by the distiller
        'style': True,
    }
    options.update(kwargs)
    return Cleaner(**options)
