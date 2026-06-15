from lxml_html_clean import Cleaner


def get_basic_ai_cleaner(**kwargs: object) -> Cleaner:
    """Build the default `lxml_html_clean.Cleaner` used by built-in distillers.

    Args:
        kwargs: Overrides for individual `Cleaner` options.
    """
    options: dict = {
        'scripts': True,  # JSON scripts are protected separately by the distiller
        'javascript': True,
        'comments': True,
        'style': True,
        'inline_style': True,
        'links': True,
        'meta': False,  # meta tags carry structured data, handled by the distiller
        'page_structure': False,
        'embedded': True,
        'frames': True,
        'forms': False,
        'annoying_tags': True,
        'kill_tags': ('svg', 'noscript', 'template', 'canvas', 'video', 'audio', 'source'),
        'remove_unknown_tags': False,  # required: the JSON-script protect tag must survive
        'safe_attrs_only': False,  # the distiller filters attributes separately
    }
    options.update(kwargs)
    return Cleaner(**options)
