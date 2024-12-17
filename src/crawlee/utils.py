from typing import Callable

from crawlee._utils.docs import docs_group


@docs_group('Functions')
def html_to_text(source: str) -> str:
    """Converts markup string to newline separated plain text without tags.

    Args:
        source: Input markup string
    Returns:
        Newline separated plain text without tags.
    """
    _html_to_text: Callable[[str], str]
    try:
        from crawlee.beautifulsoup_crawler._utils import html_to_text as _html_to_text
    except ImportError:
        try:
            from crawlee.parsel_crawler._utils import html_to_text as _html_to_text
        except ImportError as e:
            raise ImportError(
                'html_to_text requires either Parsel or BeautifulSoup package to be installed. Please '
                'install one of following: crawlee[beautifulsoup], crawlee[parsel] or crawlee[all].'
            ) from e
    return _html_to_text(source)
