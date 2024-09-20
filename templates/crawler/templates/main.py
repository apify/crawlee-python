# % if cookiecutter.enable_apify_integration
from apify import Actor
# % endif
# % block import required
# % endblock

from .routes import router


async def main() -> None:
    """The crawler entry point."""
    # % filter truncate(0, end='')
    # % block instantiation required
    # % endblock
    # % endfilter

    # % if cookiecutter.enable_apify_integration
    async with Actor:
        # % filter indent(width=8, first=False)
        {{ self.instantiation() }}
        # % endfilter
    # % else
        # % filter indent(width=4, first=False)
    {{ self.instantiation() }}
        # % endfilter
    # % endif

    await crawler.run(
        [
            '{{ cookiecutter.start_url }}',
        ]
    )
