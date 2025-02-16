# % if cookiecutter.enable_apify_integration
from apify import Actor
# % endif
# % block import required
# % endblock
# % if cookiecutter.http_client == 'curl-impersonate'
from crawlee.http_clients import CurlImpersonateHttpClient
# % elif cookiecutter.http_client == 'httpx'
from crawlee.http_clients import HttpxHttpClient
# % endif

from .routes import router

# % filter truncate(0, end='')
# % block http_client_instantiation
# % if cookiecutter.http_client == 'curl-impersonate'
http_client=CurlImpersonateHttpClient(),
# % elif cookiecutter.http_client == 'httpx'
http_client=HttpxHttpClient(),
# % endif
# % endblock
# % endfilter
# % if self.pre_main is defined

{{self.pre_main()}}

# % endif
async def main() -> None:
    """The crawler entry point."""
    # % filter truncate(0, end='')
    # % block instantiation required
    # % endblock
    # % endfilter

    # % if cookiecutter.enable_apify_integration
    async with Actor:
    # % set indent_width = 8
    # % else
    # % set indent_width = 4
    # % endif
# % filter indent(width=indent_width, first=True)
{{self.instantiation()}}

await crawler.run(
    [
        '{{ cookiecutter.start_url }}',
    ]
)
# % endfilter
