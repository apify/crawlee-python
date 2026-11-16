# % if cookiecutter.enable_apify_integration
from apify import Actor
# % endif
# % block import required
# % endblock
# % block http_client_import
# % if cookiecutter.http_client == 'curl-impersonate'
from crawlee.http_clients import CurlImpersonateHttpClient
# % elif cookiecutter.http_client == 'httpx'
from crawlee.http_clients import HttpxHttpClient
# % elif cookiecutter.http_client == 'impit'
from crawlee.http_clients import ImpitHttpClient
# % endif
# % endblock

from .routes import router

# % filter truncate(0, end='')
# % block http_client_instantiation
http_client=http_client,
# % endblock
# % endfilter
# % filter truncate(0, end='')
# % block context_manager
# % if cookiecutter.http_client == 'curl-impersonate'
CurlImpersonateHttpClient() as http_client
# % elif cookiecutter.http_client == 'httpx'
HttpxHttpClient() as http_client
# % elif cookiecutter.http_client == 'impit'
ImpitHttpClient() as http_client
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
    async with Actor, {{ self.context_manager() | trim }}:
    # % else
    async with {{ self.context_manager() | trim }}:
    # % endif
# % filter indent(width=8, first=True)
{{self.instantiation()}}
await crawler.run(
    [
        '{{ cookiecutter.start_url }}',
    ]
)
# % endfilter
