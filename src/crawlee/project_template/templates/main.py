# % if cookiecutter.enable_apify_integration
from apify import Actor
# % endif
# % block import required
# % endblock
# % if cookiecutter.http_client == 'curl-impersonate'
from crawlee.http_clients.curl_impersonate import CurlImpersonateHttpClient
# % elif cookiecutter.http_client == 'httpx'
from crawlee.http_clients._httpx import HttpxHttpClient
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
