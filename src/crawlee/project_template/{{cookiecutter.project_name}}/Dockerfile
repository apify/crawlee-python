# First, specify the base Docker image.
# You can see the Docker images from Apify at https://hub.docker.com/r/apify/.
# You can also use any other image from Docker Hub.

{# The Playwright version baked into the Apify Playwright base images. Keep this in sync with the
   `playwright` version resolved by the lockfile so the installed package matches the browser
   binaries shipped in the base image. #}
# % set playwright_version = '1.61.0'

# % if cookiecutter.crawler_type == 'playwright' or cookiecutter.crawler_type.startswith('adaptive-') or cookiecutter.crawler_type == 'stagehand'
# % set base_image = 'apify/actor-python-playwright:3.13-' ~ playwright_version
# % elif cookiecutter.crawler_type == 'playwright-camoufox'
# % set base_image = 'apify/actor-python-playwright-camoufox:3.13-' ~ playwright_version
# % elif cookiecutter.crawler_type == 'playwright-chrome'
# % set base_image = 'apify/actor-python-playwright-chrome:3.13-' ~ playwright_version
# % elif cookiecutter.crawler_type == 'playwright-firefox'
# % set base_image = 'apify/actor-python-playwright-firefox:3.13-' ~ playwright_version
# % elif cookiecutter.crawler_type == 'playwright-webkit'
# % set base_image = 'apify/actor-python-playwright-webkit:3.13-' ~ playwright_version
# % else
# % set base_image = 'apify/actor-python:3.13'
# % endif
# % set uses_playwright_base_image = 'playwright' in base_image
FROM {{ base_image }}

RUN apt update && apt install -yq git && rm -rf /var/lib/apt/lists/*

# % if cookiecutter.package_manager == 'poetry'
RUN pip install -U pip setuptools \
    && pip install 'poetry<3' \
    && poetry self add 'poetry-plugin-export'

# Second, copy just poetry.lock and pyproject.toml into the Actor image,
# since those should be the only files that affects the dependency install in the next step,
# in order to speed up the build
COPY pyproject.toml poetry.lock ./

# Install the dependencies
RUN echo "Python version:" \
 && python --version \
 && echo "Installing dependencies:" \
 # Export packages from poetry.lock and install everything using pip
 # (ignore dependency checks - the lockfile is correct, period)
 && poetry export -f requirements.txt --without-hashes | pip install -r /dev/stdin --no-dependencies \
 && echo "All installed Python packages:" \
 && pip freeze
# % if uses_playwright_base_image
# Pin playwright to the version baked into the base image so it matches the browser binaries.
RUN pip install --no-deps --force-reinstall "playwright=={{ playwright_version }}"
# % endif
# % elif cookiecutter.package_manager == 'uv'
COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /uvx /bin/

ENV UV_PROJECT_ENVIRONMENT="/usr/local"

COPY pyproject.toml uv.lock ./

RUN echo "Python version:" \
    && python --version \
    && echo "Installing dependencies:" \
    && uv sync --frozen --no-install-project --no-editable --quiet --no-dev --inexact \
    && echo "All installed Python packages:" \
    && pip freeze
# % if uses_playwright_base_image
# Pin playwright to the version baked into the base image so it matches the browser binaries.
RUN uv pip install --system --reinstall --no-deps "playwright=={{ playwright_version }}"
# % endif
# % elif cookiecutter.package_manager == 'pip'
RUN pip install -U pip setuptools

# Second, copy just requirements.txt into the Actor image,
# since it should be the only file that affects the dependency install in the next step,
# in order to speed up the build
COPY requirements.txt ./

# Install the dependencies
RUN echo "Python version:" \
 && python --version \
 && echo "Installing dependencies:" \
 && pip install -r requirements.txt \
 && echo "All installed Python packages:" \
 && pip freeze
# % if uses_playwright_base_image
# Pin playwright to the version baked into the base image so it matches the browser binaries.
RUN pip install --no-deps --force-reinstall "playwright=={{ playwright_version }}"
# % endif
# % elif cookiecutter.package_manager == 'manual'
# TODO install dependencies
# % endif

# Next, copy the remaining files and directories with the source code.
# Since we do this after installing the dependencies, quick build will be really fast
# for most source file changes.
COPY . ./

# Use compileall to ensure the runnability of the Actor Python code.
RUN python -m compileall -q .

# % if cookiecutter.crawler_type == 'playwright-camoufox'
# Fetch camoufox files that are always needed when using camoufox.
RUN python -m camoufox fetch
# % endif

# Specify how to launch the source code of your Actor.
CMD ["python", "-m", "{{ cookiecutter.__package_name }}"]
