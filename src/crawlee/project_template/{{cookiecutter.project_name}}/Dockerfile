# First, specify the base Docker image.
# You can see the Docker images from Apify at https://hub.docker.com/r/apify/.
# You can also use any other image from Docker Hub.
# % if cookiecutter.crawler_type == 'playwright' or cookiecutter.crawler_type.startswith('adaptive-') or cookiecutter.crawler_type == 'stagehand'
# % set base_image = 'apify/actor-python-playwright:3.13'
# % elif cookiecutter.crawler_type == 'playwright-camoufox'
# % set base_image = 'apify/actor-python-playwright-camoufox:3.13'
# % elif cookiecutter.crawler_type == 'playwright-chrome'
# % set base_image = 'apify/actor-python-playwright-chrome:3.13'
# % elif cookiecutter.crawler_type == 'playwright-firefox'
# % set base_image = 'apify/actor-python-playwright-firefox:3.13'
# % elif cookiecutter.crawler_type == 'playwright-webkit'
# % set base_image = 'apify/actor-python-playwright-webkit:3.13'
# % else
# % set base_image = 'apify/actor-python:3.13'
# % endif
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
 # Export packages from poetry.lock
 && poetry export -f requirements.txt --without-hashes | \
 # Replace playwright version so that it matches whatever is pre-installed in the image (the `hash` checks if playwright is installed)
    sed "s/^playwright==\(.*\)/playwright==$(hash playwright 2>/dev/null && (playwright --version | cut -d ' ' -f 2) || echo '\1')/" | \
 # Install everything using pip (ignore dependency checks - the lockfile is correct, period)
    pip install -r /dev/stdin --no-dependencies \
 && echo "All installed Python packages:" \
 && pip freeze
# % elif cookiecutter.package_manager == 'uv'
# Snapshot the base image's playwright version before COPY/uv sync so we can pin to it later
# and stay aligned with the browser binaries baked into /pw-browsers.
RUN pip install -U pip setuptools \
    && pip install 'uv<1' \
    && (hash playwright 2>/dev/null && playwright --version | cut -d ' ' -f 2 > /tmp/.base-playwright-version || true)

ENV UV_PROJECT_ENVIRONMENT="/usr/local"

COPY pyproject.toml uv.lock ./

RUN echo "Python version:" \
    && python --version \
    && echo "Installing dependencies:" \
    && uv sync --frozen --no-install-project --no-editable -q --no-dev --inexact \
    && PLAYWRIGHT_BASE_VERSION=$(cat /tmp/.base-playwright-version 2>/dev/null || echo "") \
    && if [ -n "$PLAYWRIGHT_BASE_VERSION" ]; then \
        echo "Pinning playwright to $PLAYWRIGHT_BASE_VERSION to match the browser binaries shipped in the base image" \
        && uv pip install --reinstall --no-deps "playwright==$PLAYWRIGHT_BASE_VERSION"; \
    fi \
    && rm -f /tmp/.base-playwright-version \
    && echo "All installed Python packages:" \
    && pip freeze
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
 # Install everything using pip, set playwright version so that it matches whatever is pre-installed in the image
 && cat requirements.txt | \
 # Replace playwright version so that it matches whatever is pre-installed in the image (the `hash` checks if playwright is installed)
    sed "s/^playwright==\(.*\)/playwright==$(hash playwright 2>/dev/null && (playwright --version | cut -d ' ' -f 2) || echo '\1')/" | \
 # Install everything using pip
    pip install -r /dev/stdin \
 && echo "All installed Python packages:" \
 && pip freeze
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
