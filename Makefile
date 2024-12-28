.PHONY: clean install-dev build publish-to-pypi lint type-check unit-tests unit-tests-cov integration-tests format check-code build-api-reference run-docs

DIRS_WITH_CODE = src tests docs

# This is default for local testing, but GitHub workflows override it to a higher value in CI
INTEGRATION_TESTS_CONCURRENCY = 1

clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache build dist htmlcov .coverage

install-dev:
	uv sync --group dev --all-extras
	uv run pre-commit install
	uv run playwright install

build:
	uv build -v

# APIFY_PYPI_TOKEN_CRAWLEE is expected to be set in the environment
publish-to-pypi:
	uv publish -v --token "${APIFY_PYPI_TOKEN_CRAWLEE}"

lint:
	uv run ruff format --check $(DIRS_WITH_CODE)
	uv run ruff check $(DIRS_WITH_CODE)

type-check:
	uv run mypy $(DIRS_WITH_CODE)

unit-tests:
	uv run pytest --numprocesses=auto --verbose --cov=src/crawlee tests/unit

unit-tests-cov:
	uv run pytest --numprocesses=auto --verbose --cov=src/crawlee --cov-report=html tests/unit

integration-tests:
	uv run pytest --numprocesses=$(INTEGRATION_TESTS_CONCURRENCY) tests/integration

format:
	uv run ruff check --fix $(DIRS_WITH_CODE)
	uv run ruff format $(DIRS_WITH_CODE)

# The check-code target runs a series of checks equivalent to those performed by pre-commit hooks
# and the run_checks.yaml GitHub Actions workflow.
check-code: lint type-check unit-tests

build-api-reference:
	cd website && poetry run ./build_api_reference.sh

build-docs:
	cd website && corepack enable && yarn && poetry run yarn build

run-docs: build-api-reference
	cd website && corepack enable && yarn && poetry run yarn start
