.PHONY: clean install-dev build publish twine-check lint unit-tests integration-tests type-check check-code format check-version-availability check-changelog-entry

DIRS_WITH_CODE = src tests scripts

# This is default for local testing, but GitHub workflows override it to a higher value in CI
INTEGRATION_TESTS_CONCURRENCY = 1

clean:
	rm -rf build dist .mypy_cache .pytest_cache src/*.egg-info __pycache__

install-dev:
	python3 -m pip install --upgrade pip
	pip install --no-cache-dir -e ".[dev]"
	pre-commit install

build:
	python3 -m build

publish:
	python3 -m twine upload dist/*

twine-check:
	python3 -m twine check dist/*

lint:
	python3 -m ruff check $(DIRS_WITH_CODE)

unit-tests:
	python3 -m pytest -n auto -ra tests/unit --cov=src/crawlee

unit-tests-cov:
	python3 -m pytest -n auto -ra tests/unit --cov=src/crawlee --cov-report=html

integration-tests:
	python3 -m pytest -n $(INTEGRATION_TESTS_CONCURRENCY) -ra tests/integration

type-check:
	python3 -m mypy $(DIRS_WITH_CODE)

check-code: lint type-check unit-tests

format:
	python3 -m ruff check --fix $(DIRS_WITH_CODE)
	python3 -m ruff format $(DIRS_WITH_CODE)

check-version-availability:
	python3 scripts/check_version_availability.py

check-changelog-entry:
	python3 scripts/check_version_in_changelog.py
