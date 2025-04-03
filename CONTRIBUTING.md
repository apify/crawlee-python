# Development

Here you'll find a contributing guide to get started with development.

## Environment

For local development, it is required to have Python 3.9 (or a later version) installed.

We use [uv](https://docs.astral.sh/uv/) for project management. Install it and set up your IDE accordingly.

## Dependencies

To install this package and its development dependencies, run:

```sh
make install-dev
```

## Code checking

To execute all code checking tools together, run:

```sh
make check-code
```

### Linting

We utilize [ruff](https://docs.astral.sh/ruff/) for linting, which analyzes code for potential issues and enforces consistent style. Refer to `pyproject.toml` for configuration details.

To run linting:

```sh
make lint
```

### Formatting

Our automated code formatting also leverages [ruff](https://docs.astral.sh/ruff/), ensuring uniform style and addressing fixable linting issues. Configuration specifics are outlined in `pyproject.toml`.

To run formatting:

```sh
make format
```

### Type checking

Type checking is handled by [mypy](https://mypy.readthedocs.io/), verifying code against type annotations. Configuration settings can be found in `pyproject.toml`.

To run type checking:

```sh
make type-check
```

### Unit tests

We employ pytest as our testing framework, equipped with various plugins. Check pyproject.toml for configuration details and installed plugins.

We use [pytest](https://docs.pytest.org/) as a testing framework with many plugins. Check `pyproject.toml` for configuration details and installed plugins.

To run unit tests:

```sh
make unit-tests
```

To run unit tests with HTML coverage report:

```sh
make unit-tests-cov
```

## End-to-end tests

Pre-requisites for running end-to-end tests:
 - [apify-cli](https://docs.apify.com/cli/docs/installation) correctly installed
 - `apify-cli` available in `PATH` environment variable
 - Your [apify token](https://docs.apify.com/platform/integrations/api#api-token) is available in `APIFY_TEST_USER_API_TOKEN` environment variable


To run end-to-end tests:

```sh
make e2e-templates-tests
```

## Documentation

We follow the [Google docstring format](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) for code documentation. All user-facing classes and functions must be documented. Documentation standards are enforced using [Ruff](https://docs.astral.sh/ruff/).

Our API documentation is generated from these docstrings using [pydoc-markdown](https://pypi.org/project/pydoc-markdown/) with custom post-processing. Additional content is provided through markdown files in the `docs/` directory. The final documentation is rendered using [Docusaurus](https://docusaurus.io/) and published to GitHub pages.

To run the documentation locally, ensure you have `Node.js` 20+ installed, then run:

```sh
make run-docs
```

## Release process

Publishing new versions to [PyPI](https://pypi.org/project/crawlee) is automated through GitHub Actions.

- **Beta releases**: On each commit to the master branch, a new beta release is automatically published. The version number is determined based on the latest release and conventional commits. The beta version suffix is incremented by 1 from the last beta release on PyPI.
- **Stable releases**: A stable version release may be created by triggering the `release` GitHub Actions workflow. The version number is determined based on the latest release and conventional commits (`auto` release type), or it may be overriden using the `custom` release type.

### Publishing to PyPI manually

1. **Do not do this unless absolutely necessary.** In all conceivable scenarios, you should use the `release` workflow instead.
2. **Make sure you know what you're doing.**

3. Update the version number:

- Modify the `version` field under `project` in `pyproject.toml`.

```toml
[project]
name = "crawlee"
version = "x.z.y"
```

4. Generate the distribution archives for the package:

```shell
uv build
```

5. Set up the PyPI API token for authentication and upload the package to PyPI:

```shell
uv publish --token YOUR_API_TOKEN
```
