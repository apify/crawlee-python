# Development

## Environment

For local development, it is required to have Python 3.9 (or a later version) installed.

It is recommended to set up a virtual environment while developing this package to isolate your development environment,
however, due to the many varied ways Python can be installed and virtual environments can be set up, this is left up to
the developers to do themselves.

One recommended way is with the built-in `venv` module:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

To improve on the experience, you can use [pyenv](https://github.com/pyenv/pyenv) to have an environment with a pinned
Python version, and [direnv](https://github.com/direnv/direnv) to automatically activate/deactivate the environment
when you enter/exit the project folder.

## Dependencies

To install this package and its development dependencies, run `make install-dev`.

## Code checking

To run all our code checking tools together, just run `make check-code`.

### Linting

We use [ruff](https://docs.astral.sh/ruff/) for linting to to analyze the code for potential issues and enforce
uniformed code style. See the `pyproject.toml` for its configuration. To run the linting, just run `make lint`.

### Formatting

We use [ruff](https://docs.astral.sh/ruff/) for automated code formatting. It formats the code to follow uniformed
code style and addresses auto-fixable linting issues. See the `pyproject.toml` for its configuration. To run
the formatting, just run `make format`.

### Type checking

We use [mypy](https://mypy.readthedocs.io/en/stable/) for type checking. See the `mypy.ini` for its configuration.
To run the type checking, just run `make type-check`.

### Unit tests

We use [pytest](https://docs.pytest.org/) as a testing framework with many plugins. See the `pyproject.toml` for
both its configuration and the list of installed plugins. To run unit tests execute `make unit-tests`. To run unit
tests with HTML coverage report execute `make unit-tests-cov`.

## Integration tests

todo

## Documentation

We use the [Google docstring format](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
for documenting the code. We document every user-facing class or method, and enforce that using the flake8-docstrings
library.

The documentation is then rendered from the docstrings in the code, using `pydoc-markdown` and some heavy
post-processing, and from Markdown documents in the `docs` folder in the `docs` branch, and then rendered using
Docusaurus and published to GitHub pages.

## Release process

Publishing new versions to [PyPI](https://pypi.org/project/apify) happens automatically through GitHub Actions.

On each commit to the `master` branch, a new beta release is published, taking the version number from `pyproject.toml`
and automatically incrementing the beta version suffix by 1 from the last beta release published to PyPI.

A stable version is published when a new release is created using GitHub Releases, again taking the version number
from `pyproject.toml`. The built package assets are automatically uploaded to the GitHub release.

If there is already a stable version with the same version number as in `pyproject.toml` published to PyPI, the publish
process fails, so don't forget to update the version number before releasing a new version. The release process also
fails when the released version is not described in `CHANGELOG.md`, so don't forget to describe the changes in
the new version there.

### Beta release checklist

Beta release happens automatically after you merge a pull request or add a direct commit to the master branch. Before
you do that check the following:

- Make sure that in the [pyproject.toml](https://github.com/apify/crawlee-py/blob/master/pyproject.toml)
a project version is set to the latest non-published version.
- Describe your changes to the [CHANGELOG.md](https://github.com/apify/crawlee-py/blob/master/CHANGELOG.md)
in the section with the latest non-published version.

### Production release checklist

Production release happens after the GitHub release is created. Before you do that check the following:

- Make sure that the beta release with the latest commit is successfully deployed.
- Make sure that all the changes that happened from the last production release are described in the
[CHANGELOG.md](https://github.com/apify/crawlee-py/blob/master/CHANGELOG.md).
- When drafting a new GitHub release:
    - Create a new tag in the format of `v1.2.3` targeting the master branch.
    - Fill in the release title in the format of `1.2.3`.
    - Copy the changes from the [CHANGELOG.md](https://github.com/apify/crawlee-py/blob/master/CHANGELOG.md)
    and paste them into the release description.
    - Check the "Set as the latest release" option.
