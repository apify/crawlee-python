# Development

Here you'll find a contributing guide to get started with development.

## Environment

For local development, you need a Python version that satisfies `requires-python` in `pyproject.toml`.

We use [uv](https://docs.astral.sh/uv/) for project management. Install it and set up your IDE accordingly.

We use [Poe the Poet](https://poethepoet.natn.io/) as a task runner, similar to npm scripts in `package.json`. All tasks are defined in `pyproject.toml` under `[tool.poe.tasks]`. Run `uv run poe` to list them with their descriptions, and `uv run poe <task>` to run one.

## Dependencies

To install this package with its development dependencies, the pre-commit hooks, and the Playwright browsers, run:

```sh
uv run poe install-dev
```

## Code checking

To run all code checks (lint, type check, and unit tests), run:

```sh
uv run poe check-code
```

We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting, [ty](https://docs.astral.sh/ty/) for type checking, and [pytest](https://docs.pytest.org/) for tests. All of them are configured in `pyproject.toml`, and each has its own task (`lint`, `format`, `type-check`, `unit-tests`).

## Tests

### Unit tests

Unit tests live in `tests/unit`. Run them with `uv run poe unit-tests`, or a single test with `uv run pytest tests/unit/<path>.py::<test_name>`. See [`tests/unit/README.md`](tests/unit/README.md) for how to handle flaky tests.

### End-to-end tests

Prerequisites:

- [apify-cli](https://docs.apify.com/cli/docs/installation) installed and available in `PATH`
- Set `APIFY_TEST_USER_API_TOKEN` to your [Apify API token](https://docs.apify.com/platform/integrations/api#api-token)

To run end-to-end tests:

```sh
uv run poe e2e-templates-tests
```

## Code style

### Comments and docstrings

- Comments and docstrings describe what the code does now. The bug a change fixed, what a PR changed, and contrasts with removed behavior (`instead of ...`, `no longer ...`) belong in the commit message.
- Don't explain language features in comments. A comment earns its place with a domain reason, an invariant the types can't express, or a non-obvious consequence for callers.
- Use single backticks for code references, never RST-style double backticks.
- Wrap docstrings and prose comments to the full line length configured in `pyproject.toml`.
- Write only ASCII in source files: `->` instead of an arrow, `-` instead of a dash, straight quotes.
- Write `RQ` for the request queue. Log messages and public `.mdx` docs spell out the full term.

### Tests

- Write flat `def test_*()` functions, never `class Test*`, and no section-header comments between tests.
- Almost every test gets a one-line docstring stating what it verifies, without a `Regression test:`-style prefix.
- In `@pytest.mark.parametrize`, wrap each case in `pytest.param(..., id='...')` with an id that describes the case.
- Use `Mock` and the `monkeypatch` fixture for test doubles, not hand-rolled stubs, counters, or flags.
- A test that changes process-wide or event-loop-wide state restores it. Prefer `monkeypatch`; for APIs it can't patch (`loop.set_exception_handler()`, `signal.signal()`, logging config, `os.chdir()`), capture the original value and restore it in a `finally`.
- On Windows with Python 3.12 or older, `datetime.now()` ticks in 1-15 ms steps, so two timestamps taken a few milliseconds apart can be equal. A test that asserts one timestamp is later than another injects a `Mock` clock instead of sleeping.
- For a TCP endpoint that must refuse connections, bind a socket to `('127.0.0.1', 0)` and hold it open without calling `listen()`. A hard-coded port can be taken by a server in a parallel xdist worker. Test servers use `127.0.0.1`, since on Windows `localhost` resolves to both IPv4 and IPv6.

## Documentation

We follow the [Google docstring format](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) for code documentation. All user-facing classes and functions must be documented. Documentation standards are enforced using [Ruff](https://docs.astral.sh/ruff/).

Our API documentation is generated from these docstrings using [pydoc-markdown](https://pypi.org/project/pydoc-markdown/) with custom post-processing. Additional content is provided through markdown files in the `docs/` directory. The final documentation is rendered using [Docusaurus](https://docusaurus.io/) and published to GitHub Pages.

Code examples in `docs/` have their own lint settings in `docs/pyproject.toml`. Never edit `website/versioned_docs/`: those snapshots are produced by the release automation and frozen against the code that shipped. Fix `docs/`, and the next release snapshot carries the change.

To run the documentation locally (requires Node.js 22.12 or newer and pnpm), run:

```sh
uv run poe run-docs
```

### Linting the docs and website

Markdown content (this guide, `README.md`, and the `docs/` folder) is checked with
[markdownlint](https://github.com/DavidAnson/markdownlint). The Docusaurus website code is linted
with [oxlint](https://oxc.rs/) and formatted with [oxfmt](https://oxc.rs/). All of them run in CI.
To run them locally (requires Node.js 22.12 or newer and pnpm), from the `website/` directory:

```sh
pnpm lint          # lint Markdown and website code
pnpm lint:fix      # auto-fix both
pnpm format        # format the website code
```

Doc images are committed as optimized `.webp`. To convert a new image, run
`pnpm opt:images <path-to-image>` from the `website/` directory.

## Commits

We use [Conventional Commits](https://www.conventionalcommits.org/) format for commit messages. The release process reads them to determine the version bump and build the changelog. Pick the type by what the commit changes, not by why:

| Type | Description |
| ---- | ----------- |
| `feat` | A new feature in the source code |
| `fix` | A bug fix in the source code |
| `perf` | A source code change that improves performance |
| `refactor` | A source code change that neither fixes a bug nor adds a feature |
| `style` | A source code change that doesn't affect its meaning (white-space, formatting) |
| `test` | Adding or changing tests |
| `docs` | Documentation changes |
| `ci` | CI workflow changes |
| `build` | Build system changes |
| `chore` | Dependency bumps, tooling, generated files, and other housekeeping |
| `revert` | Reverting a previous commit |

`feat`, `fix`, `perf`, `refactor`, and `style` trigger a release and appear in the changelog, so use them for source code changes only. `docs` triggers a docs release on master.

Logging changes are breaking: changing a log level, a logger name, or the wording of a log message, or removing one, breaks downstream log-based tooling. Keep the accurate type, add `!` after it, and list the changes in a `BREAKING CHANGE:` footer. PRs are squash-merged, so the `!` must also be in the PR title.

## Release process

Publishing new versions to [PyPI](https://pypi.org/project/crawlee) is automated through GitHub Actions.

- **Beta releases**: On each commit to the master branch, a new beta release is automatically published. The version number is determined based on the latest release and conventional commits. The beta version suffix is incremented by 1 from the last beta release on PyPI.
- **Stable releases**: A stable version release may be created by triggering the `release` GitHub Actions workflow. The version number is determined based on the latest release and conventional commits (`auto` release type), or it may be overridden using the `custom` release type.

### Publishing to PyPI manually

1. **Do not do this unless absolutely necessary.** In all conceivable scenarios, you should use the `release` workflow instead.
2. **Make sure you know what you're doing.**
3. Update the version number by modifying the `version` field under `project` in `pyproject.toml`:

    ```toml
    [project]
    name = "crawlee"
    version = "x.z.y"
    ```

4. Build the package:

    ```sh
    uv run poe build
    ```

5. Upload to PyPI:

    ```sh
    uv publish --token YOUR_API_TOKEN
    ```
