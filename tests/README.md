# Tests

## Structure

- `unit/` holds tests that run without a network or a browser. Run them with `uv run poe unit-tests`.
- `integration/` holds tests that send real HTTP requests, mostly to the local test server in `integration/server.py`, or launch a Playwright browser. Run them with `uv run poe integration-tests`.
- `e2e/` holds tests that build the project templates and run them as Actors on the Apify platform. Run them with `uv run poe e2e-templates-tests`.
- `_fixtures.py` has the fixtures that isolate each test's environment, imported by the unit and integration conftests.
- `_utils.py` has helpers shared by the unit and integration tests.

A test goes to `integration/` when it uses the `server_url` fixture (or another local server or proxy fixture), opens a real HTTP connection, or launches a browser. Everything else goes to `unit/`.

## Flaky tests

A test that fails only sometimes usually points to a bug in the code or a flaw in the test, so find the cause first. Some flakiness comes from how the tests run instead: tests that cannot be fully isolated, or a CI runner short on CPU or memory.

Handle a flaky test with the first of these that works:

1. Find the root cause and fix the code or the test.
2. Run the test alone. `@pytest.mark.run_alone` runs it before the parallel tests, with no other test alongside. Use it for resource-sensitive tests, or for the rare test that cannot run in parallel by design. `@run_alone_on_mac` from `tests/_utils.py` does the same on macOS only.
3. Retry it with `@pytest.mark.flaky(reruns=..., reason=...)`. Use this when the cause is not understood or cannot be removed, and say why in `reason`.
4. Skip it with `@pytest.mark.skip`, as a last resort. A skipped test can hide a real bug, so track it in a GitHub issue.
