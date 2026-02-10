# Unit tests

Some tests may exhibit flaky behavior in CI. The reason for flaky behavior should be understood as it can indicate bug in the code or design flaw in the test. There are other reasons related to test execution, such as some tests that are not (or can not be) properly isolated, or limited resource constraints of the test executor.

Here are some suggested approaches to mitigate flakiness, sorted in the order of preference:
  - Investigate the root cause and fix the code or test.
  - Apply one of the pytest marks to mitigate the flakiness:
    - `@run_alone_on_mac` - Test with such mark will run alone on macOS exeutor in CI (normally several tests run in parallel, which can cause resource-sensitive tests to fail.) Use for resource sensitive tests that are known to be flaky only on macOS.
    - `@run_alone` - Test with such mark will run alone on any executor. Use for resource sensitive tests that are known to be flaky on all platforms or for tests that can not be run in parallel with other test due to their design (This should be extremely rare).
    - `@pytest.mark.flaky` - Test with such mark will be retried several times if it fails. Use for tests that are known to be flaky, but the reason for flakiness is not understood or can not be easily mitigated.
    - `@pytest.mark.skip` - Test with such mark will be skipped. Use when none of the above approaches mitigate the test flakiness. Marking test as skipped should be a last resort, as it can hide potential bugs and give false sense of security. Skipped tests should be tracked in GitHub issue.
