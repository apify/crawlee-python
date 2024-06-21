# Changelog

## [0.0.5](../../releases/tag/v0.0.5) - Unreleased

### Adds

- Add explicit error messages for missing package extras during import
- Better browser abstraction:
    - `BrowserController` - Wraps a single browser instance and maintains its state.
    - `BrowserPlugin` - Manages the browser automation framework, and basically acts as a factory for controllers.
- Browser rotation with a maximum number of pages opened per browser.
- Add emit persist state event to event manager
- Add batched request addition in `RequestQueue`
- Add start requests option to `BasicCrawler`
- Add storage-related helpers `get_data`, `push_data` and `export_to` to `BasicCrawler` and `BasicContext`
- Add `PlaywrightCrawler`'s enqueue links helper

### Fixes

- Fix type error in persist state of statistics

## [0.0.4](../../releases/tag/v0.0.4) - 2024-05-30

- Another internal release, adding statistics capturing, proxy configuration and
the initial version of browser management and `PlaywrightCrawler`.

### Adds

- `Statistics`
- `ProxyConfiguration`
- `BrowserPool`
- `PlaywrightCrawler`

## [0.0.3](../../releases/tag/v0.0.3) - 2024-05-15

- Another internal release, adding mainly session management and `BeautifulSoupCrawler`.

### Adds

- `HttpxClient`
- `SessionPool`
- `BeautifulSoupCrawler`
- `BaseStorageClient`
- `Storages` and `MemoryStorageClient` were refactored

## [0.0.2](../../releases/tag/v0.0.2) - 2024-04-11

- The first internal release with `BasicCrawler` and `HttpCrawler`.

### Adds

- `EventManager` & `LocalEventManager`
- `Snapshotter`
- `AutoscaledPool`
- `MemoryStorageClient`
- `Storages`
- `BasicCrawler` & `HttpCrawler`

## [0.0.1](../../releases/tag/v0.0.1) - 2024-01-30

- Dummy package `crawlee` was released on PyPI.
