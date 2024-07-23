# Changelog

## 0.1.2 (Unreleased)

### Features

- Add validation of URLs.

### Bug fixes

- Strip whitespace from href in `enqueue_links`.
- Use `error_handler` for context pipeline errors.

## [0.1.1](https://github.com/apify/crawlee-python/releases/tag/v0.1.1) (2024-07-19)

### Features

- Support for proxy configuration in `PlaywrightCrawler`.
- Blocking detection in `PlaywrightCrawler`.
- Expose `crawler.log` to public.

### Bug fixes

- Fix Pylance `reportPrivateImportUsage` errors by defining `__all__` in modules `__init__.py`.
- Set `HTTPX` logging level to `WARNING` by default.
- Fix CLI behavior with existing project folders.

## [0.1.0](https://github.com/apify/crawlee-python/releases/tag/v0.1.0) (2024-07-09)

### Features

- new project bootstrapping via `pipx run crawlee create`

### Bug fixes

- improve error handling in project bootstrapping

## [0.0.7](https://github.com/apify/crawlee-python/releases/tag/v0.0.7) (2024-06-27)

### Bug fixes

- selector handling for `RETRY_CSS_SELECTORS` in `_handle_blocked_request` in `BeautifulSoupCrawler`
- selector handling in `enqueue_links` in `BeautifulSoupCrawler`
- improve `AutoscaledPool` state management

## [0.0.6](https://github.com/apify/crawlee-python/releases/tag/v0.0.6) (2024-06-25)

### Features

- BREAKING: `BasicCrawler.export_data` helper method which replaces `BasicCrawler.export_to`
- `Configuration.get_global_configuration` method
- Automatic logging setup
- Context helper for logging (`context.log`)

### Bug fixes

- Handling of relative URLs in `add_requests`
- Graceful exit in `BasicCrawler.run`

## [0.0.5](https://github.com/apify/crawlee-python/releases/tag/v0.0.5) (2024-06-21)

### Features

- Add explicit error messages for missing package extras during import
- Better browser abstraction:
    - `BrowserController` - Wraps a single browser instance and maintains its state.
    - `BrowserPlugin` - Manages the browser automation framework, and basically acts as a factory for controllers.
- Browser rotation with a maximum number of pages opened per browser.
- Add emit persist state event to event manager
- Add batched request addition in `RequestQueue`
- Add start requests option to `BasicCrawler`
- Add storage-related helpers `get_data`, `push_data` and `export_to` to `BasicCrawler` and `BasicContext`
- Add enqueue links helper to `PlaywrightCrawler`
- Add max requests per crawl option to `BasicCrawler`

### Bug fixes

- Fix type error in persist state of statistics

## [0.0.4](https://github.com/apify/crawlee-python/releases/tag/v0.0.4) (2024-05-30)

- Another internal release, adding statistics capturing, proxy configuration and
the initial version of browser management and `PlaywrightCrawler`.

### Features

- `Statistics`
- `ProxyConfiguration`
- `BrowserPool`
- `PlaywrightCrawler`

## [0.0.3](https://github.com/apify/crawlee-python/releases/tag/v0.0.3) (2024-05-15)

- Another internal release, adding mainly session management and `BeautifulSoupCrawler`.

### Features

- `HttpxClient`
- `SessionPool`
- `BeautifulSoupCrawler`
- `BaseStorageClient`
- `Storages` and `MemoryStorageClient` were refactored

## [0.0.2](https://github.com/apify/crawlee-python/releases/tag/v0.0.2) (2024-04-11)

- The first internal release with `BasicCrawler` and `HttpCrawler`.

### Features

- `EventManager` & `LocalEventManager`
- `Snapshotter`
- `AutoscaledPool`
- `MemoryStorageClient`
- `Storages`
- `BasicCrawler` & `HttpCrawler`

## [0.0.1](https://github.com/apify/crawlee-python/releases/tag/v0.0.1) (2024-01-30)

- Dummy package `crawlee` was released on PyPI.
