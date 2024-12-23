# Changelog

All notable changes to this project will be documented in this file.

<!-- git-cliff-unreleased-start -->
## 0.5.0 - **not yet released**

### 🚀 Features

- Add possibility to use None as no proxy in tiered proxies ([#760](https://github.com/apify/crawlee-python/pull/760)) ([0fbd017](https://github.com/apify/crawlee-python/commit/0fbd01723b9fe2e3410e0f358cab2f22848b08d0)) by [@Pijukatel](https://github.com/Pijukatel), closes [#687](https://github.com/apify/crawlee-python/issues/687)
- Add `use_state` context method ([#682](https://github.com/apify/crawlee-python/pull/682)) ([868b41e](https://github.com/apify/crawlee-python/commit/868b41ebd4c8003fa60ab07887577d0fb85b6ecc)) by [@Mantisus](https://github.com/Mantisus), closes [#191](https://github.com/apify/crawlee-python/issues/191)
- Add pre-navigation hooks router to AbstractHttpCrawler ([#791](https://github.com/apify/crawlee-python/pull/791)) ([0f23205](https://github.com/apify/crawlee-python/commit/0f23205923065074c522b3de9d47218a204dfa78)) by [@Pijukatel](https://github.com/Pijukatel), closes [#635](https://github.com/apify/crawlee-python/issues/635)
- Add example of how to integrate Camoufox into PlaywrightCrawler ([#789](https://github.com/apify/crawlee-python/pull/789)) ([246cfc4](https://github.com/apify/crawlee-python/commit/246cfc4ebc8bce1d15e1dddd62d652bd65869328)) by [@Pijukatel](https://github.com/Pijukatel), closes [#684](https://github.com/apify/crawlee-python/issues/684)
- Expose event types, improve on&#x2F;emit signature, allow parameterless listeners ([#800](https://github.com/apify/crawlee-python/pull/800)) ([c102c4c](https://github.com/apify/crawlee-python/commit/c102c4c894a00b09adfd5f4911563c81cf3e98b4)) by [@janbuchar](https://github.com/janbuchar), closes [#561](https://github.com/apify/crawlee-python/issues/561)
- Add stop method to BasicCrawler ([#807](https://github.com/apify/crawlee-python/pull/807)) ([6d01af4](https://github.com/apify/crawlee-python/commit/6d01af4231d02b4349a8719f5ed18d812843fde5)) by [@Pijukatel](https://github.com/Pijukatel), closes [#651](https://github.com/apify/crawlee-python/issues/651)
- Add `html_to_text` helper function ([#792](https://github.com/apify/crawlee-python/pull/792)) ([2b9d970](https://github.com/apify/crawlee-python/commit/2b9d97009dd653870681bb3cadbb46b214ff1a73)) by [@Pijukatel](https://github.com/Pijukatel), closes [#659](https://github.com/apify/crawlee-python/issues/659)
- [**breaking**] Implement `RequestManagerTandem`, remove `add_request` from `RequestList`, accept any iterable in `RequestList` constructor ([#777](https://github.com/apify/crawlee-python/pull/777)) ([4172652](https://github.com/apify/crawlee-python/commit/4172652079e5e91190c1cc5e2138fd41a7c84a6b)) by [@janbuchar](https://github.com/janbuchar)

### 🐛 Bug Fixes

- Fix circular import in `KeyValueStore` ([#805](https://github.com/apify/crawlee-python/pull/805)) ([8bdf49d](https://github.com/apify/crawlee-python/commit/8bdf49d1cb2a94b66f69fd1b77063a4113517fae)) by [@Mantisus](https://github.com/Mantisus), closes [#804](https://github.com/apify/crawlee-python/issues/804)
- [**breaking**] Refactor service usage to rely on `service_locator` ([#691](https://github.com/apify/crawlee-python/pull/691)) ([1d31c6c](https://github.com/apify/crawlee-python/commit/1d31c6c7e7a9ec7cee5b2de900568d9f77db65ba)) by [@vdusek](https://github.com/vdusek), closes [#369](https://github.com/apify/crawlee-python/issues/369), [#539](https://github.com/apify/crawlee-python/issues/539), [#699](https://github.com/apify/crawlee-python/issues/699)
- Pass `verify` in httpx client ([#802](https://github.com/apify/crawlee-python/pull/802)) ([074d083](https://github.com/apify/crawlee-python/commit/074d0836b55e52f13726e7cd1c21602623fda4fc)) by [@Mantisus](https://github.com/Mantisus), closes [#798](https://github.com/apify/crawlee-python/issues/798)
- Fix `page_options` for `PlaywrightBrowserPlugin` ([#796](https://github.com/apify/crawlee-python/pull/796)) ([bd3bdd4](https://github.com/apify/crawlee-python/commit/bd3bdd4046c2ddea62feb77322033cad50f382dd)) by [@Mantisus](https://github.com/Mantisus), closes [#755](https://github.com/apify/crawlee-python/issues/755)
- Fix event migrating handler in `RequestQueue` ([#825](https://github.com/apify/crawlee-python/pull/825)) ([fd6663f](https://github.com/apify/crawlee-python/commit/fd6663f903bc7eecd1000da89e06197b43dfb962)) by [@Mantisus](https://github.com/Mantisus), closes [#815](https://github.com/apify/crawlee-python/issues/815)
- Respect user configuration for work with status codes ([#812](https://github.com/apify/crawlee-python/pull/812)) ([8daf4bd](https://github.com/apify/crawlee-python/commit/8daf4bd49c1b09a0924f827daedebf7600ac609b)) by [@Mantisus](https://github.com/Mantisus), closes [#708](https://github.com/apify/crawlee-python/issues/708), [#756](https://github.com/apify/crawlee-python/issues/756)
- `abort-on-error` for successive runs ([#834](https://github.com/apify/crawlee-python/pull/834)) ([0cea673](https://github.com/apify/crawlee-python/commit/0cea67387bf366800b447de784af580159b199ee)) by [@Mantisus](https://github.com/Mantisus)
- Relax ServiceLocator restrictions ([#837](https://github.com/apify/crawlee-python/pull/837)) ([aa3667f](https://github.com/apify/crawlee-python/commit/aa3667f344d78945df3eca77431e1409f43f8bb5)) by [@janbuchar](https://github.com/janbuchar), closes [#806](https://github.com/apify/crawlee-python/issues/806)
- Fix typo in exports ([#841](https://github.com/apify/crawlee-python/pull/841)) ([8fa6ac9](https://github.com/apify/crawlee-python/commit/8fa6ac994fe4f3f6430cb796a0c6a732c93c672b)) by [@janbuchar](https://github.com/janbuchar)

### Refactor

- [**breaking**] Refactor HttpCrawler, BeautifulSoupCrawler, ParselCrawler inheritance ([#746](https://github.com/apify/crawlee-python/pull/746)) ([9d3c269](https://github.com/apify/crawlee-python/commit/9d3c2697c91ce93028ca86a91d85d465d36c1ad7)) by [@Pijukatel](https://github.com/Pijukatel), closes [#350](https://github.com/apify/crawlee-python/issues/350)
- [**breaking**] Remove `json_` and `order_no` from `Request` ([#788](https://github.com/apify/crawlee-python/pull/788)) ([5381d13](https://github.com/apify/crawlee-python/commit/5381d13aa51a757fc1906f400788555df090a1af)) by [@Mantisus](https://github.com/Mantisus), closes [#94](https://github.com/apify/crawlee-python/issues/94)
- [**breaking**] Rename PwPreNavContext to PwPreNavCrawlingContext ([#827](https://github.com/apify/crawlee-python/pull/827)) ([84b61a3](https://github.com/apify/crawlee-python/commit/84b61a3d25bee42faed4e81cd156663f251b3d3d)) by [@vdusek](https://github.com/vdusek)
- [**breaking**] Rename PlaywrightCrawler kwargs: browser_options, page_options ([#831](https://github.com/apify/crawlee-python/pull/831)) ([ffc6048](https://github.com/apify/crawlee-python/commit/ffc6048e9dc5c5e862271fa50c48bb0fb6f0a18f)) by [@Pijukatel](https://github.com/Pijukatel)
- [**breaking**] Update the crawlers &amp; storage clients structure ([#828](https://github.com/apify/crawlee-python/pull/828)) ([0ba04d1](https://github.com/apify/crawlee-python/commit/0ba04d1633881043928a408678932c46fb90e21f)) by [@vdusek](https://github.com/vdusek), closes [#764](https://github.com/apify/crawlee-python/issues/764)


<!-- git-cliff-unreleased-end -->
## [0.4.5](https://github.com/apify/crawlee-python/releases/tag/v0.4.5) (2024-12-06)

### 🚀 Features

- Improve project bootstrapping ([#538](https://github.com/apify/crawlee-python/pull/538)) ([367899c](https://github.com/apify/crawlee-python/commit/367899cbad5021674f6e41c4dd7eb2266fe043aa)) by [@janbuchar](https://github.com/janbuchar), closes [#317](https://github.com/apify/crawlee-python/issues/317), [#414](https://github.com/apify/crawlee-python/issues/414), [#495](https://github.com/apify/crawlee-python/issues/495), [#511](https://github.com/apify/crawlee-python/issues/511)

### 🐛 Bug Fixes

- Add upper bound of HTTPX version ([#775](https://github.com/apify/crawlee-python/pull/775)) ([b59e34d](https://github.com/apify/crawlee-python/commit/b59e34d6301e26825d88608152ffb337ef602a9f)) by [@vdusek](https://github.com/vdusek)
- Fix incorrect use of desired concurrency ratio ([#780](https://github.com/apify/crawlee-python/pull/780)) ([d1f8bfb](https://github.com/apify/crawlee-python/commit/d1f8bfb68ce2ef13b550ce415a3689858112a4c7)) by [@Pijukatel](https://github.com/Pijukatel), closes [#759](https://github.com/apify/crawlee-python/issues/759)
- Remove pydantic constraint &lt;2.10.0 and update timedelta validator, serializer type hints ([#757](https://github.com/apify/crawlee-python/pull/757)) ([c0050c0](https://github.com/apify/crawlee-python/commit/c0050c0ee76e5deb28f174ecf276b0e6abf68b9d)) by [@Pijukatel](https://github.com/Pijukatel)


## [0.4.4](https://github.com/apify/crawlee-python/releases/tag/v0.4.4) (2024-11-29)

### 🚀 Features

- Expose browser_options and page_options to PlaywrightCrawler ([#730](https://github.com/apify/crawlee-python/pull/730)) ([dbe85b9](https://github.com/apify/crawlee-python/commit/dbe85b90e59def281cfc6617a0eb869a4adf2fc0)) by [@vdusek](https://github.com/vdusek), closes [#719](https://github.com/apify/crawlee-python/issues/719)
- Add `abort_on_error` property ([#731](https://github.com/apify/crawlee-python/pull/731)) ([6dae03a](https://github.com/apify/crawlee-python/commit/6dae03a68a2d23c68c78d8d44611d43e40eb9404)) by [@Mantisus](https://github.com/Mantisus), closes [#704](https://github.com/apify/crawlee-python/issues/704)

### 🐛 Bug Fixes

- Fix init of context managers and context handling in `BasicCrawler` ([#714](https://github.com/apify/crawlee-python/pull/714)) ([486fe6d](https://github.com/apify/crawlee-python/commit/486fe6d6cd56cb560ab51a32ec0286d9e32267cb)) by [@vdusek](https://github.com/vdusek)


## [0.4.3](https://github.com/apify/crawlee-python/releases/tag/v0.4.3) (2024-11-21)

### 🐛 Bug Fixes

- Pydantic 2.10.0 issues ([#716](https://github.com/apify/crawlee-python/pull/716)) ([8d8b3fc](https://github.com/apify/crawlee-python/commit/8d8b3fcff8be10edf5351f5324c7ba112c1d2ba0)) by [@Pijukatel](https://github.com/Pijukatel)


## [0.4.2](https://github.com/apify/crawlee-python/releases/tag/v0.4.2) (2024-11-20)

### 🐛 Bug Fixes

- Respect custom HTTP headers in `PlaywrightCrawler` ([#685](https://github.com/apify/crawlee-python/pull/685)) ([a84125f](https://github.com/apify/crawlee-python/commit/a84125f031347426de44b8f015c87882c8f96f72)) by [@Mantisus](https://github.com/Mantisus)
- Fix serialization payload in Request. Fix Docs for Post Request ([#683](https://github.com/apify/crawlee-python/pull/683)) ([e8b4d2d](https://github.com/apify/crawlee-python/commit/e8b4d2d4989fd9967403b828c914cb7ae2ef9b8b)) by [@Mantisus](https://github.com/Mantisus), closes [#668](https://github.com/apify/crawlee-python/issues/668)
- Accept string payload in the Request constructor ([#697](https://github.com/apify/crawlee-python/pull/697)) ([19f5add](https://github.com/apify/crawlee-python/commit/19f5addc0223d68389eea47864830c709335ab6e)) by [@vdusek](https://github.com/vdusek)
- Fix snapshots handling ([#692](https://github.com/apify/crawlee-python/pull/692)) ([4016c0d](https://github.com/apify/crawlee-python/commit/4016c0d8121a8950ab1df22188eac838a011c39f)) by [@Pijukatel](https://github.com/Pijukatel)


## [0.4.1](https://github.com/apify/crawlee-python/releases/tag/v0.4.1) (2024-11-11)

### 🚀 Features

- Add `max_crawl_depth` option to `BasicCrawler` ([#637](https://github.com/apify/crawlee-python/pull/637)) ([77deaa9](https://github.com/apify/crawlee-python/commit/77deaa964e2c1e74af1c5117a13d8d8257f0e27e)) by [@Prathamesh010](https://github.com/Prathamesh010), closes [#460](https://github.com/apify/crawlee-python/issues/460)
- Add BeautifulSoupParser type alias ([#674](https://github.com/apify/crawlee-python/pull/674)) ([b2cf88f](https://github.com/apify/crawlee-python/commit/b2cf88ffea8d75808c9210850a03fcc70b0b9e3d)) by [@Pijukatel](https://github.com/Pijukatel)

### 🐛 Bug Fixes

- Fix total_size usage in memory size monitoring ([#661](https://github.com/apify/crawlee-python/pull/661)) ([c2a3239](https://github.com/apify/crawlee-python/commit/c2a32397eecd5cc7f412c2af7269b004a8b2eaf2)) by [@janbuchar](https://github.com/janbuchar)
- Add HttpHeaders to module exports ([#664](https://github.com/apify/crawlee-python/pull/664)) ([f0c5ca7](https://github.com/apify/crawlee-python/commit/f0c5ca717d9f9e304d375da2c23552c26ca870da)) by [@vdusek](https://github.com/vdusek), closes [#663](https://github.com/apify/crawlee-python/issues/663)
- Fix unhandled ValueError in request handler result processing ([#666](https://github.com/apify/crawlee-python/pull/666)) ([0a99d7f](https://github.com/apify/crawlee-python/commit/0a99d7f693245eb9a065016fb6f2d268f6956805)) by [@janbuchar](https://github.com/janbuchar)
- Fix BaseDatasetClient.iter_items type hints ([#680](https://github.com/apify/crawlee-python/pull/680)) ([a968b1b](https://github.com/apify/crawlee-python/commit/a968b1be6fceb56676b0198a044c8fceac7c92a6)) by [@Pijukatel](https://github.com/Pijukatel)


## [0.4.0](https://github.com/apify/crawlee-python/releases/tag/v0.4.0) (2024-11-01)

### 🚀 Features

- [**breaking**] Add headers in unique key computation ([#609](https://github.com/apify/crawlee-python/pull/609)) ([6c4746f](https://github.com/apify/crawlee-python/commit/6c4746fa8ff86952a812b32a1d70dc910e76b43e)) by [@Prathamesh010](https://github.com/Prathamesh010), closes [#548](https://github.com/apify/crawlee-python/issues/548)
- Add `pre_navigation_hooks` to `PlaywrightCrawler` ([#631](https://github.com/apify/crawlee-python/pull/631)) ([5dd5b60](https://github.com/apify/crawlee-python/commit/5dd5b60e2a44d5bd3748b613790e1bee3232d6f3)) by [@Prathamesh010](https://github.com/Prathamesh010), closes [#427](https://github.com/apify/crawlee-python/issues/427)
- Add `always_enqueue` option to bypass URL deduplication ([#621](https://github.com/apify/crawlee-python/pull/621)) ([4e59fa4](https://github.com/apify/crawlee-python/commit/4e59fa46daaec05e52262cf62c26f28ddcd772af)) by [@Rutam21](https://github.com/Rutam21), closes [#547](https://github.com/apify/crawlee-python/issues/547)
- Split and add extra configuration to export_data method ([#580](https://github.com/apify/crawlee-python/pull/580)) ([6751635](https://github.com/apify/crawlee-python/commit/6751635e1785a4a27f60092c82f5dd0c40193d52)) by [@deshansh](https://github.com/deshansh), closes [#526](https://github.com/apify/crawlee-python/issues/526)

### 🐛 Bug Fixes

- Use strip in headers normalization ([#614](https://github.com/apify/crawlee-python/pull/614)) ([a15b21e](https://github.com/apify/crawlee-python/commit/a15b21e51deaf2b67738f95bc2b15c1c16d1775f)) by [@vdusek](https://github.com/vdusek)
- [**breaking**] Merge payload and data fields of Request ([#542](https://github.com/apify/crawlee-python/pull/542)) ([d06fcef](https://github.com/apify/crawlee-python/commit/d06fcef3fee44616ded5f587b9c7313b82a57cc7)) by [@vdusek](https://github.com/vdusek), closes [#560](https://github.com/apify/crawlee-python/issues/560)
- Default ProxyInfo port if httpx.URL port is None ([#619](https://github.com/apify/crawlee-python/pull/619)) ([8107a6f](https://github.com/apify/crawlee-python/commit/8107a6f97e8f16a330e7d02d3fc6ea34c5f78d77)) by [@steffansafey](https://github.com/steffansafey), closes [#618](https://github.com/apify/crawlee-python/issues/618)

### Chore

- [**breaking**] Remove Request.query_params field ([#639](https://github.com/apify/crawlee-python/pull/639)) ([6ec0ec4](https://github.com/apify/crawlee-python/commit/6ec0ec4fa0cef9b8bf893e70d99f068675c9c54c)) by [@vdusek](https://github.com/vdusek), closes [#615](https://github.com/apify/crawlee-python/issues/615)


## [0.3.9](https://github.com/apify/crawlee-python/releases/tag/v0.3.9) (2024-10-23)

### 🚀 Features

- Key-value store context helpers ([#584](https://github.com/apify/crawlee-python/pull/584)) ([fc15622](https://github.com/apify/crawlee-python/commit/fc156222c3747fc4cc7bd7666a21769845c7d0d5)) by [@janbuchar](https://github.com/janbuchar)
- Added get_public_url method to KeyValueStore ([#572](https://github.com/apify/crawlee-python/pull/572)) ([3a4ba8f](https://github.com/apify/crawlee-python/commit/3a4ba8f459903b6288aec40de2c3ca862e36abec)) by [@akshay11298](https://github.com/akshay11298), closes [#514](https://github.com/apify/crawlee-python/issues/514)

### 🐛 Bug Fixes

- Workaround for JSON value typing problems ([#581](https://github.com/apify/crawlee-python/pull/581)) ([403496a](https://github.com/apify/crawlee-python/commit/403496a53c12810351139a6e073238143ecc5930)) by [@janbuchar](https://github.com/janbuchar), closes [#563](https://github.com/apify/crawlee-python/issues/563)


## [0.3.8](https://github.com/apify/crawlee-python/releases/tag/v0.3.8) (2024-10-02)

### 🚀 Features

- Mask Playwright's "headless" headers ([#545](https://github.com/apify/crawlee-python/pull/545)) ([d1445e4](https://github.com/apify/crawlee-python/commit/d1445e4858fd804bb4a2e35efa1d2f5254d8df6b)) by [@vdusek](https://github.com/vdusek), closes [#401](https://github.com/apify/crawlee-python/issues/401)
- Add new model for `HttpHeaders` ([#544](https://github.com/apify/crawlee-python/pull/544)) ([854f2c1](https://github.com/apify/crawlee-python/commit/854f2c1e2e09cf398e04b1e153534282add1247e)) by [@vdusek](https://github.com/vdusek)

### 🐛 Bug Fixes

- Call `error_handler` for `SessionError` ([#557](https://github.com/apify/crawlee-python/pull/557)) ([e75ac4b](https://github.com/apify/crawlee-python/commit/e75ac4b70cd48a4ca9f8245cea3c5f3c188b8824)) by [@vdusek](https://github.com/vdusek), closes [#546](https://github.com/apify/crawlee-python/issues/546)
- Extend from `StrEnum` in `RequestState` to fix serialization ([#556](https://github.com/apify/crawlee-python/pull/556)) ([6bf35ba](https://github.com/apify/crawlee-python/commit/6bf35ba4a6913819706ebd1d2c1156a4c62f944e)) by [@vdusek](https://github.com/vdusek), closes [#551](https://github.com/apify/crawlee-python/issues/551)
- Add equality check to UserData model ([#562](https://github.com/apify/crawlee-python/pull/562)) ([899a25c](https://github.com/apify/crawlee-python/commit/899a25ca63f570b3c4d8d56c85a838b371fd3924)) by [@janbuchar](https://github.com/janbuchar)


## [0.3.7](https://github.com/apify/crawlee-python/releases/tag/v0.3.7) (2024-09-25)

### 🐛 Bug Fixes

- Improve `Request.user_data` serialization ([#540](https://github.com/apify/crawlee-python/pull/540)) ([de29c0e](https://github.com/apify/crawlee-python/commit/de29c0e6b737a9d2544c5382472618dde76eb2a5)) by [@janbuchar](https://github.com/janbuchar), closes [#524](https://github.com/apify/crawlee-python/issues/524)
- Adopt new version of curl-cffi ([#543](https://github.com/apify/crawlee-python/pull/543)) ([f6fcf48](https://github.com/apify/crawlee-python/commit/f6fcf48d99bfcb4b8e75c5c9c38dc8c265164a10)) by [@vdusek](https://github.com/vdusek)


## [0.3.6](https://github.com/apify/crawlee-python/releases/tag/v0.3.6) (2024-09-19)

### 🚀 Features

- Add HTTP/2 support for HTTPX client ([#513](https://github.com/apify/crawlee-python/pull/513)) ([0eb0a33](https://github.com/apify/crawlee-python/commit/0eb0a33411096011198e52c393f35730f1a0b6ac)) by [@vdusek](https://github.com/vdusek), closes [#512](https://github.com/apify/crawlee-python/issues/512)
- Expose extended unique key when creating a new Request ([#515](https://github.com/apify/crawlee-python/pull/515)) ([1807f41](https://github.com/apify/crawlee-python/commit/1807f419e47a815dd706d09acb0f3b3af8cfc691)) by [@vdusek](https://github.com/vdusek)
- Add header generator and integrate it into HTTPX client ([#530](https://github.com/apify/crawlee-python/pull/530)) ([b63f9f9](https://github.com/apify/crawlee-python/commit/b63f9f98c6613e095546ef544eab271d433e3379)) by [@vdusek](https://github.com/vdusek), closes [#402](https://github.com/apify/crawlee-python/issues/402)

### 🐛 Bug Fixes

- Use explicitly UTF-8 encoding in local storage ([#533](https://github.com/apify/crawlee-python/pull/533)) ([a3a0ab2](https://github.com/apify/crawlee-python/commit/a3a0ab2f6809b7a06319a77dfbf289df78638dea)) by [@vdusek](https://github.com/vdusek), closes [#532](https://github.com/apify/crawlee-python/issues/532)


## [0.3.5](https://github.com/apify/crawlee-python/releases/tag/v0.3.5) (2024-09-10)

### 🚀 Features

- Memory usage limit configuration via environment variables ([#502](https://github.com/apify/crawlee-python/pull/502)) ([c62e554](https://github.com/apify/crawlee-python/commit/c62e5545de6a1836f0514ebd3dd695e4fd856844)) by [@janbuchar](https://github.com/janbuchar)

### 🐛 Bug Fixes

- Http clients detect 4xx as errors by default ([#498](https://github.com/apify/crawlee-python/pull/498)) ([1895dca](https://github.com/apify/crawlee-python/commit/1895dca538f415feca37b4a030525c7c0d32f114)) by [@vdusek](https://github.com/vdusek), closes [#496](https://github.com/apify/crawlee-python/issues/496)
- Correctly handle log level configuration ([#508](https://github.com/apify/crawlee-python/pull/508)) ([7ea8fe6](https://github.com/apify/crawlee-python/commit/7ea8fe69f4a6146a1e417bebff60c08a85e2ca27)) by [@janbuchar](https://github.com/janbuchar)


## [0.3.4](https://github.com/apify/crawlee-python/releases/tag/v0.3.4) (2024-09-05)

### 🐛 Bug Fixes

- Expose basic crawling context ([#501](https://github.com/apify/crawlee-python/pull/501)) ([b484535](https://github.com/apify/crawlee-python/commit/b484535dbacc5d206a026f55a1d3e58edd375e91)) by [@vdusek](https://github.com/vdusek)


## [0.3.3](https://github.com/apify/crawlee-python/releases/tag/v0.3.3) (2024-09-05)

### 🐛 Bug Fixes

- Deduplicate requests by unique key before submitting them to the queue ([#499](https://github.com/apify/crawlee-python/pull/499)) ([6a3e0e7](https://github.com/apify/crawlee-python/commit/6a3e0e78490851c43cefb0497ce34ca52a31a25c)) by [@janbuchar](https://github.com/janbuchar)


## [0.3.2](https://github.com/apify/crawlee-python/releases/tag/v0.3.2) (2024-09-02)

### 🐛 Bug Fixes

- Double incrementation of `item_count` ([#443](https://github.com/apify/crawlee-python/pull/443)) ([cd9adf1](https://github.com/apify/crawlee-python/commit/cd9adf15731e8c4a39cb142b6d1a62909cafdc51)) by [@cadlagtrader](https://github.com/cadlagtrader), closes [#442](https://github.com/apify/crawlee-python/issues/442)
- Field alias in `BatchRequestsOperationResponse` ([#485](https://github.com/apify/crawlee-python/pull/485)) ([126a862](https://github.com/apify/crawlee-python/commit/126a8629cb5b989a0f9fe22156fb09731a34acd2)) by [@janbuchar](https://github.com/janbuchar)
- JSON handling with Parsel ([#490](https://github.com/apify/crawlee-python/pull/490)) ([ebf5755](https://github.com/apify/crawlee-python/commit/ebf575539ffb631ae131a1b801cec8f21dd0cf4c)) by [@janbuchar](https://github.com/janbuchar), closes [#488](https://github.com/apify/crawlee-python/issues/488)


## [0.3.1](https://github.com/apify/crawlee-python/releases/tag/v0.3.1) (2024-08-30)

### 🚀 Features

- Curl http client selects chrome impersonation by default ([#473](https://github.com/apify/crawlee-python/pull/473)) ([82dc939](https://github.com/apify/crawlee-python/commit/82dc93957b1a380ea975564dea5c6ba4639be548)) by [@vdusek](https://github.com/vdusek)


## [0.3.0](https://github.com/apify/crawlee-python/releases/tag/v0.3.0) (2024-08-27)

### 🚀 Features

- Implement ParselCrawler that adds support for Parsel ([#348](https://github.com/apify/crawlee-python/pull/348)) ([a3832e5](https://github.com/apify/crawlee-python/commit/a3832e527f022f32cce4a80055da3b7967b74522)) by [@asymness](https://github.com/asymness), closes [#335](https://github.com/apify/crawlee-python/issues/335)
- Add support for filling a web form ([#453](https://github.com/apify/crawlee-python/pull/453)) ([5a125b4](https://github.com/apify/crawlee-python/commit/5a125b464b2619000b92dacad4c3a7faa1869f29)) by [@vdusek](https://github.com/vdusek), closes [#305](https://github.com/apify/crawlee-python/issues/305)

### 🐛 Bug Fixes

- Remove indentation from statistics logging and print the data in tables ([#322](https://github.com/apify/crawlee-python/pull/322)) ([359b515](https://github.com/apify/crawlee-python/commit/359b515d647f064886f91441c2c01d3099e21035)) by [@TymeeK](https://github.com/TymeeK), closes [#306](https://github.com/apify/crawlee-python/issues/306)
- Remove redundant log, fix format ([#408](https://github.com/apify/crawlee-python/pull/408)) ([8d27e39](https://github.com/apify/crawlee-python/commit/8d27e3928c605d6eceb51a948453a15024fa2aa2)) by [@janbuchar](https://github.com/janbuchar)
- Dequeue items from RequestQueue in the correct order ([#411](https://github.com/apify/crawlee-python/pull/411)) ([96fc33e](https://github.com/apify/crawlee-python/commit/96fc33e2cc4631cae3c50dad9eace6407103a2a9)) by [@janbuchar](https://github.com/janbuchar)
- Relative URLS supports & If not a URL, pass #417 ([#431](https://github.com/apify/crawlee-python/pull/431)) ([ccd8145](https://github.com/apify/crawlee-python/commit/ccd81454166ece68391cdffedb8efe9e663361d9)) by [@black7375](https://github.com/black7375), closes [#417](https://github.com/apify/crawlee-python/issues/417)
- Typo in ProlongRequestLockResponse ([#458](https://github.com/apify/crawlee-python/pull/458)) ([30ccc3a](https://github.com/apify/crawlee-python/commit/30ccc3a4763bc3706a3bbeaedc95f9648f5ba09a)) by [@janbuchar](https://github.com/janbuchar)
- Add missing __all__ to top-level __init__.py file ([#463](https://github.com/apify/crawlee-python/pull/463)) ([353a1ce](https://github.com/apify/crawlee-python/commit/353a1ce28cd38c97ffb36dc1e6b0e86d3aef1a48)) by [@janbuchar](https://github.com/janbuchar)

### Refactor

- [**breaking**] RequestQueue and service management rehaul ([#429](https://github.com/apify/crawlee-python/pull/429)) ([b155a9f](https://github.com/apify/crawlee-python/commit/b155a9f602a163e891777bef5608072fb5d0156f)) by [@janbuchar](https://github.com/janbuchar), closes [#83](https://github.com/apify/crawlee-python/issues/83), [#174](https://github.com/apify/crawlee-python/issues/174), [#203](https://github.com/apify/crawlee-python/issues/203), [#423](https://github.com/apify/crawlee-python/issues/423)
- [**breaking**] Declare private and public interface ([#456](https://github.com/apify/crawlee-python/pull/456)) ([d6738df](https://github.com/apify/crawlee-python/commit/d6738df30586934e8d1aba50b9cd437a0ea40400)) by [@vdusek](https://github.com/vdusek)


## [0.2.1](https://github.com/apify/crawlee-python/releases/tag/v0.2.1) (2024-08-05)

### 🐛 Bug Fixes

- Do not import curl impersonate in http clients init ([#396](https://github.com/apify/crawlee-python/pull/396)) ([3bb8009](https://github.com/apify/crawlee-python/commit/3bb80093e61c1615f869ecd5ab80b061e0e5db36)) by [@vdusek](https://github.com/vdusek)


## [0.2.0](https://github.com/apify/crawlee-python/releases/tag/v0.2.0) (2024-08-05)

### 🚀 Features

- Add new curl impersonate HTTP client ([#387](https://github.com/apify/crawlee-python/pull/387)) ([9c06260](https://github.com/apify/crawlee-python/commit/9c06260c0ee958522caa9322001a3186e9e43af4)) by [@vdusek](https://github.com/vdusek), closes [#292](https://github.com/apify/crawlee-python/issues/292)
- **playwright:** `infinite_scroll` helper ([#393](https://github.com/apify/crawlee-python/pull/393)) ([34f74bd](https://github.com/apify/crawlee-python/commit/34f74bdcffb42a6c876a856e1c89923d9b3e60bd)) by [@janbuchar](https://github.com/janbuchar)


## [0.1.2](https://github.com/apify/crawlee-python/releases/tag/v0.1.2) (2024-07-30)

### 🚀 Features

- Add URL validation ([#343](https://github.com/apify/crawlee-python/pull/343)) ([1514538](https://github.com/apify/crawlee-python/commit/15145388009c85ab54dc72ea8f2d07efd78f80fd)) by [@vdusek](https://github.com/vdusek), closes [#300](https://github.com/apify/crawlee-python/issues/300)

### 🐛 Bug Fixes

- Minor log fix ([#341](https://github.com/apify/crawlee-python/pull/341)) ([0688bf1](https://github.com/apify/crawlee-python/commit/0688bf1860534ab6b2a85dc850bf3d56507ab154)) by [@souravjain540](https://github.com/souravjain540)
- Also use error_handler for context pipeline errors ([#331](https://github.com/apify/crawlee-python/pull/331)) ([7a66445](https://github.com/apify/crawlee-python/commit/7a664456b45c7e429b4c90aaf1c09d5796b93e3d)) by [@janbuchar](https://github.com/janbuchar), closes [#296](https://github.com/apify/crawlee-python/issues/296)
- Strip whitespace from href in enqueue_links ([#346](https://github.com/apify/crawlee-python/pull/346)) ([8a3174a](https://github.com/apify/crawlee-python/commit/8a3174aed24f9eb4f9ac415a79a58685a081cde2)) by [@janbuchar](https://github.com/janbuchar), closes [#337](https://github.com/apify/crawlee-python/issues/337)
- Warn instead of crashing when an empty dataset is being exported ([#342](https://github.com/apify/crawlee-python/pull/342)) ([22b95d1](https://github.com/apify/crawlee-python/commit/22b95d1948d4acd23a010898fa6af2f491e7f514)) by [@janbuchar](https://github.com/janbuchar), closes [#334](https://github.com/apify/crawlee-python/issues/334)
- Avoid Github rate limiting in project bootstrapping test ([#364](https://github.com/apify/crawlee-python/pull/364)) ([992f07f](https://github.com/apify/crawlee-python/commit/992f07f266f7b8433d99e9a179f277995f81eb17)) by [@janbuchar](https://github.com/janbuchar)
- Pass crawler configuration to storages ([#375](https://github.com/apify/crawlee-python/pull/375)) ([b2d3a52](https://github.com/apify/crawlee-python/commit/b2d3a52712abe21f4a4a5db4e20c80afe72c27de)) by [@janbuchar](https://github.com/janbuchar)
- Purge request queue on repeated crawler runs ([#377](https://github.com/apify/crawlee-python/pull/377)) ([7ad3d69](https://github.com/apify/crawlee-python/commit/7ad3d6908e153c590bff72478af7ee3239a249bc)) by [@janbuchar](https://github.com/janbuchar), closes [#152](https://github.com/apify/crawlee-python/issues/152)


## [0.1.1](https://github.com/apify/crawlee-python/releases/tag/v0.1.1) (2024-07-19)

### 🚀 Features

- Expose crawler log ([#316](https://github.com/apify/crawlee-python/pull/316)) ([ae475fa](https://github.com/apify/crawlee-python/commit/ae475fa450c4fe053620d7b7eb475f3d58804674)) by [@vdusek](https://github.com/vdusek), closes [#303](https://github.com/apify/crawlee-python/issues/303)
- Integrate proxies into `PlaywrightCrawler` ([#325](https://github.com/apify/crawlee-python/pull/325)) ([2e072b6](https://github.com/apify/crawlee-python/commit/2e072b6ad7d5d82d96a7b489cafb87e7bfaf6e83)) by [@vdusek](https://github.com/vdusek)
- Blocking detection for playwright crawler ([#328](https://github.com/apify/crawlee-python/pull/328)) ([49ff6e2](https://github.com/apify/crawlee-python/commit/49ff6e25c12a97550eee718d64bb4130f9990189)) by [@vdusek](https://github.com/vdusek), closes [#239](https://github.com/apify/crawlee-python/issues/239)

### 🐛 Bug Fixes

- Pylance reportPrivateImportUsage errors ([#313](https://github.com/apify/crawlee-python/pull/313)) ([09d7203](https://github.com/apify/crawlee-python/commit/09d72034d5db8c47f461111ec093761935a3e2ef)) by [@vdusek](https://github.com/vdusek), closes [#283](https://github.com/apify/crawlee-python/issues/283)
- Set httpx logging to warning ([#314](https://github.com/apify/crawlee-python/pull/314)) ([1585def](https://github.com/apify/crawlee-python/commit/1585defffb2c0c844fab39bbc0e0b793d6169cbf)) by [@vdusek](https://github.com/vdusek), closes [#302](https://github.com/apify/crawlee-python/issues/302)
- Byte size serialization in MemoryInfo ([#245](https://github.com/apify/crawlee-python/pull/245)) ([a030174](https://github.com/apify/crawlee-python/commit/a0301746c2df076d281708344fb906e1c42e0790)) by [@janbuchar](https://github.com/janbuchar)
- Project bootstrapping in existing folder ([#318](https://github.com/apify/crawlee-python/pull/318)) ([c630818](https://github.com/apify/crawlee-python/commit/c630818538e0c37217ab73f6c6da05505ed8b364)) by [@janbuchar](https://github.com/janbuchar), closes [#301](https://github.com/apify/crawlee-python/issues/301)


## [0.1.0](https://github.com/apify/crawlee-python/releases/tag/v0.1.0) (2024-07-08)

### 🚀 Features

- Project templates ([#237](https://github.com/apify/crawlee-python/pull/237)) ([c23c12c](https://github.com/apify/crawlee-python/commit/c23c12c66688f825f74deb39702f07cc6c6bbc46)) by [@janbuchar](https://github.com/janbuchar), closes [#215](https://github.com/apify/crawlee-python/issues/215)

### 🐛 Bug Fixes

- CLI UX improvements ([#271](https://github.com/apify/crawlee-python/pull/271)) ([123d515](https://github.com/apify/crawlee-python/commit/123d515b224c663577bfe0fab387d0aa11e5e4d4)) by [@janbuchar](https://github.com/janbuchar), closes [#267](https://github.com/apify/crawlee-python/issues/267)
- Error handling in CLI and templates documentation ([#273](https://github.com/apify/crawlee-python/pull/273)) ([61083c3](https://github.com/apify/crawlee-python/commit/61083c33434d431a118538f15bfa9a68c312ab03)) by [@vdusek](https://github.com/vdusek), closes [#268](https://github.com/apify/crawlee-python/issues/268)


## [0.0.7](https://github.com/apify/crawlee-python/releases/tag/v0.0.7) (2024-06-27)

### 🐛 Bug Fixes

- Do not wait for consistency in request queue ([#235](https://github.com/apify/crawlee-python/pull/235)) ([03ff138](https://github.com/apify/crawlee-python/commit/03ff138aadaf8e915abc7fafb854fe12947b9696)) by [@vdusek](https://github.com/vdusek)
- Selector handling in BeautifulSoupCrawler enqueue_links ([#231](https://github.com/apify/crawlee-python/pull/231)) ([896501e](https://github.com/apify/crawlee-python/commit/896501edb44f801409fec95cb3e5f2bcfcb4188d)) by [@janbuchar](https://github.com/janbuchar), closes [#230](https://github.com/apify/crawlee-python/issues/230)
- Handle blocked request ([#234](https://github.com/apify/crawlee-python/pull/234)) ([f8ef79f](https://github.com/apify/crawlee-python/commit/f8ef79ffcb7410713182af716d37dbbaad66fdbc)) by [@Mantisus](https://github.com/Mantisus)
- Improve AutoscaledPool state management ([#241](https://github.com/apify/crawlee-python/pull/241)) ([fdea3d1](https://github.com/apify/crawlee-python/commit/fdea3d16b13afe70039d864de861486c760aa0ba)) by [@janbuchar](https://github.com/janbuchar), closes [#236](https://github.com/apify/crawlee-python/issues/236)


## [0.0.6](https://github.com/apify/crawlee-python/releases/tag/v0.0.6) (2024-06-25)

### 🚀 Features

- Maintain a global configuration instance ([#207](https://github.com/apify/crawlee-python/pull/207)) ([e003aa6](https://github.com/apify/crawlee-python/commit/e003aa63d859bec8199d0c890b5c9604f163ccd3)) by [@janbuchar](https://github.com/janbuchar)
- Add max requests per crawl to `BasicCrawler` ([#198](https://github.com/apify/crawlee-python/pull/198)) ([b5b3053](https://github.com/apify/crawlee-python/commit/b5b3053f43381601274e4034d07b4bf41720c7c2)) by [@vdusek](https://github.com/vdusek)
- Add support decompress *br* response content ([#226](https://github.com/apify/crawlee-python/pull/226)) ([a3547b9](https://github.com/apify/crawlee-python/commit/a3547b9c882dc5333a4fcd1223687ef85e79138d)) by [@Mantisus](https://github.com/Mantisus)
- BasicCrawler.export_data helper ([#222](https://github.com/apify/crawlee-python/pull/222)) ([237ec78](https://github.com/apify/crawlee-python/commit/237ec789b7dccc17cc57ef47ec56bcf73c6ca006)) by [@janbuchar](https://github.com/janbuchar), closes [#211](https://github.com/apify/crawlee-python/issues/211)
- Automatic logging setup ([#229](https://github.com/apify/crawlee-python/pull/229)) ([a67b72f](https://github.com/apify/crawlee-python/commit/a67b72faacd75674071bae496d59e1c60636350c)) by [@janbuchar](https://github.com/janbuchar), closes [#214](https://github.com/apify/crawlee-python/issues/214)

### 🐛 Bug Fixes

- Handling of relative URLs in add_requests ([#213](https://github.com/apify/crawlee-python/pull/213)) ([8aa8c57](https://github.com/apify/crawlee-python/commit/8aa8c57f44149caa0e01950a5d773726f261699a)) by [@janbuchar](https://github.com/janbuchar), closes [#202](https://github.com/apify/crawlee-python/issues/202), [#204](https://github.com/apify/crawlee-python/issues/204)
- Graceful exit in BasicCrawler.run ([#224](https://github.com/apify/crawlee-python/pull/224)) ([337286e](https://github.com/apify/crawlee-python/commit/337286e1b721cf61f57bc0ff3ead08df1f4f5448)) by [@janbuchar](https://github.com/janbuchar), closes [#212](https://github.com/apify/crawlee-python/issues/212)


## [0.0.5](https://github.com/apify/crawlee-python/releases/tag/v0.0.5) (2024-06-21)

### 🚀 Features

- Browser rotation and better browser abstraction ([#177](https://github.com/apify/crawlee-python/pull/177)) ([a42ae6f](https://github.com/apify/crawlee-python/commit/a42ae6f53c5e24678f04011c3684290b68684016)) by [@vdusek](https://github.com/vdusek), closes [#131](https://github.com/apify/crawlee-python/issues/131)
- Add emit persist state event to event manager ([#181](https://github.com/apify/crawlee-python/pull/181)) ([97f6c68](https://github.com/apify/crawlee-python/commit/97f6c68275b65f76c62b6d16d94354fc7f00d336)) by [@vdusek](https://github.com/vdusek)
- Batched request addition in RequestQueue ([#186](https://github.com/apify/crawlee-python/pull/186)) ([f48c806](https://github.com/apify/crawlee-python/commit/f48c8068fe16ce3dd4c46fc248733346c0621411)) by [@vdusek](https://github.com/vdusek)
- Add storage helpers to crawler & context ([#192](https://github.com/apify/crawlee-python/pull/192)) ([f8f4066](https://github.com/apify/crawlee-python/commit/f8f4066d8b32d6e7dc0d999a5aa8db75f99b43b8)) by [@vdusek](https://github.com/vdusek), closes [#98](https://github.com/apify/crawlee-python/issues/98), [#100](https://github.com/apify/crawlee-python/issues/100), [#172](https://github.com/apify/crawlee-python/issues/172)
- Handle all supported configuration options ([#199](https://github.com/apify/crawlee-python/pull/199)) ([23c901c](https://github.com/apify/crawlee-python/commit/23c901cd68cf14b4041ee03568622ee32822e94b)) by [@janbuchar](https://github.com/janbuchar), closes [#84](https://github.com/apify/crawlee-python/issues/84)
- Add Playwright's enqueue links helper ([#196](https://github.com/apify/crawlee-python/pull/196)) ([849d73c](https://github.com/apify/crawlee-python/commit/849d73cc7d137171b98f9f2ab85374e8beec0dad)) by [@vdusek](https://github.com/vdusek)

### 🐛 Bug Fixes

- Tmp path in tests is working ([#164](https://github.com/apify/crawlee-python/pull/164)) ([382b6f4](https://github.com/apify/crawlee-python/commit/382b6f48174bdac3931cc379eaf770ab06f826dc)) by [@vdusek](https://github.com/vdusek), closes [#159](https://github.com/apify/crawlee-python/issues/159)
- Add explicit err msgs for missing pckg extras during import ([#165](https://github.com/apify/crawlee-python/pull/165)) ([200ebfa](https://github.com/apify/crawlee-python/commit/200ebfa63d6e20e17c8ca29544ef7229ed0df308)) by [@vdusek](https://github.com/vdusek), closes [#155](https://github.com/apify/crawlee-python/issues/155)
- Make timedelta_ms accept string-encoded numbers ([#190](https://github.com/apify/crawlee-python/pull/190)) ([d8426ff](https://github.com/apify/crawlee-python/commit/d8426ff41e36f701af459ad17552fee39637674d)) by [@janbuchar](https://github.com/janbuchar)
- **deps:** Update dependency psutil to v6 ([#193](https://github.com/apify/crawlee-python/pull/193)) ([eb91f51](https://github.com/apify/crawlee-python/commit/eb91f51e19da406e3f9293e5336c1f85fc7885a4)) by [@renovate[bot]](https://github.com/renovate[bot])
- Improve compatibility between ProxyConfiguration and its SDK counterpart ([#201](https://github.com/apify/crawlee-python/pull/201)) ([1a76124](https://github.com/apify/crawlee-python/commit/1a76124080d561e0153a4dda0bdb0d9863c3aab6)) by [@janbuchar](https://github.com/janbuchar)
- Correct return type of storage get_info methods ([#200](https://github.com/apify/crawlee-python/pull/200)) ([332673c](https://github.com/apify/crawlee-python/commit/332673c4fb519b80846df7fb8cd8bb521538a8a4)) by [@janbuchar](https://github.com/janbuchar)
- Type error in statistics persist state ([#206](https://github.com/apify/crawlee-python/pull/206)) ([96ceef6](https://github.com/apify/crawlee-python/commit/96ceef697769cd57bd1a50b6615cf1e70549bd2d)) by [@vdusek](https://github.com/vdusek), closes [#194](https://github.com/apify/crawlee-python/issues/194)


## [0.0.4](https://github.com/apify/crawlee-python/releases/tag/v0.0.4) (2024-05-30)

### 🚀 Features

- Capture statistics about the crawler run ([#142](https://github.com/apify/crawlee-python/pull/142)) ([eeebe9b](https://github.com/apify/crawlee-python/commit/eeebe9b1e24338d68a0a55228bbfc717f4d9d295)) by [@janbuchar](https://github.com/janbuchar), closes [#97](https://github.com/apify/crawlee-python/issues/97)
- Proxy configuration ([#156](https://github.com/apify/crawlee-python/pull/156)) ([5c3753a](https://github.com/apify/crawlee-python/commit/5c3753a5527b1d01f7260b9e4c566e43f956a5e8)) by [@janbuchar](https://github.com/janbuchar), closes [#136](https://github.com/apify/crawlee-python/issues/136)
- Add first version of browser pool and playwright crawler ([#161](https://github.com/apify/crawlee-python/pull/161)) ([2d2a050](https://github.com/apify/crawlee-python/commit/2d2a0505b1c2b1529a8835163ca97d1ec2a6e44a)) by [@vdusek](https://github.com/vdusek)


## [0.0.3](https://github.com/apify/crawlee-python/releases/tag/v0.0.3) (2024-05-13)

### 🚀 Features

- AutoscaledPool implementation ([#55](https://github.com/apify/crawlee-python/pull/55)) ([621ada2](https://github.com/apify/crawlee-python/commit/621ada2bd1ba4e2346fb948dc02686e2b37e3856)) by [@janbuchar](https://github.com/janbuchar), closes [#19](https://github.com/apify/crawlee-python/issues/19)
- Add Snapshotter ([#20](https://github.com/apify/crawlee-python/pull/20)) ([492ee38](https://github.com/apify/crawlee-python/commit/492ee38c893b8f54e9583dd492576c5106e29881)) by [@vdusek](https://github.com/vdusek)
- Implement BasicCrawler ([#56](https://github.com/apify/crawlee-python/pull/56)) ([6da971f](https://github.com/apify/crawlee-python/commit/6da971fcddbf8b6795346c88e295dada28e7b1d3)) by [@janbuchar](https://github.com/janbuchar), closes [#30](https://github.com/apify/crawlee-python/issues/30)
- BeautifulSoupCrawler ([#107](https://github.com/apify/crawlee-python/pull/107)) ([4974dfa](https://github.com/apify/crawlee-python/commit/4974dfa20c7911ee073438fd388e60ba4b2c07db)) by [@janbuchar](https://github.com/janbuchar), closes [#31](https://github.com/apify/crawlee-python/issues/31)
- Add_requests and enqueue_links context helpers ([#120](https://github.com/apify/crawlee-python/pull/120)) ([dc850a5](https://github.com/apify/crawlee-python/commit/dc850a5778b105ff09e19eaecbb0a12d94798a62)) by [@janbuchar](https://github.com/janbuchar), closes [#5](https://github.com/apify/crawlee-python/issues/5)
- Use SessionPool in BasicCrawler ([#128](https://github.com/apify/crawlee-python/pull/128)) ([9fc4648](https://github.com/apify/crawlee-python/commit/9fc464837e596b3b5a7cd818b6d617550e249352)) by [@janbuchar](https://github.com/janbuchar), closes [#110](https://github.com/apify/crawlee-python/issues/110)
- Add base storage client and resource subclients ([#138](https://github.com/apify/crawlee-python/pull/138)) ([44d6597](https://github.com/apify/crawlee-python/commit/44d65974e4837576918069d7e63f8b804964971a)) by [@vdusek](https://github.com/vdusek)

### 🐛 Bug Fixes

- **deps:** Update dependency docutils to ^0.21.0 ([#101](https://github.com/apify/crawlee-python/pull/101)) ([534b613](https://github.com/apify/crawlee-python/commit/534b613f7cdfe7adf38b548ee48537db3167d1ec)) by [@renovate[bot]](https://github.com/renovate[bot])
- **deps:** Update dependency eval-type-backport to ^0.2.0 ([#124](https://github.com/apify/crawlee-python/pull/124)) ([c9e69a8](https://github.com/apify/crawlee-python/commit/c9e69a8534f4d82d9a6314947d76a86bcb744607)) by [@renovate[bot]](https://github.com/renovate[bot])
- Fire local SystemInfo events every second ([#144](https://github.com/apify/crawlee-python/pull/144)) ([f1359fa](https://github.com/apify/crawlee-python/commit/f1359fa7eea23f8153ad711287c073e45d498401)) by [@vdusek](https://github.com/vdusek)
- Storage manager & purging the defaults ([#150](https://github.com/apify/crawlee-python/pull/150)) ([851042f](https://github.com/apify/crawlee-python/commit/851042f25ad07e25651768e476f098ef0ed21914)) by [@vdusek](https://github.com/vdusek)


<!-- generated by git-cliff -->