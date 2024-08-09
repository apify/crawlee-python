# Changelog

All notable changes to this project will be documented in this file.

## 0.2.2 - **not yet released**

### 🚀 Features

- Implement ParselCrawler that adds support for Parsel ([#348](https://github.com/apify/crawlee-python/pull/348), closes [#335](https://github.com/apify/crawlee-python/issues/335)) ([a3832e5](https://github.com/apify/crawlee-python/commit/a3832e527f022f32cce4a80055da3b7967b74522)) by [@asymness](https://github.com/asymness)

### 🐛 Bug Fixes

- Remove indentation from statistics logging and print the data in tables ([#322](https://github.com/apify/crawlee-python/pull/322), closes [#306](https://github.com/apify/crawlee-python/issues/306)) ([359b515](https://github.com/apify/crawlee-python/commit/359b515d647f064886f91441c2c01d3099e21035)) by [@TymeeK](https://github.com/TymeeK)
- Remove redundant log, fix format ([#408](https://github.com/apify/crawlee-python/pull/408)) ([8d27e39](https://github.com/apify/crawlee-python/commit/8d27e3928c605d6eceb51a948453a15024fa2aa2)) by [@janbuchar](https://github.com/janbuchar)
- Dequeue items from RequestQueue in the correct order ([#411](https://github.com/apify/crawlee-python/pull/411)) ([96fc33e](https://github.com/apify/crawlee-python/commit/96fc33e2cc4631cae3c50dad9eace6407103a2a9)) by [@janbuchar](https://github.com/janbuchar)

## [0.2.1](https://github.com/apify/crawlee-python/releases/tag/v0.2.1) (2024-08-05)

### 🐛 Bug Fixes

- Do not import curl impersonate in http clients init ([#396](https://github.com/apify/crawlee-python/pull/396)) ([3bb8009](https://github.com/apify/crawlee-python/commit/3bb80093e61c1615f869ecd5ab80b061e0e5db36)) by [@vdusek](https://github.com/vdusek)

## [0.2.0](https://github.com/apify/crawlee-python/releases/tag/v0.2.0) (2024-08-05)

### 🚀 Features

- Add new curl impersonate HTTP client ([#387](https://github.com/apify/crawlee-python/pull/387), closes [#292](https://github.com/apify/crawlee-python/issues/292)) ([9c06260](https://github.com/apify/crawlee-python/commit/9c06260c0ee958522caa9322001a3186e9e43af4)) by [@vdusek](https://github.com/vdusek)
- *(playwright)* `infinite_scroll` helper ([#393](https://github.com/apify/crawlee-python/pull/393)) ([34f74bd](https://github.com/apify/crawlee-python/commit/34f74bdcffb42a6c876a856e1c89923d9b3e60bd)) by [@janbuchar](https://github.com/janbuchar)

## [0.1.2](https://github.com/apify/crawlee-python/releases/tag/v0.1.2) (2024-07-30)

### 🚀 Features

- Add URL validation ([#343](https://github.com/apify/crawlee-python/pull/343), closes [#300](https://github.com/apify/crawlee-python/issues/300)) ([1514538](https://github.com/apify/crawlee-python/commit/15145388009c85ab54dc72ea8f2d07efd78f80fd)) by [@vdusek](https://github.com/vdusek)

### 🐛 Bug Fixes

- Minor log fix ([#341](https://github.com/apify/crawlee-python/pull/341)) ([0688bf1](https://github.com/apify/crawlee-python/commit/0688bf1860534ab6b2a85dc850bf3d56507ab154)) by [@souravjain540](https://github.com/souravjain540)
- Also use error_handler for context pipeline errors ([#331](https://github.com/apify/crawlee-python/pull/331), closes [#296](https://github.com/apify/crawlee-python/issues/296)) ([7a66445](https://github.com/apify/crawlee-python/commit/7a664456b45c7e429b4c90aaf1c09d5796b93e3d)) by [@janbuchar](https://github.com/janbuchar)
- Strip whitespace from href in enqueue_links ([#346](https://github.com/apify/crawlee-python/pull/346), closes [#337](https://github.com/apify/crawlee-python/issues/337)) ([8a3174a](https://github.com/apify/crawlee-python/commit/8a3174aed24f9eb4f9ac415a79a58685a081cde2)) by [@janbuchar](https://github.com/janbuchar)
- Warn instead of crashing when an empty dataset is being exported ([#342](https://github.com/apify/crawlee-python/pull/342), closes [#334](https://github.com/apify/crawlee-python/issues/334)) ([22b95d1](https://github.com/apify/crawlee-python/commit/22b95d1948d4acd23a010898fa6af2f491e7f514)) by [@janbuchar](https://github.com/janbuchar)
- Avoid Github rate limiting in project bootstrapping test ([#364](https://github.com/apify/crawlee-python/pull/364)) ([992f07f](https://github.com/apify/crawlee-python/commit/992f07f266f7b8433d99e9a179f277995f81eb17)) by [@janbuchar](https://github.com/janbuchar)
- Pass crawler configuration to storages ([#375](https://github.com/apify/crawlee-python/pull/375)) ([b2d3a52](https://github.com/apify/crawlee-python/commit/b2d3a52712abe21f4a4a5db4e20c80afe72c27de)) by [@janbuchar](https://github.com/janbuchar)
- Purge request queue on repeated crawler runs ([#377](https://github.com/apify/crawlee-python/pull/377), closes [#152](https://github.com/apify/crawlee-python/issues/152)) ([7ad3d69](https://github.com/apify/crawlee-python/commit/7ad3d6908e153c590bff72478af7ee3239a249bc)) by [@janbuchar](https://github.com/janbuchar)

## [0.1.1](https://github.com/apify/crawlee-python/releases/tag/v0.1.1) (2024-07-19)

### 🚀 Features

- Expose crawler log ([#316](https://github.com/apify/crawlee-python/pull/316), closes [#303](https://github.com/apify/crawlee-python/issues/303)) ([ae475fa](https://github.com/apify/crawlee-python/commit/ae475fa450c4fe053620d7b7eb475f3d58804674)) by [@vdusek](https://github.com/vdusek)
- Integrate proxies into `PlaywrightCrawler` ([#325](https://github.com/apify/crawlee-python/pull/325)) ([2e072b6](https://github.com/apify/crawlee-python/commit/2e072b6ad7d5d82d96a7b489cafb87e7bfaf6e83)) by [@vdusek](https://github.com/vdusek)
- Blocking detection for playwright crawler ([#328](https://github.com/apify/crawlee-python/pull/328), closes [#239](https://github.com/apify/crawlee-python/issues/239)) ([49ff6e2](https://github.com/apify/crawlee-python/commit/49ff6e25c12a97550eee718d64bb4130f9990189)) by [@vdusek](https://github.com/vdusek)

### 🐛 Bug Fixes

- Pylance reportPrivateImportUsage errors ([#313](https://github.com/apify/crawlee-python/pull/313), closes [#283](https://github.com/apify/crawlee-python/issues/283)) ([09d7203](https://github.com/apify/crawlee-python/commit/09d72034d5db8c47f461111ec093761935a3e2ef)) by [@vdusek](https://github.com/vdusek)
- Set httpx logging to warning ([#314](https://github.com/apify/crawlee-python/pull/314), closes [#302](https://github.com/apify/crawlee-python/issues/302)) ([1585def](https://github.com/apify/crawlee-python/commit/1585defffb2c0c844fab39bbc0e0b793d6169cbf)) by [@vdusek](https://github.com/vdusek)
- Byte size serialization in MemoryInfo ([#245](https://github.com/apify/crawlee-python/pull/245)) ([a030174](https://github.com/apify/crawlee-python/commit/a0301746c2df076d281708344fb906e1c42e0790)) by [@janbuchar](https://github.com/janbuchar)
- Project bootstrapping in existing folder ([#318](https://github.com/apify/crawlee-python/pull/318), closes [#301](https://github.com/apify/crawlee-python/issues/301)) ([c630818](https://github.com/apify/crawlee-python/commit/c630818538e0c37217ab73f6c6da05505ed8b364)) by [@janbuchar](https://github.com/janbuchar)

## [0.1.0](https://github.com/apify/crawlee-python/releases/tag/v0.1.0) (2024-07-08)

### 🚀 Features

- Project templates ([#237](https://github.com/apify/crawlee-python/pull/237), closes [#215](https://github.com/apify/crawlee-python/issues/215)) ([c23c12c](https://github.com/apify/crawlee-python/commit/c23c12c66688f825f74deb39702f07cc6c6bbc46)) by [@janbuchar](https://github.com/janbuchar)

### 🐛 Bug Fixes

- CLI UX improvements ([#271](https://github.com/apify/crawlee-python/pull/271), closes [#267](https://github.com/apify/crawlee-python/issues/267)) ([123d515](https://github.com/apify/crawlee-python/commit/123d515b224c663577bfe0fab387d0aa11e5e4d4)) by [@janbuchar](https://github.com/janbuchar)
- Error handling in CLI and templates documentation ([#273](https://github.com/apify/crawlee-python/pull/273), closes [#268](https://github.com/apify/crawlee-python/issues/268)) ([61083c3](https://github.com/apify/crawlee-python/commit/61083c33434d431a118538f15bfa9a68c312ab03)) by [@vdusek](https://github.com/vdusek)

## [0.0.7](https://github.com/apify/crawlee-python/releases/tag/v0.0.7) (2024-06-27)

### 🐛 Bug Fixes

- Do not wait for consistency in request queue ([#235](https://github.com/apify/crawlee-python/pull/235)) ([03ff138](https://github.com/apify/crawlee-python/commit/03ff138aadaf8e915abc7fafb854fe12947b9696)) by [@vdusek](https://github.com/vdusek)
- Selector handling in BeautifulSoupCrawler enqueue_links ([#231](https://github.com/apify/crawlee-python/pull/231), closes [#230](https://github.com/apify/crawlee-python/issues/230)) ([896501e](https://github.com/apify/crawlee-python/commit/896501edb44f801409fec95cb3e5f2bcfcb4188d)) by [@janbuchar](https://github.com/janbuchar)
- Handle blocked request ([#234](https://github.com/apify/crawlee-python/pull/234)) ([f8ef79f](https://github.com/apify/crawlee-python/commit/f8ef79ffcb7410713182af716d37dbbaad66fdbc)) by [@Mantisus](https://github.com/Mantisus)
- Improve AutoscaledPool state management ([#241](https://github.com/apify/crawlee-python/pull/241), closes [#236](https://github.com/apify/crawlee-python/issues/236)) ([fdea3d1](https://github.com/apify/crawlee-python/commit/fdea3d16b13afe70039d864de861486c760aa0ba)) by [@janbuchar](https://github.com/janbuchar)

## [0.0.6](https://github.com/apify/crawlee-python/releases/tag/v0.0.6) (2024-06-25)

### 🚀 Features

- Maintain a global configuration instance ([#207](https://github.com/apify/crawlee-python/pull/207)) ([e003aa6](https://github.com/apify/crawlee-python/commit/e003aa63d859bec8199d0c890b5c9604f163ccd3)) by [@janbuchar](https://github.com/janbuchar)
- Add max requests per crawl to `BasicCrawler` ([#198](https://github.com/apify/crawlee-python/pull/198)) ([b5b3053](https://github.com/apify/crawlee-python/commit/b5b3053f43381601274e4034d07b4bf41720c7c2)) by [@vdusek](https://github.com/vdusek)
- Add support decompress *br* response content ([#226](https://github.com/apify/crawlee-python/pull/226)) ([a3547b9](https://github.com/apify/crawlee-python/commit/a3547b9c882dc5333a4fcd1223687ef85e79138d)) by [@Mantisus](https://github.com/Mantisus)
- BasicCrawler.export_data helper ([#222](https://github.com/apify/crawlee-python/pull/222), closes [#211](https://github.com/apify/crawlee-python/issues/211)) ([237ec78](https://github.com/apify/crawlee-python/commit/237ec789b7dccc17cc57ef47ec56bcf73c6ca006)) by [@janbuchar](https://github.com/janbuchar)
- Automatic logging setup ([#229](https://github.com/apify/crawlee-python/pull/229), closes [#214](https://github.com/apify/crawlee-python/issues/214)) ([a67b72f](https://github.com/apify/crawlee-python/commit/a67b72faacd75674071bae496d59e1c60636350c)) by [@janbuchar](https://github.com/janbuchar)

### 🐛 Bug Fixes

- Handling of relative URLs in add_requests ([#213](https://github.com/apify/crawlee-python/pull/213), closes [#202](https://github.com/apify/crawlee-python/issues/202), [#204](https://github.com/apify/crawlee-python/issues/204)) ([8aa8c57](https://github.com/apify/crawlee-python/commit/8aa8c57f44149caa0e01950a5d773726f261699a)) by [@janbuchar](https://github.com/janbuchar)
- Graceful exit in BasicCrawler.run ([#224](https://github.com/apify/crawlee-python/pull/224), closes [#212](https://github.com/apify/crawlee-python/issues/212)) ([337286e](https://github.com/apify/crawlee-python/commit/337286e1b721cf61f57bc0ff3ead08df1f4f5448)) by [@janbuchar](https://github.com/janbuchar)

## [0.0.5](https://github.com/apify/crawlee-python/releases/tag/v0.0.5) (2024-06-21)

### 🚀 Features

- Browser rotation and better browser abstraction ([#177](https://github.com/apify/crawlee-python/pull/177), closes [#131](https://github.com/apify/crawlee-python/issues/131)) ([a42ae6f](https://github.com/apify/crawlee-python/commit/a42ae6f53c5e24678f04011c3684290b68684016)) by [@vdusek](https://github.com/vdusek)
- Add emit persist state event to event manager ([#181](https://github.com/apify/crawlee-python/pull/181)) ([97f6c68](https://github.com/apify/crawlee-python/commit/97f6c68275b65f76c62b6d16d94354fc7f00d336)) by [@vdusek](https://github.com/vdusek)
- Batched request addition in RequestQueue ([#186](https://github.com/apify/crawlee-python/pull/186)) ([f48c806](https://github.com/apify/crawlee-python/commit/f48c8068fe16ce3dd4c46fc248733346c0621411)) by [@vdusek](https://github.com/vdusek)
- Add storage helpers to crawler & context ([#192](https://github.com/apify/crawlee-python/pull/192), closes [#98](https://github.com/apify/crawlee-python/issues/98), [#100](https://github.com/apify/crawlee-python/issues/100), [#172](https://github.com/apify/crawlee-python/issues/172)) ([f8f4066](https://github.com/apify/crawlee-python/commit/f8f4066d8b32d6e7dc0d999a5aa8db75f99b43b8)) by [@vdusek](https://github.com/vdusek)
- Handle all supported configuration options ([#199](https://github.com/apify/crawlee-python/pull/199), closes [#84](https://github.com/apify/crawlee-python/issues/84)) ([23c901c](https://github.com/apify/crawlee-python/commit/23c901cd68cf14b4041ee03568622ee32822e94b)) by [@janbuchar](https://github.com/janbuchar)
- Add Playwright's enqueue links helper ([#196](https://github.com/apify/crawlee-python/pull/196)) ([849d73c](https://github.com/apify/crawlee-python/commit/849d73cc7d137171b98f9f2ab85374e8beec0dad)) by [@vdusek](https://github.com/vdusek)

### 🐛 Bug Fixes

- Tmp path in tests is working ([#164](https://github.com/apify/crawlee-python/pull/164), closes [#159](https://github.com/apify/crawlee-python/issues/159)) ([382b6f4](https://github.com/apify/crawlee-python/commit/382b6f48174bdac3931cc379eaf770ab06f826dc)) by [@vdusek](https://github.com/vdusek)
- Add explicit err msgs for missing pckg extras during import ([#165](https://github.com/apify/crawlee-python/pull/165), closes [#155](https://github.com/apify/crawlee-python/issues/155)) ([200ebfa](https://github.com/apify/crawlee-python/commit/200ebfa63d6e20e17c8ca29544ef7229ed0df308)) by [@vdusek](https://github.com/vdusek)
- Make timedelta_ms accept string-encoded numbers ([#190](https://github.com/apify/crawlee-python/pull/190)) ([d8426ff](https://github.com/apify/crawlee-python/commit/d8426ff41e36f701af459ad17552fee39637674d)) by [@janbuchar](https://github.com/janbuchar)
- *(deps)* Update dependency psutil to v6 ([#193](https://github.com/apify/crawlee-python/pull/193)) ([eb91f51](https://github.com/apify/crawlee-python/commit/eb91f51e19da406e3f9293e5336c1f85fc7885a4)) by [@renovate[bot]](https://github.com/renovate[bot])
- Improve compatibility between ProxyConfiguration and its SDK counterpart ([#201](https://github.com/apify/crawlee-python/pull/201)) ([1a76124](https://github.com/apify/crawlee-python/commit/1a76124080d561e0153a4dda0bdb0d9863c3aab6)) by [@janbuchar](https://github.com/janbuchar)
- Correct return type of storage get_info methods ([#200](https://github.com/apify/crawlee-python/pull/200)) ([332673c](https://github.com/apify/crawlee-python/commit/332673c4fb519b80846df7fb8cd8bb521538a8a4)) by [@janbuchar](https://github.com/janbuchar)
- Type error in statistics persist state ([#206](https://github.com/apify/crawlee-python/pull/206), closes [#194](https://github.com/apify/crawlee-python/issues/194)) ([96ceef6](https://github.com/apify/crawlee-python/commit/96ceef697769cd57bd1a50b6615cf1e70549bd2d)) by [@vdusek](https://github.com/vdusek)

## [0.0.4](https://github.com/apify/crawlee-python/releases/tag/v0.0.4) (2024-05-30)

### 🚀 Features

- Capture statistics about the crawler run ([#142](https://github.com/apify/crawlee-python/pull/142), closes [#97](https://github.com/apify/crawlee-python/issues/97)) ([eeebe9b](https://github.com/apify/crawlee-python/commit/eeebe9b1e24338d68a0a55228bbfc717f4d9d295)) by [@janbuchar](https://github.com/janbuchar)
- Proxy configuration ([#156](https://github.com/apify/crawlee-python/pull/156), closes [#136](https://github.com/apify/crawlee-python/issues/136)) ([5c3753a](https://github.com/apify/crawlee-python/commit/5c3753a5527b1d01f7260b9e4c566e43f956a5e8)) by [@janbuchar](https://github.com/janbuchar)
- Add first version of browser pool and playwright crawler ([#161](https://github.com/apify/crawlee-python/pull/161)) ([2d2a050](https://github.com/apify/crawlee-python/commit/2d2a0505b1c2b1529a8835163ca97d1ec2a6e44a)) by [@vdusek](https://github.com/vdusek)

## [0.0.3](https://github.com/apify/crawlee-python/releases/tag/v0.0.3) (2024-05-13)

### 🚀 Features

- AutoscaledPool implementation ([#55](https://github.com/apify/crawlee-python/pull/55), closes [#19](https://github.com/apify/crawlee-python/issues/19)) ([621ada2](https://github.com/apify/crawlee-python/commit/621ada2bd1ba4e2346fb948dc02686e2b37e3856)) by [@janbuchar](https://github.com/janbuchar)
- Add Snapshotter ([#20](https://github.com/apify/crawlee-python/pull/20)) ([492ee38](https://github.com/apify/crawlee-python/commit/492ee38c893b8f54e9583dd492576c5106e29881)) by [@vdusek](https://github.com/vdusek)
- Implement BasicCrawler ([#56](https://github.com/apify/crawlee-python/pull/56), closes [#30](https://github.com/apify/crawlee-python/issues/30)) ([6da971f](https://github.com/apify/crawlee-python/commit/6da971fcddbf8b6795346c88e295dada28e7b1d3)) by [@janbuchar](https://github.com/janbuchar)
- BeautifulSoupCrawler ([#107](https://github.com/apify/crawlee-python/pull/107), closes [#31](https://github.com/apify/crawlee-python/issues/31)) ([4974dfa](https://github.com/apify/crawlee-python/commit/4974dfa20c7911ee073438fd388e60ba4b2c07db)) by [@janbuchar](https://github.com/janbuchar)
- Add_requests and enqueue_links context helpers ([#120](https://github.com/apify/crawlee-python/pull/120), closes [#5](https://github.com/apify/crawlee-python/issues/5)) ([dc850a5](https://github.com/apify/crawlee-python/commit/dc850a5778b105ff09e19eaecbb0a12d94798a62)) by [@janbuchar](https://github.com/janbuchar)
- Use SessionPool in BasicCrawler ([#128](https://github.com/apify/crawlee-python/pull/128), closes [#110](https://github.com/apify/crawlee-python/issues/110)) ([9fc4648](https://github.com/apify/crawlee-python/commit/9fc464837e596b3b5a7cd818b6d617550e249352)) by [@janbuchar](https://github.com/janbuchar)
- Add base storage client and resource subclients ([#138](https://github.com/apify/crawlee-python/pull/138)) ([44d6597](https://github.com/apify/crawlee-python/commit/44d65974e4837576918069d7e63f8b804964971a)) by [@vdusek](https://github.com/vdusek)

### 🐛 Bug Fixes

- *(deps)* Update dependency docutils to ^0.21.0 ([#101](https://github.com/apify/crawlee-python/pull/101)) ([534b613](https://github.com/apify/crawlee-python/commit/534b613f7cdfe7adf38b548ee48537db3167d1ec)) by [@renovate[bot]](https://github.com/renovate[bot])
- *(deps)* Update dependency eval-type-backport to ^0.2.0 ([#124](https://github.com/apify/crawlee-python/pull/124)) ([c9e69a8](https://github.com/apify/crawlee-python/commit/c9e69a8534f4d82d9a6314947d76a86bcb744607)) by [@renovate[bot]](https://github.com/renovate[bot])
- Fire local SystemInfo events every second ([#144](https://github.com/apify/crawlee-python/pull/144)) ([f1359fa](https://github.com/apify/crawlee-python/commit/f1359fa7eea23f8153ad711287c073e45d498401)) by [@vdusek](https://github.com/vdusek)
- Storage manager & purging the defaults ([#150](https://github.com/apify/crawlee-python/pull/150)) ([851042f](https://github.com/apify/crawlee-python/commit/851042f25ad07e25651768e476f098ef0ed21914)) by [@vdusek](https://github.com/vdusek)

<!-- generated by git-cliff -->
