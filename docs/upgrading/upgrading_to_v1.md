---
id: upgrading-to-v1
title: Upgrading to v1
---

This page summarizes the breaking changes between Crawlee for Python v0.6 and v1.0.


## Separate use of word `browser` in similar contexts

Two different contexts:
- Playwright related browser
- fingerprinting related browser

Type of `HeaderGeneratorOptions.browsers` changed from `Literal['chromium', 'firefox', 'webkit', 'edge']` to `Literal['chrome', 'firefox', 'safari', 'edge']` as it is related to the fingerprinting context and not to the Playwright context.

Before:

```python
from crawlee.fingerprint_suite import HeaderGeneratorOptions

HeaderGeneratorOptions(browsers=['chromium'])
HeaderGeneratorOptions(browsers=['webkit'])
```

Now:

```python
from crawlee.fingerprint_suite import HeaderGeneratorOptions

HeaderGeneratorOptions(browsers=['chrome'])
HeaderGeneratorOptions(browsers=['safari'])
```

