---
id: upgrading-to-v03
title: Upgrading to v0.3
---

This page summarizes most of the breaking changes between Crawlee for Python v0.2.x and v0.3.0.

## Public and private interface declaration

In previous versions, the majority of the package was fully public, including many elements intended for internal use only. With the release of v0.3, we have clearly defined the public and private interface of the package. As a result, some imports have been updated (see below). If you are importing something now designated as private, we recommend reconsidering its use or discussing your use case with us in the discussions/issues.

Here is a list of the updated public imports:

```diff
- from crawlee.enqueue_strategy import EnqueueStrategy
+ from crawlee import EnqueueStrategy
```

```diff
- from crawlee.models import Request
+ from crawlee import Request
```

```diff
- from crawlee.basic_crawler import Router
+ from crawlee.router import Router
```

## Request queue

<!-- TODO -->
