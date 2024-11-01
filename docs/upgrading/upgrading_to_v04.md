---
id: upgrading-to-v04
title: Upgrading to v0.4
---

This page summarizes most of the breaking changes between Crawlee for Python v0.3.x and v0.4.0.

### Request model

- The `Request.query_params` field has been removed. Please add query parameters directly to the URL, which was possible before as well, and is now the only supported approach.
- The `Request.payload` and `Request.data` fields have been consolidated. Now, only `Request.payload` remains, and it should be used for all payload data in requests.

### Extended unique key computation

- The computation of `extended_unique_key` now includes HTTP headers. While this change impacts the behavior, the interface remains the same.
