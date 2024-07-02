---
id: crawl-website-with-relative-links
title: Crawl website with relative links
---

When crawling a website, you may encounter different types of links present that you may want to crawl. To facilitate the easy crawling of such links, we provide the `enqueue_links()` method on the crawler context, which will automatically find links and add them to the crawler's `RequestQueue`.
