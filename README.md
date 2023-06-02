# search-engine-rs
A basic web scraper and in the future search engine

It is very primitive and misses some important features such as duplicate link checking.
It is NOT meant for production, and most likely get stuck in an infinite cycle.

Has very primitive support for robots.txt (checks against ALL user-agent fields, not * and ours)

Runs N concurrent requests at the moment (see `NUM_THREADS`)

Seeded with `engine.start_crawl`:

```rust
engine.start_crawl(vec![
  "https://google.com",
  "https://jackz.me",
  "https://microsoft.com",
  "https://github.com"
]);
```
