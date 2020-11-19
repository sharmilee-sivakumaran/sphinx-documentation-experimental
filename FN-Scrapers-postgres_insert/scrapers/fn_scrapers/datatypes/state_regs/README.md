# State Regs Scrapers

## Migration Checklist

 - [ ] Check for calls to `self._scraper`
 - [ ] Verify `self.register_and_extract` uses `files.extractors`, not `extraction_type`
 - [ ] Replace `([\w_.]+).xpath_single(` with `xp_first($1, `.
 - [ ] Replace `.get_attrib\(([^)]+)` with `.get($1)`.
 - [ ] Remove `NoticeReportingPolicy`
 - [ ] Add `@scraper` and `@tags` to class
 - [ ] Refactor `scrape()` to `do_scrape()`
 - [ ] Remove `.element` Because we aren't doing elementwrappers anymore