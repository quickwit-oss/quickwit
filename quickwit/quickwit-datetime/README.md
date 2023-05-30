Why a datetime crate? Why is it no in quickwit-common or where it is consumed?

- We don't want to add a dependency to tantivy in quickwit-common
- We need this date logic both in quickwit-query and in quickwit-docmapper
