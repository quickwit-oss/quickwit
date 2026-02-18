---
name: fmt
description: Run `make fmt` to check the code format.
---

# Format Check

Run `make fmt` from the `quickwit/` subdirectory to check code formatting:

```
cd /Users/paul.masurel/git/quickwit/quickwit && make fmt
```

This command checks:
1. Rust code formatting
2. License headers
3. Log format policy (no trailing punctuation, no uppercase first character)

If there are log format issues, fix them by:
- Making the first character lowercase
- Removing trailing punctuation (periods, exclamation marks, etc.)

Fix any issues found and re-run until clean.
