# Quickwit Claude Guidelines

## Code Formatting

Run `make fmt` to check and fix code formatting. This command performs three checks:

1. **Rust formatting**: Ensures Rust code is properly formatted (via `cargo fmt`)
2. **License headers**: Checks that files are prepended with the correct LICENSE header
3. **Log format policy**: Checks that log statements follow our format rules:
   - No trailing punctuation in log messages
   - No uppercase for the first character of log messages
   - See `scripts/check_log_format.sh` for details

### Quick Fix

Use `/fmt` to automatically run format checks and see issues.
