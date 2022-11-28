# Datetime format

Quickwit's DateTime is a wrapper around Tantivy's provided DateTime type which is internally represented as an `i64` microseconds value. For optimization reasons, Tantivy stores the value differently at the following locations:
- DocStore: Dates are stored as they are received from the input document.
- TermDict: Dates are stored with `seconds` precision.
- FastField: Dates are stored using the DateTime type configured precision that can take of the following values: `seconds`, `milliseconds`, `microseconds`.
