# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Query validation against index schema before dispatch to leaf nodes (#1109, @linxGnu)
- Support for custom S3 endpoint (#1108)

### Fixed
- Minor bug in leaf search stream (#1110)

### Removed
- QW_ENV environment variable

### Security
- Compiled binaries with Rust 1.58.1, which fixes CVE-2022-21658

## [0.2.0] - 2022-01-12

## [0.1.0] - 2021-07-13
