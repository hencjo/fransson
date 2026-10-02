# Changelog

All notable changes to Fransson will be documented in this file.

This changelog is written for users. Release versions are inferred from the
populated Unreleased categories, not commit summaries.

## [Unreleased]

### Breaking changes

### Added

### Changed

### Fixed

### Security

### Documentation

### Upgrade notes

## [0.2.0] - 2026-10-02

### Breaking changes

- Config string values now support `${NAME}` interpolation from Fransson's inherited environment, with caller-chosen variable names. **Upgrade:** replace every source and destination `sasl.password_env: NAME` with `sasl.password: "${NAME}"`; `password_env` is no longer accepted. Export all referenced variables before running Fransson, including those in unused connections. Missing or non-Unicode variables fail config loading; SASL passwords must be nonblank.
- Existing literal dollar signs must be escaped as `$$` when they would otherwise form `${...}` or `$$`; for example, use `$${NAME}` for literal `${NAME}`. Bare `$NAME` stays unchanged. Expansion is single-pass and applies only to string values, not mapping keys or numeric/boolean fields; shell commands, default expressions, and automatic `.env` loading are not supported.

### Changed

Releases are now cut locally with `scripts/release --execute`, rather than through
release-plz PRs. The script infers the next version from this changelog, stamps
the release notes, and pushes a release commit and tag. GitHub builds the existing
GNU/Linux archive and checksum, then publishes them with these release notes.
Consumers can keep using the same `fransson-v<version>` tags and archive names;
maintainers should follow the new release and recovery instructions in
`CONTRIBUTING.md`.
