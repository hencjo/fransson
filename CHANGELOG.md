# Changelog

All notable changes to Fransson will be documented in this file.

This changelog is written for users. Release versions are inferred from the
populated Unreleased categories, not commit summaries.

## [Unreleased]

### Changed

Releases are now cut locally with `scripts/release --execute`, rather than through
release-plz PRs. The script infers the next version from this changelog, stamps
the release notes, and pushes a release commit and tag. GitHub builds the existing
GNU/Linux archive and checksum, then publishes them with these release notes.
Consumers can keep using the same `fransson-v<version>` tags and archive names;
maintainers should follow the new release and recovery instructions in
`CONTRIBUTING.md`.
