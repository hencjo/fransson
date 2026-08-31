# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/hencjo/fransson/releases/tag/fransson-v0.1.0) - 2026-08-31

### Added

- [**breaking**] bind state to Kafka topic identity

### Fixed

- isolate source status failures
- stop transfers on consumer failures
- verify bounded consumers reach captured offsets
- fail dump when captured offsets expire
- [**breaking**] reset consumer offsets for fresh destination topics
- fence persisted progress with live topic identities
- fingerprint the archive bytes actually restored
- [**breaking**] reject unusable clone checkpoints
- [**breaking**] make restore state describe archive application
- detect concurrent topic recreation

### Other

- .codex -> .gitignore
- Package .deb, dynamic linking and release-plz.
- Fixed build.
- Initial commit.
# Changelog

All notable changes to Fransson will be documented in this file.

This changelog is maintained by release-plz from Conventional Commit summaries.
