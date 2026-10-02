# Contributing

## Development

Enter the development shell and run the checks before submitting changes:

```bash
nix develop
cargo fmt -- --check
cargo test
cargo clippy --all-targets -- -D warnings
nix flake check
nix eval --raw .#packages.x86_64-linux.fransson.version
git diff --check
```

The Nix version must match `Cargo.toml`.

The ignored reconciliation tests require a disposable Kafka 4.2.0 broker, exercise topic UUID fencing and consumer-offset reset behavior, and delete the uniquely named topics they create:

```bash
FRANSSON_TEST_KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  cargo test kafka_reconciliation_ -- --ignored --nocapture
```

The ignored librdkafka mock-cluster tests cover deterministic dump/restore, bounded emptiness probing, stream startup, and outage recovery. They require permission to open local sockets:

```bash
for test in \
  kafka_dump_restore_dump_is_byte_identical \
  kafka_empty_probe_counts_tombstones_and_zero_length_records \
  kafka_stream_assignment_skips_existing_records \
  kafka_stream_producer_waits_through_destination_outage
do
  cargo test "$test" -- --ignored --nocapture
done
```

Keep `README.md`, this guide, and the files under `examples/` accurate when changing public behavior.

## Config interpolation tests

Keep environment expansion tests deterministic by injecting a lookup into
`config_env::decode`, rather than mutating the process environment. Cover literal
dollars, missing variables, single-pass expansion, YAML injection resistance,
string-only typing, and secret-safe errors. Published examples must pass through
the same decoding pipeline using fixture environment values. SASL uses `password`,
not the removed `password_env` field; never include resolved passwords in diagnostics.

## Commits and changelog

Use Conventional Commit summaries (`fix:`, `feat:`, `docs:`, `ci:`, and `feat!:`).
Commit messages do not determine release versions or generate release notes.
Write user-facing changes directly under `## [Unreleased]` in `CHANGELOG.md`.
Explain what changed, who is affected, and how to upgrade; include before/after
configuration or CLI examples when useful. Keep the published example configs
under `examples/` accurate as part of the same change.

Use these exact level-three category headings; empty headings and HTML comments
are placeholders and do not affect the version:

| Category | Automatic bump |
| --- | --- |
| Breaking changes | Minor on 0.x; major on 1.x and later |
| Added, Changed | Minor |
| Fixed, Security, Documentation | Patch |
| Upgrade notes | None; accompanies another populated category |

The highest populated category wins. Put incompatible changes under **Breaking
changes**, not merely Changed. Nested headings (level four or deeper), paragraphs,
lists, and fenced code examples are supported. Unknown or duplicate categories,
unclosed fences/comments, and empty releases are rejected. A documentation-only
release bumps patch. Use `--version 1.0.0` deliberately when ready for 1.0;
explicit versions must be at least the inferred version. Prereleases are not
supported by this workflow.

Release stamping replaces `{{version}}` and `{{date}}` in the current Unreleased
body (including code examples), dates the release heading in UTC, and inserts a
fresh empty Unreleased skeleton. Do not use these exact placeholders when you
intend literal text. Existing historical sections are left untouched.

## Releases

`Cargo.toml` is the build version source of truth; `flake.nix` reads it directly.
Do not manually bump Cargo versions or rewrite published changelog entries.
Fransson is not published to crates.io. Release artifacts remain the GNU/Linux
archive and SHA-256 checksum, including README, LICENSE, and `examples/`.

Enter `nix develop` for Bash, awk, Rust, and cargo-release, then:

```bash
./scripts/tests/release.sh
./scripts/release                    # read-only preview; also works on dirty work
./scripts/release --version 1.0.0     # optional deliberate version override
```

Run the development checks above, review the changelog, and commit and push normal
work first. Execution requires a completely clean `master` tracking
`origin/master`, with no local-only or remote-only commits:

```bash
./scripts/release --execute
# Or, when intentionally graduating to 1.0:
./scripts/release --execute --version 1.0.0
```

The script fetches origin, validates the state and version, and asks cargo-release
to update Cargo.toml/Cargo.lock, stamp the changelog through a Bash hook, commit
`chore: release <version>`, and create `fransson-v<version>`. It atomically pushes
the commit and tag to origin. Repository configuration is isolated from personal
cargo-release settings. Never invoke the internal `--stamp` hook manually.

The tag triggers **Release artifacts** on GitHub. It validates the tag, manifest,
and changelog, builds and smoke-tests the archive, uploads assets to a draft, and
publishes the release using that version's changelog section. A failed build does
not publish a new release. No release PR, crates.io token, or PAT is needed;
the workflow requests `contents: write` for its GitHub token. Your local Git
credentials must be allowed to push `master` and release tags; branch protection
must permit this maintainer-driven flow.

The wrapper uses cargo-release's explicit version, hook, commit, tag, and push
steps, avoiding the all-in-one command's unnecessary crates.io ownership lookup.
Cargo may still need network access for normal dependency metadata resolution.

### Recovery and rebuilding

The script never resets your work or moves tags after a failure. Inspect
`git status`, `git log -3`, `git tag --list 'fransson-v*'`, and
`git ls-remote --tags origin` before retrying.

- **Before a release commit/tag exists:** inspect any partially stamped files.
  Restore only the release-generated edits after review, fix the cause, and run
  the script again. Execution began from a clean tree, but do not discard any work
  you have done since the failure.
- **Release commit and tag exist locally, but the push failed:** do not run the
  release script again (Unreleased is now empty). Verify the tag points to the
  release commit and run `./scripts/release --verify-tag fransson-v<VERSION>`.
  Retry `git push --atomic origin HEAD:refs/heads/master refs/tags/fransson-v<VERSION>`.
  Never force-push; if master has advanced, resolve the situation explicitly.
- **Release commit exists but tag creation failed:** fix the cause, check the
  manifest and `./scripts/release --notes <VERSION>`, then retry only the tag step:
  `cargo release tag --isolated --config release.toml --execute`.
  Validate and push the resulting tag as described above; do not bump again.
- **Tag already reached GitHub:** never move or recreate it. Rerun the failed
  workflow or manually run **Release artifacts**, supplying the existing tag.
  Reruns reuse the release and replace its same-named assets; partial drafts are
  completed. Historical tags predating this tooling need their original workflow.

### Release tooling tests

`./scripts/tests/release.sh` runs parser and shell tests using temporary Git
repositories and a fake cargo-release command. It never pushes to the real
repository. Set `RELEASE_TEST_REAL=1` to additionally exercise the installed
cargo-release against a dependency-free Rust fixture and a temporary bare remote:

```bash
RELEASE_TEST_REAL=1 ./scripts/tests/release.sh
```
