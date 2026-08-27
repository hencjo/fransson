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

Keep `README.md`, this guide, and the files under `examples/` accurate when changing public behavior.

## Commits and versions

Use Conventional Commit summaries so release-plz can choose the version and write a useful changelog:

```text
fix: handle missing restore state
feat: add topic reset mode
docs: clarify archive behavior
ci: publish GNU/Linux release artifact
feat!: change the YAML topic schema
```

Do not manually bump `Cargo.toml` or edit generated release entries in `CHANGELOG.md`. Breaking changes use `!` or a `BREAKING CHANGE:` footer.

## Releases

`Cargo.toml` is the version source of truth; `flake.nix` reads it directly. Fransson is not published to crates.io.

Pushing normal work to `master` opens or updates a release PR. A release is published only after that PR is merged:

1. Review the version and changelog in the release PR.
2. Merge the release PR.
3. release-plz creates `fransson-v<version>` and the matching GitHub release.
4. The artifact workflow attaches the GNU/Linux tarball and checksum.

To rebuild artifacts for an existing release, manually run **Release artifacts** with its tag.

### One-time GitHub setup

Under **Settings → Actions → General**:

1. Set workflow permissions to **Read and write permissions**.
2. Enable **Allow GitHub Actions to create and approve pull requests**.

No crates.io token is required.
