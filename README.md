# Fransson

Fransson gives developers repeatable Kafka data in local and test environments: archive selected production topics, restore them deterministically, or forward only new events between clusters.

```text
production Kafka ── read ──▶ Fransson ── write ──▶ local/test Kafka
                                  │
                                  └──────────────▶ deterministic archive
```

Fransson is deliberately one-way: source connections are read-only and every write targets the configured destination. Selection is topic-level; within the offsets covered by a mode, Fransson does not filter records. It preserves partition, key, payload, timestamp, and headers, and never transforms or repartitions data.

## Choose a mode

| Mode | Starts at | Persists progress | Used by | Best for |
| --- | --- | --- | --- | --- |
| `clone` | Earliest offset, or the last acknowledged offset in state | Yes | `restore`, `run` | Reproducing compacted/reference topics |
| `stream` | Source end after each startup reconciliation | No | `run` | Forwarding only events produced while Fransson is running |
| `restore` | Beginning of an archive | Application marker | `restore`, `run` | Recreating a known topic snapshot |
| `manage` | No data transfer | Topic reconciliation only | `restore`, `run` | Managing shape while preserving matching topics |
| `empty` | No data transfer | No | `restore`, `run` | Starting every invocation with a new empty topic |

`restore` is the one-shot command: it reconciles destinations, applies incomplete archives, and advances clones through their startup high-watermarks. `run` does the same reconciliation, then keeps clones and streams running. Streams are inactive during `restore`.

## Run with Nix

No installation is required:

```bash
nix run github:hencjo/fransson -- --help
```

Or build the named package:

```bash
nix build github:hencjo/fransson#fransson
./result/bin/fransson --help
```

The examples below use `fransson` for readability; replace it with `nix run github:hencjo/fransson --` if it is not on your `PATH`.

### GNU/Linux release archive

GitHub releases also provide an `x86_64` GNU/Linux archive and SHA-256 checksum:

```text
fransson-<version>-linux-x86_64-gnu.tar.gz
fransson-<version>-linux-x86_64-gnu.tar.gz.sha256
```

The binary targets Ubuntu 22.04 or newer and dynamically links Cyrus SASL. On Ubuntu or Debian, install its runtime dependencies with:

```bash
sudo apt-get install libsasl2-2 libsasl2-modules zlib1g
```

GSSAPI/Kerberos users also need their distro's Cyrus SASL GSSAPI module, such as `libsasl2-modules-gssapi-mit` on Ubuntu.

Use Nix when you want Fransson and all runtime dependencies managed together.

## Dump production data and restore it locally

Create `fransson.yaml` with the source credentials, local destination, and restore target:

```yaml
sources:
  production:
    bootstrap_servers: production-kafka.example.com:9093
    group_id: fransson-local-seed
    security_protocol: SASL_SSL
    sasl:
      mechanism: PLAIN
      username: developer
      password_env: PRODUCTION_KAFKA_PASSWORD

destination:
  bootstrap_servers: localhost:9092

topics:
  - name: products
    config:
      cleanup.policy: compact
    restore:
      archive: products.fransson.zst
```

Dump `production:products` through the high-watermarks observed when the command starts:

```bash
fransson dump \
  --config fransson.yaml \
  --source production:products \
  --archive products.fransson.zst
```

Then restore it into local Kafka:

```bash
fransson restore --config fransson.yaml
```

The archive contains no source topic name, so its destination name comes from the YAML. Equivalent partition contents produce byte-identical archives. `--force` on `dump` replaces an existing archive.

## Stream new production events into test

Configure a stream mapping:

```yaml
sources:
  production:
    bootstrap_servers: production-kafka.example.com:9093
    group_id: fransson-production-to-test
    security_protocol: SASL_SSL
    sasl:
      mechanism: PLAIN
      username: developer
      password_env: PRODUCTION_KAFKA_PASSWORD

destination:
  bootstrap_servers: test-kafka.example.com:9092

topics:
  - name: checkout.events
    config:
      cleanup.policy: delete
      retention.ms: "604800000"
    stream:
      source: production:checkout.events
```

Start forwarding:

```bash
fransson run --config fransson.yaml
```

Each run starts at the source end after destination reconciliation. Existing records and records produced while Fransson is stopped are intentionally skipped.

## Run Fransson as a Devenv process

Add Fransson as a locked [Devenv input](https://devenv.sh/inputs/):

```yaml
# devenv.yaml
inputs:
  fransson:
    url: github:hencjo/fransson
    inputs:
      nixpkgs:
        follows: nixpkgs
```

Expose the package and define a [process](https://devenv.sh/processes/):

```nix
# devenv.nix
{ inputs, pkgs, ... }:

let
  fransson = inputs.fransson.packages.${pkgs.stdenv.system}.fransson;
in
{
  packages = [ fransson ];

  processes.fransson.exec = "fransson run --config ./fransson.yaml --state-dir ./.fransson";
}
```

Start it with:

```bash
devenv up
```

Keep `fransson.yaml`, `.fransson/`, and archives on persistent writable storage. For services and containers, pass `--state-dir` explicitly and provision that directory. Do not interpolate YAML containing relative archive paths as a Nix path: that copies it into the read-only Nix store.

## Configuration reference

All commands use the same strict YAML schema. Unknown fields, obsolete names, conflicting modes, and invalid combinations fail loudly. See [`config.example.yaml`](examples/config.example.yaml) for the complete authenticated example and [`config.no-auth-dst.example.yaml`](examples/config.no-auth-dst.example.yaml) for a destination without authentication.

```yaml
sources:
  primary:
    bootstrap_servers: source:9093
    client_id: fransson-primary
    group_id: fransson-primary
    max_in_flight_per_partition: 64
    security_protocol: SASL_SSL
    sasl:
      mechanism: PLAIN
      username: source-user
      password_env: PRIMARY_KAFKA_PASSWORD
    properties:
      fetch.wait.max.ms: "50"

destination:
  bootstrap_servers: destination:9092
  client_id: fransson-destination
  properties: {}

topics:
  - name: orders.backup
    replication_factor: 3
    config:
      cleanup.policy: compact
      retention.ms: "-1"
    clone:
      source: primary:orders

  - name: payments.live
    config:
      cleanup.policy: delete
    stream:
      source: primary:payments

  - name: orders.restored
    force: true
    config:
      cleanup.policy: compact
    restore:
      archive: orders.fransson.zst

  - name: scratch.events
    manage:
      partitions: 3

  - name: disposable.events
    force: true
    empty:
      partitions: 3
```

### Connections

- Every source needs `bootstrap_servers` and an explicit `group_id`. Fransson manually assigns partitions and never commits consumer offsets, but Kafka still checks group authorization.
- `client_id`, `security_protocol`, and `sasl` are optional. `password_env` names the environment variable holding the SASL password.
- `properties` passes advanced librdkafka settings through. It cannot override typed fields or Fransson's reliability settings, including `auto.offset.reset=error` for clone checkpoints.
- `max_in_flight_per_partition` defaults to `64`.
- Destination brokers are contacted only by `restore` and `run`; `dump` opens only its selected source.
- The destination principal must be able to enumerate consumer groups, inspect their committed offsets, and delete offsets for managed topics. Fransson needs cluster-wide group visibility so it cannot silently miss a group.

### Topics

- Every topic configures exactly one of `manage`, `empty`, `clone`, `stream`, or `restore`.
- `manage.partitions` and `empty.partitions` are required. Clone and stream partition counts come from their source; restore partition counts come from the archive.
- `manage` creates a missing topic and reconciles its shape, but preserves a matching existing topic and its records.
- `empty` creates a missing topic or recreates an existing one, guaranteeing a fresh empty topic immediately after reconciliation.
- `replication_factor` is optional. Without it, new topics use the broker default and existing replication is not managed.
- `config` owns the listed Kafka topic properties. Fransson also owns `message.timestamp.type=CreateTime` so copied timestamps survive.
- Relative archive paths resolve from the YAML file's directory.
- A source topic can be mapped only once in a configuration.

## Reconciliation and force

Missing destination topics are created normally. An existing topic is drifted when its partition count, explicitly managed replication factor, configured Kafka properties, archive application marker, or saved clone source differs from the configuration. Existing `empty` topics always require recreation, even when their shape matches.

Fransson preflights every destination before changing anything. Required recreation fails safely unless the topic has `force: true` or the invocation uses `--force`. Authorization deletes and recreates the destination topic, clears its state, and reapplies its configured data mode. **This destroys existing destination data.**

Whenever Fransson creates a fresh topic identity—whether the topic was missing or is being recreated—it deletes every discovered consumer group's committed offsets for that topic only. Offsets for unrelated topics remain untouched. An active group subscribed to the topic or missing group permissions fails reconciliation; consumer offsets are never reset optionally or skipped silently.

Destructive reconciliation requires exclusive ownership of the destination topic. Stop producers, consumers, and Kafka Streams applications that can reference it, or disable broker-side topic auto-creation first. If another client recreates the topic between Fransson's delete and create operations, Fransson fails rather than accepting a topic whose identity and contents it does not control.

An `empty` topic is reset once during every `restore` invocation and every `run` startup; Fransson does not keep it empty after applications begin writing. A configuration containing only `manage` or `empty` topics must use `restore`, because `run` requires at least one active clone or stream.

Application writes after an applied restore do not count as drift and do not trigger another restore. If a restore fails partway through, its applied marker is absent; the next reconciliation requires force before recreating the partial destination and trying again.

Clone checkpoints must remain between the source partition's current low and high watermarks. If source retention removes an unprocessed checkpoint, Fransson fails closed and requires force to rebuild the destination from the source records that remain; it never jumps silently to the source end.

## State, archives, and delivery

- `restore` and `run` use `.fransson/state.json` in the current directory by default. `--state-dir DIR` selects persistent storage explicitly; `dump` never reads or writes state.
- The file is a local work ledger, not a snapshot of declared Kafka contents. `state show` does not contact Kafka, and its entries may be stale until the next reconciliation.
- State is grouped by Kafka cluster ID and destination topic name, then fenced by immutable topic UUIDs. Clone state also records the source cluster and topic UUIDs. Fransson requires Kafka 2.8 or newer.
- A missing or mismatched state entry for an existing clone or restore topic fails closed and requires `--force`; state from another cluster can never be applied silently.
- `fransson state show` prints the registry. `fransson state reset --config FILE --topic TOPIC` resets one configured destination; `--all` resets every destination in that configuration. Resetting state never changes Kafka data.
- Clone state records the next offset only after destination acknowledgement. Before each atomic persistence, Fransson revalidates the live source and destination cluster/topic identities and stops rather than checkpointing work against a replacement topic.
- Restore state records `applying` before copying and `applied` with the archive SHA-256 and format version only after every record is acknowledged and the destination identity is revalidated. `applied` does not claim that later application writes, retention, or compaction left the topic equal to the archive.
- Fransson fingerprints the exact archive bytes consumed. Replacing or modifying an archive during reconciliation fails the restore and leaves it `applying` rather than recording the wrong archive as applied.
- Archives omit topic names and physical Kafka offsets, and are written atomically after all startup high-watermarks have been consumed.
- A dump fails rather than jumping past a captured source offset that expires during collection.
- Active streams use an idempotent producer and wait through retriable destination outages.
- Fransson reads only its current state and archive formats; there are no compatibility aliases or migrations for pre-`0.1.0` files.

## Command reference

```text
fransson dump --config FILE --source SOURCE:TOPIC --archive FILE [--force]
fransson restore --config FILE [--state-dir DIR] [--force]
fransson run --config FILE [--state-dir DIR] [--force]
fransson state show [--state-dir DIR]
fransson state reset --config FILE (--topic TOPIC | --all) [--state-dir DIR]
```

- `dump` creates one deterministic compressed archive and never connects to the destination.
- `restore` performs one bounded reconciliation, restore, and clone pass; streams remain inactive.
- `run` reconciles first, then continuously clones and forwards stream records until stopped.
- `restore --force` and `run --force` authorize every required destination topic recreation for that invocation.

Use `fransson <command> --help` for the complete option reference.

## Compatibility

Fransson follows [Semantic Versioning](https://semver.org/) from `0.1.0` onward. The CLI, YAML schema, state format, and archive format are public interfaces; incompatible changes require a breaking release.

## Development

Enter the development shell and run the checks:

```bash
nix develop
cargo test
nix flake check
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for commit conventions and the release process. Releases are generated through release-plz; do not manually bump the Cargo version or changelog.
