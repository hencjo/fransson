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
| `restore` | Beginning of an archive | Completion marker | `restore`, `run` | Recreating a known topic snapshot |
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

## Dump production data and restore it locally

Create `fransson.yaml` with the source credentials, local destination, and restore target:

```yaml
state_file: .state/fransson.json

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
state_file: .state/fransson.json

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

  processes.fransson.exec = "fransson run --config ./fransson.yaml";
}
```

Start it with:

```bash
devenv up
```

Keep `fransson.yaml`, its state file, and archives in the project working directory. Do not interpolate the YAML path as a Nix path: that copies it into the read-only Nix store and breaks writable relative paths.

## Configuration reference

All commands use the same strict YAML schema. Unknown fields, obsolete names, conflicting modes, and invalid combinations fail loudly. See [`config.example.yaml`](examples/config.example.yaml) for the complete authenticated example and [`config.no-auth-dst.example.yaml`](examples/config.no-auth-dst.example.yaml) for a destination without authentication.

```yaml
state_file: .state/fransson.json

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
- `properties` passes advanced librdkafka settings through. It cannot override typed fields or Fransson's reliability settings.
- `max_in_flight_per_partition` defaults to `64`.
- Destination brokers are contacted only by `restore` and `run`; `dump` opens only its selected source.

### Topics

- Every topic configures exactly one of `manage`, `empty`, `clone`, `stream`, or `restore`.
- `manage.partitions` and `empty.partitions` are required. Clone and stream partition counts come from their source; restore partition counts come from the archive.
- `manage` creates a missing topic and reconciles its shape, but preserves a matching existing topic and its records.
- `empty` creates a missing topic or recreates an existing one, guaranteeing a fresh empty topic immediately after reconciliation.
- `replication_factor` is optional. Without it, new topics use the broker default and existing replication is not managed.
- `config` owns the listed Kafka topic properties. Fransson also owns `message.timestamp.type=CreateTime` so copied timestamps survive.
- Relative `state_file` and archive paths resolve from the YAML file's directory.
- A source topic can be mapped only once in a configuration.

## Reconciliation and force

Missing destination topics are created normally. An existing topic is drifted when its partition count, explicitly managed replication factor, configured Kafka properties, archive completion marker, or saved clone source differs from the configuration. Existing `empty` topics always require recreation, even when their shape matches.

Fransson preflights every destination before changing anything. Required recreation fails safely unless the topic has `force: true` or the invocation uses `--force`. Authorization deletes and recreates the destination topic, clears its state, and reapplies its configured data mode. **This destroys existing destination data.**

An `empty` topic is reset once during every `restore` invocation and every `run` startup; Fransson does not keep it empty after applications begin writing. A configuration containing only `manage` or `empty` topics must use `restore`, because `run` requires at least one active clone or stream.

Application writes after a completed restore do not count as drift and do not trigger another restore. If a restore fails partway through, its completion marker is absent; the next reconciliation requires force before recreating the partial destination and trying again.

## State, archives, and delivery

- `state_file` is required by `restore` and `run`; `dump` can use a source-only configuration.
- Clone state records the next offset only after destination acknowledgement and is persisted atomically.
- Restore state records the archive SHA-256 and format version only after every record is acknowledged.
- Archives omit topic names and physical Kafka offsets, and are written atomically after all startup high-watermarks have been consumed.
- Active streams use an idempotent producer and wait through retriable destination outages.
- Fransson reads only its current state and archive formats; there are no compatibility aliases or migrations for pre-`0.1.0` files.

## Command reference

```text
fransson dump --config FILE --source SOURCE:TOPIC --archive FILE [--force]
fransson restore --config FILE [--force]
fransson run --config FILE [--force]
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
