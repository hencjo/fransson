# Upgrading Fransson state handling

This guide is written for an LLM or automation agent upgrading a deployment from the old YAML-configured state file to the identity-bound local state registry.

## Breaking changes

- The top-level YAML field `state_file` was removed and is rejected.
- `restore` and `run` now use `./.fransson/state.json` by default.
- `--state-dir DIR` selects another persistent directory. It names a directory, not a JSON file.
- Old state files cannot be migrated. New state records include Kafka cluster IDs and immutable topic UUIDs.
- Fransson now requires Kafka 2.8 or newer.
- Existing clone or restore destinations without matching new state are untrusted. Rebuilding them requires `--force` and destroys their current data.

`dump` remains stateless and must not receive `--state-dir`.

## Mechanical upgrade procedure

### 1. Find affected files

From the deployment repository, search for old YAML and command invocations:

```bash
rg -n '^\s*state_file:' --glob '*.yaml' --glob '*.yml'
rg -n 'fransson (restore|run|dump)'
```

Do not print configuration values or secrets into an LLM conversation.

### 2. Remove `state_file` from YAML

Old:

```yaml
state_file: .state/fransson.json

sources:
  production:
    bootstrap_servers: production-kafka:9092
    group_id: fransson-production
```

New:

```yaml
sources:
  production:
    bootstrap_servers: production-kafka:9092
    group_id: fransson-production
```

Do not replace `state_file` with another YAML field. Archive paths remain relative to the YAML file; state paths do not.

### 3. Choose persistent state storage

For interactive use from a stable project directory, the default is sufficient:

```bash
fransson restore --config fransson.yaml
```

For Nix, systemd, containers, CI, or any command whose working directory may change, provision a persistent writable directory and pass it explicitly:

```bash
fransson run \
  --config /etc/fransson/fransson.yaml \
  --state-dir /var/lib/fransson
```

The resulting registry is `/var/lib/fransson/state.json`. Persist the entire directory because it also contains lock files. Do not place it in the read-only Nix store, a temporary container filesystem, or storage shared concurrently across hosts.

### 4. Update every `restore` and `run` invocation

Add the same `--state-dir` to all commands belonging to one deployment. Update service definitions, scripts, Nix expressions, container arguments, and documentation together.

Do not add the option to `dump`:

```bash
fransson dump \
  --config fransson.yaml \
  --source production:products \
  --archive products.fransson.zst
```

### 5. Plan the one-time rebuild

Do not copy offsets or restore markers from the old JSON file. They lack the cluster and topic identities required by the new schema.

For existing clone or restore destination topics, the first new invocation fails closed and asks for force. Obtain explicit operator approval before running:

```bash
fransson restore \
  --config fransson.yaml \
  --state-dir /var/lib/fransson \
  --force
```

`--force` authorizes destination topic deletion and recreation. Stop applications using those topics before proceeding. A missing destination topic can be created without force.

### 6. Verify the result

Inspect the new registry:

```bash
fransson state show --state-dir /var/lib/fransson
```

Confirm that entries contain the expected destination cluster and topic names, topic UUIDs, and either clone offsets or a completed restore marker. Never edit `state.json` manually.

## Resetting state later

Reset one configured destination:

```bash
fransson state reset \
  --config fransson.yaml \
  --state-dir /var/lib/fransson \
  --topic products
```

Reset every destination declared by a configuration:

```bash
fransson state reset \
  --config fransson.yaml \
  --state-dir /var/lib/fransson \
  --all
```

Resetting state does not modify Kafka. The next `restore` or `run` treats an existing clone or restore topic as untrusted and requires `--force` before rebuilding it. Delete the whole `.fransson` directory only while Fransson is stopped.

## LLM completion checklist

- No active YAML contains `state_file`.
- Every service deployment provisions a persistent writable state directory.
- Every `restore` and `run` invocation uses the intended directory consistently.
- No `dump` invocation includes `--state-dir`.
- Kafka is version 2.8 or newer and the principal can describe managed topics.
- The operator approved any required `--force` rebuild.
- `fransson state show` reports the expected identity-bound state after the first successful run.
