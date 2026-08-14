use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use fs2::FileExt;
use futures_util::stream::{FuturesOrdered, StreamExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    stream_consumer::StreamPartitionQueue, Consumer, DefaultConsumerContext, StreamConsumer,
};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::{Header, Headers, Message, OwnedHeaders, OwnedMessage};
use rdkafka::metadata::{Metadata, MetadataTopic};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

mod archive;
mod kafka_identity;

use archive::{ArchiveEvent, ArchiveHeader, ArchiveReader, ArchiveRecord, ArchiveWriter};

const SOURCE_REF_DELIMITER: &str = ":";
const TOPIC_RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(30);
const TOPIC_RECONCILIATION_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Reconcile, clone, stream, dump, and restore Kafka topics without changing record identity"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create a deterministic archive through a topic's startup high-watermarks
    Dump(DumpArgs),
    /// Reconcile topics, apply archives, and clone through startup high-watermarks
    Restore(ConfigArgs),
    /// Reconcile topics, continuously clone, and stream new events
    Run(ConfigArgs),
    /// Inspect or reset Fransson's local state registry
    State(StateArgs),
}

#[derive(Debug, Args)]
struct ConfigArgs {
    /// YAML configuration file
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,
    /// Persistent state directory
    #[arg(long, value_name = "DIR", default_value = ".fransson")]
    state_dir: PathBuf,
    /// Authorize every required destination topic recreation
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct StateArgs {
    #[command(subcommand)]
    command: StateCommand,
}

#[derive(Debug, Subcommand)]
enum StateCommand {
    /// Print the state registry as JSON
    Show(StateShowArgs),
    /// Remove state for configured destination topics
    Reset(StateResetArgs),
}

#[derive(Debug, Args)]
struct StateShowArgs {
    /// Persistent state directory
    #[arg(long, value_name = "DIR", default_value = ".fransson")]
    state_dir: PathBuf,
}

#[derive(Debug, Args)]
#[command(group(clap::ArgGroup::new("selector").required(true).multiple(false).args(["topic", "all"])))]
struct StateResetArgs {
    /// YAML configuration file
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,
    /// Persistent state directory
    #[arg(long, value_name = "DIR", default_value = ".fransson")]
    state_dir: PathBuf,
    /// Reset one configured destination topic
    #[arg(long, value_name = "TOPIC")]
    topic: Option<String>,
    /// Reset every destination topic declared by the configuration
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Args)]
struct DumpArgs {
    /// YAML configuration file containing the selected source
    #[arg(short, long, value_name = "FILE")]
    config: PathBuf,
    /// Source topic in SOURCE:TOPIC form
    #[arg(long, value_name = "SOURCE:TOPIC")]
    source: String,
    /// Archive file to create
    #[arg(short = 'a', long, value_name = "FILE")]
    archive: PathBuf,
    /// Replace an existing archive
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppConfig {
    #[serde(default)]
    sources: HashMap<String, SourceKafkaConfig>,
    #[serde(default)]
    destination: Option<DestinationKafkaConfig>,
    #[serde(default)]
    topics: Vec<DestinationTopicConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceKafkaConfig {
    bootstrap_servers: String,
    #[serde(default)]
    client_id: Option<String>,
    group_id: String,
    #[serde(default = "default_max_in_flight_per_partition")]
    max_in_flight_per_partition: usize,
    #[serde(default)]
    security_protocol: Option<String>,
    #[serde(default)]
    sasl: Option<SaslConfig>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DestinationKafkaConfig {
    bootstrap_servers: String,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    security_protocol: Option<String>,
    #[serde(default)]
    sasl: Option<SaslConfig>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SaslConfig {
    mechanism: String,
    username: String,
    password_env: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DestinationTopicConfig {
    name: String,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    replication_factor: Option<i32>,
    #[serde(default)]
    config: BTreeMap<String, String>,
    #[serde(default)]
    manage: Option<StaticTopicConfig>,
    #[serde(default)]
    empty: Option<StaticTopicConfig>,
    #[serde(default)]
    clone: Option<CloneConfig>,
    #[serde(default)]
    stream: Option<StreamConfig>,
    #[serde(default)]
    restore: Option<RestoreConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CloneConfig {
    source: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamConfig {
    source: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreConfig {
    archive: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticTopicConfig {
    partitions: i32,
}

#[derive(Debug, Clone)]
struct SourceTopicRef {
    instance: String,
    topic: String,
}

impl fmt::Display for SourceTopicRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}{}{}",
            self.instance, SOURCE_REF_DELIMITER, self.topic
        )
    }
}

#[derive(Debug, Clone)]
struct TransferPlan {
    destination_topic: String,
    source: SourceTopicRef,
    kind: TransferKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferKind {
    Clone,
    Stream,
}

#[derive(Debug, Clone)]
struct ManagedTopic {
    name: String,
    force: bool,
    replication_factor: Option<i32>,
    config: BTreeMap<String, String>,
    static_topic: Option<StaticTopicPlan>,
    transfer: Option<TransferPlan>,
    restore: Option<RestorePlan>,
}

#[derive(Debug, Clone)]
struct StaticTopicPlan {
    partitions: i32,
    kind: StaticTopicKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaticTopicKind {
    Manage,
    Empty,
}

#[derive(Debug, Clone)]
struct RestorePlan {
    archive: PathBuf,
}

const STATE_FORMAT_VERSION: u16 = 2;
const STATE_FILE_NAME: &str = "state.json";
const STATE_LOCK_NAME: &str = "state.lock";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateRegistry {
    format_version: u16,
    clusters: BTreeMap<String, ClusterState>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterState {
    topics: BTreeMap<String, TopicState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TopicState {
    topic_id: String,
    state: TopicModeState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum TopicModeState {
    Clone(CloneState),
    Restore(RestoreMarker),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CloneState {
    source: SourceIdentity,
    next_offsets: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceIdentity {
    cluster_id: String,
    topic: String,
    topic_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreMarker {
    archive_sha256: String,
    archive_format_version: u16,
    status: RestoreStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RestoreStatus {
    InProgress,
    Complete,
}

#[derive(Debug, Clone, Default)]
struct OffsetState {
    topics: BTreeMap<String, TopicState>,
}

impl Default for StateRegistry {
    fn default() -> Self {
        Self {
            format_version: STATE_FORMAT_VERSION,
            clusters: BTreeMap::new(),
        }
    }
}

impl StateRegistry {
    fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let bytes = fs::read(path)
            .with_context(|| format!("failed to read state file {}", path.display()))?;
        let state: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse state file {}", path.display()))?;
        if state.format_version != STATE_FORMAT_VERSION {
            bail!(
                "unsupported state format version {} in {}",
                state.format_version,
                path.display()
            );
        }
        Ok(state)
    }

    fn persist(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create parent directory for {}", path.display())
            })?;
        }

        let tmp_path = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(self).context("failed to serialize offset state")?;
        let mut tmp = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .with_context(|| {
                format!("failed to open temporary state file {}", tmp_path.display())
            })?;
        tmp.write_all(&bytes).with_context(|| {
            format!(
                "failed to write temporary state file {}",
                tmp_path.display()
            )
        })?;
        tmp.sync_all().with_context(|| {
            format!("failed to sync temporary state file {}", tmp_path.display())
        })?;
        drop(tmp);
        fs::rename(&tmp_path, path).with_context(|| {
            format!(
                "failed to move temporary state file {} into place at {}",
                tmp_path.display(),
                path.display()
            )
        })?;
        if let Some(parent) = path.parent() {
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .with_context(|| format!("failed to sync state directory {}", parent.display()))?;
        }
        Ok(())
    }
}

impl OffsetState {
    fn next_offset(&self, destination_topic: &str, partition: i32) -> Option<i64> {
        let TopicModeState::Clone(clone) = &self.topics.get(destination_topic)?.state else {
            return None;
        };
        clone.next_offsets.get(&partition.to_string()).copied()
    }

    fn update_next_offset(
        &mut self,
        destination_topic: &str,
        topic_id: &str,
        source: &SourceIdentity,
        partition: i32,
        next_offset: i64,
    ) {
        let entry = self
            .topics
            .entry(destination_topic.to_owned())
            .or_insert_with(|| TopicState {
                topic_id: topic_id.to_owned(),
                state: TopicModeState::Clone(CloneState {
                    source: source.clone(),
                    next_offsets: BTreeMap::new(),
                }),
            });
        if let TopicModeState::Clone(clone) = &mut entry.state {
            clone
                .next_offsets
                .insert(partition.to_string(), next_offset);
        }
    }

    fn clear_topic(&mut self, destination_topic: &str) -> bool {
        self.topics.remove(destination_topic).is_some()
    }

    fn restore_matches(&self, destination_topic: &str, topic_id: &str, fingerprint: &str) -> bool {
        self.topics.get(destination_topic).is_some_and(|state| {
            state.topic_id == topic_id
                && matches!(&state.state, TopicModeState::Restore(marker)
                    if marker.archive_sha256 == fingerprint
                    && marker.archive_format_version == archive::format_version()
                    && marker.status == RestoreStatus::Complete)
        })
    }

    fn mark_restore(
        &mut self,
        destination_topic: &str,
        topic_id: String,
        fingerprint: String,
        status: RestoreStatus,
    ) {
        self.topics.insert(
            destination_topic.to_owned(),
            TopicState {
                topic_id,
                state: TopicModeState::Restore(RestoreMarker {
                    archive_sha256: fingerprint,
                    archive_format_version: archive::format_version(),
                    status,
                }),
            },
        );
    }
}

struct StateStore {
    file: PathBuf,
    lock: PathBuf,
    cluster_id: String,
    managed_topics: Vec<String>,
    _topic_locks: Vec<File>,
}

impl StateStore {
    fn open(
        directory: PathBuf,
        cluster_id: String,
        topics: &[ManagedTopic],
    ) -> Result<(Self, OffsetState)> {
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create state directory {}", directory.display()))?;
        let file = directory.join(STATE_FILE_NAME);
        let lock = directory.join(STATE_LOCK_NAME);
        let mut names: Vec<String> = topics.iter().map(|topic| topic.name.clone()).collect();
        names.sort();
        let lock_dir = directory.join("locks");
        fs::create_dir_all(&lock_dir)?;
        let mut topic_locks = Vec::new();
        for name in &names {
            let digest = sha256_text(&format!("{cluster_id}\0{name}"));
            let path = lock_dir.join(format!("{digest}.lock"));
            let handle = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)?;
            handle.try_lock_exclusive().with_context(|| {
                format!(
                    "destination topic {name} is already managed by another local Fransson process"
                )
            })?;
            topic_locks.push(handle);
        }
        let registry = with_registry_lock(&lock, || StateRegistry::load(&file))?;
        let state = OffsetState {
            topics: registry
                .clusters
                .get(&cluster_id)
                .map(|cluster| cluster.topics.clone())
                .unwrap_or_default(),
        };
        Ok((
            Self {
                file,
                lock,
                cluster_id,
                managed_topics: names,
                _topic_locks: topic_locks,
            },
            state,
        ))
    }

    fn persist(&self, snapshot: &OffsetState) -> Result<()> {
        with_registry_lock(&self.lock, || {
            let mut registry = StateRegistry::load(&self.file)?;
            let cluster = registry
                .clusters
                .entry(self.cluster_id.clone())
                .or_default();
            for topic in &self.managed_topics {
                if let Some(state) = snapshot.topics.get(topic) {
                    cluster.topics.insert(topic.clone(), state.clone());
                } else {
                    cluster.topics.remove(topic);
                }
            }
            if cluster.topics.is_empty() {
                registry.clusters.remove(&self.cluster_id);
            }
            registry.persist(&self.file)
        })
    }
}

fn with_registry_lock<T>(lock_path: &Path, operation: impl FnOnce() -> Result<T>) -> Result<T> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock.lock_exclusive()?;
    let result = operation();
    FileExt::unlock(&lock)?;
    result
}

fn sha256_text(value: &str) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(value.as_bytes()))
}

#[derive(Clone)]
struct RuntimeContext {
    state_store: Arc<StateStore>,
    state: Arc<Mutex<OffsetState>>,
    state_dirty: Arc<AtomicBool>,
    producer: FutureProducer,
    stream_producer: Option<FutureProducer>,
    status: Arc<Mutex<StatusBoard>>,
    destination_topic_ids: Arc<Mutex<HashMap<String, String>>>,
    source_identities: Arc<HashMap<String, SourceIdentity>>,
}

#[derive(Debug, Clone)]
struct StatusLine {
    destination_topic: String,
    partition: i32,
    next_offset: Option<i64>,
    last_error: Option<String>,
}

#[derive(Debug, Default)]
struct StatusBoard {
    order: Vec<String>,
    lines: HashMap<String, StatusLine>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionMode {
    Restore,
    Run,
}

type CloneEndOffsets = HashMap<String, i64>;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Dump(args) => dump_topic(args).await,
        Command::Restore(args) => {
            execute_config(
                args.config,
                args.state_dir,
                ExecutionMode::Restore,
                args.force,
            )
            .await
        }
        Command::Run(args) => {
            execute_config(args.config, args.state_dir, ExecutionMode::Run, args.force).await
        }
        Command::State(args) => execute_state_command(args).await,
    }
}

async fn execute_state_command(args: StateArgs) -> Result<()> {
    match args.command {
        StateCommand::Show(args) => {
            let directory = absolute_path(&args.state_dir)?;
            let file = directory.join(STATE_FILE_NAME);
            let registry = if directory.exists() {
                with_registry_lock(&directory.join(STATE_LOCK_NAME), || {
                    StateRegistry::load(&file)
                })?
            } else {
                StateRegistry::default()
            };
            println!("{}", serde_json::to_string_pretty(&registry)?);
            Ok(())
        }
        StateCommand::Reset(args) => reset_state(args).await,
    }
}

async fn reset_state(args: StateResetArgs) -> Result<()> {
    let config = load_config(&args.config)?;
    validate_config(&config)?;
    let destination = config
        .destination
        .as_ref()
        .context("destination is required by state reset")?;
    let topics = resolve_topics(&config, &args.config)?;
    if topics.is_empty() {
        bail!("topics must contain at least one destination for state reset");
    }
    let selected: Vec<String> = if args.all {
        topics.iter().map(|topic| topic.name.clone()).collect()
    } else {
        let topic = args.topic.context("--topic or --all is required")?;
        if !topics.iter().any(|configured| configured.name == topic) {
            bail!("destination topic {topic} is not declared by the configuration");
        }
        vec![topic]
    };
    let consumer = build_consumer_for_destination_metadata(destination)?;
    consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .context("failed to connect to destination Kafka cluster")?;
    let cluster_id = kafka_identity::cluster_id(consumer.client())
        .context("failed to identify destination Kafka cluster")?;
    let directory = absolute_path(&args.state_dir)?;
    let (store, mut state) = StateStore::open(directory, cluster_id, &topics)?;
    for topic in &selected {
        state.clear_topic(topic);
    }
    store.persist(&state)?;
    println!("reset state for {}", selected.join(", "));
    Ok(())
}

async fn execute_config(
    config_path: PathBuf,
    state_dir: PathBuf,
    mode: ExecutionMode,
    force: bool,
) -> Result<()> {
    let config = load_config(&config_path)?;
    validate_config(&config)?;
    let destination = config
        .destination
        .as_ref()
        .context("destination is required by restore and run")?;
    if config.topics.is_empty() {
        bail!("topics must contain at least one destination for restore and run");
    }
    let topics = resolve_topics(&config, &config_path)?;
    let transfer_topics = collect_transfer_topics_by_source(&topics);
    if mode == ExecutionMode::Run && transfer_topics.is_empty() {
        bail!("run requires at least one clone or stream topic");
    }
    let clone_topics = collect_clone_topics_by_source(&topics);
    let source_identities = fetch_source_identities(&config.sources, &clone_topics)?;
    let clone_end_offsets = if mode == ExecutionMode::Restore {
        fetch_clone_end_offsets(&config.sources, &clone_topics)?
    } else {
        HashMap::new()
    };
    let producer = build_producer(destination)?;
    let destination_cluster_id = kafka_identity::cluster_id(producer.client())
        .context("failed to identify destination Kafka cluster")?;
    let stream_producer = if mode == ExecutionMode::Run
        && topics.iter().any(|topic| {
            topic
                .transfer
                .as_ref()
                .is_some_and(|transfer| transfer.kind == TransferKind::Stream)
        }) {
        Some(build_stream_producer(destination)?)
    } else {
        None
    };

    let resolved_state_dir = absolute_path(&state_dir)?;
    eprintln!(
        "state: {}",
        resolved_state_dir.join(STATE_FILE_NAME).display()
    );
    let (state_store, state) =
        StateStore::open(resolved_state_dir, destination_cluster_id, &topics)?;
    let runtime = RuntimeContext {
        state_store: Arc::new(state_store),
        state: Arc::new(Mutex::new(state)),
        state_dirty: Arc::new(AtomicBool::new(false)),
        producer,
        stream_producer,
        status: Arc::new(Mutex::new(StatusBoard::default())),
        destination_topic_ids: Arc::new(Mutex::new(HashMap::new())),
        source_identities: Arc::new(source_identities),
    };

    reconcile_destination_topics(
        &config.sources,
        destination,
        &topics,
        &transfer_topics,
        &runtime,
        force,
    )
    .await?;
    clear_non_stateful_topic_state(&topics, &runtime).await?;
    {
        let state = runtime.state.lock().await;
        validate_state_sources(&state, &topics, &runtime.source_identities)?;
    }

    let mut workers = JoinSet::new();
    let mut background = JoinSet::new();
    let reporter_runtime = runtime.clone();
    background.spawn(async move { run_status_reporter(reporter_runtime).await });
    let flusher_runtime = runtime.clone();
    background.spawn(async move { run_state_flusher(flusher_runtime).await });

    let active_topics = if mode == ExecutionMode::Run {
        transfer_topics
    } else {
        clone_topics
    };
    for (source_name, plans) in active_topics {
        let source_config = config
            .sources
            .get(&source_name)
            .ok_or_else(|| anyhow!("missing source configuration for {source_name}"))?;
        let max_in_flight_per_partition = source_config.max_in_flight_per_partition;
        let consumer = Arc::new(if mode == ExecutionMode::Restore {
            build_source_consumer(source_config, true)?
        } else {
            build_consumer(source_config)?
        });
        let metadata = fetch_metadata(consumer.as_ref(), &plans)?;
        let assignment = {
            let state = runtime.state.lock().await;
            build_assignment(&metadata, &plans, &state, &clone_end_offsets)?
        };
        {
            let state = runtime.state.lock().await;
            let mut status = runtime.status.lock().await;
            initialize_status_lines(&mut status, &metadata, &plans, &state)?;
        }
        consumer
            .assign(&assignment)
            .with_context(|| format!("failed to assign partitions for source {source_name}"))?;

        let transfer_map = build_transfer_map(&plans);
        for plan in &plans {
            let metadata_topic = metadata
                .topics()
                .iter()
                .find(|topic| topic.name() == plan.source.topic)
                .ok_or_else(|| {
                    anyhow!("missing metadata for source topic {}", plan.source.topic)
                })?;

            for partition in metadata_topic.partitions() {
                let partition_queue = consumer
                    .split_partition_queue(&plan.source.topic, partition.id())
                    .ok_or_else(|| {
                        anyhow!(
                            "failed to split partition queue for {}:{}:{}",
                            source_name,
                            plan.source.topic,
                            partition.id()
                        )
                    })?;
                let runtime = runtime.clone();
                let plan = plan.clone();
                let partition_id = partition.id();
                let end_offset = if mode == ExecutionMode::Restore {
                    Some(
                        *clone_end_offsets
                            .get(&clone_boundary_key(
                                &plan.source.instance,
                                &plan.source.topic,
                                partition_id,
                            ))
                            .ok_or_else(|| {
                                anyhow!(
                                    "missing captured end offset for {}:{} partition {}",
                                    plan.source.instance,
                                    plan.source.topic,
                                    partition_id
                                )
                            })?,
                    )
                } else {
                    None
                };
                workers.spawn(async move {
                    run_partition_loop(
                        plan,
                        partition_id,
                        partition_queue,
                        runtime,
                        max_in_flight_per_partition,
                        end_offset,
                    )
                    .await
                });
            }
        }

        let runtime = runtime.clone();
        let source_name = source_name.clone();
        let event_consumer = consumer.clone();
        background.spawn(async move {
            run_event_pump(source_name, transfer_map, event_consumer, runtime).await
        });
    }

    if mode == ExecutionMode::Restore {
        let outcome = loop {
            if workers.is_empty() {
                break Ok(());
            }
            tokio::select! {
                result = workers.join_next() => {
                    let result = result
                        .context("transfer partition task disappeared")?
                        .context("transfer partition task panicked");
                    if let Err(error) = result.and_then(|result| result) {
                        break Err(error);
                    }
                }
                result = background.join_next() => {
                    let result = result
                        .context("background task disappeared")?
                        .context("background task panicked");
                    break match result {
                        Err(error) => Err(error),
                        Ok(Err(error)) => Err(error),
                        Ok(Ok(())) => Err(anyhow!("background task exited unexpectedly")),
                    };
                }
            }
        };
        let flush_result = flush_state(&runtime, true).await;
        workers.abort_all();
        background.abort_all();
        outcome?;
        flush_result?;
    } else {
        let outcome = tokio::select! {
            _ = tokio::signal::ctrl_c() => Ok(()),
            maybe_result = workers.join_next(), if !workers.is_empty() => {
                match maybe_result {
                    Some(result) => {
                        match result.context("transfer partition task panicked") {
                            Err(error) => Err(error),
                            Ok(Err(error)) => Err(error),
                            Ok(Ok(())) => Err(anyhow!("transfer partition task exited unexpectedly")),
                        }
                    }
                    None => Err(anyhow!("all transfer partition tasks exited unexpectedly")),
                }
            }
            maybe_result = background.join_next() => {
                match maybe_result {
                    Some(result) => {
                        match result.context("background task panicked") {
                            Err(error) => Err(error),
                            Ok(Err(error)) => Err(error),
                            Ok(Ok(())) => Err(anyhow!("background task exited unexpectedly")),
                        }
                    }
                    None => Err(anyhow!("all background tasks exited unexpectedly")),
                }
            }
        };
        let flush_result = flush_state(&runtime, true).await;
        workers.abort_all();
        background.abort_all();
        outcome?;
        flush_result?;
    }

    let mut stdout = io::stdout().lock();
    let _ = writeln!(stdout);

    Ok(())
}

async fn run_partition_loop(
    plan: TransferPlan,
    partition_id: i32,
    partition_queue: StreamPartitionQueue<DefaultConsumerContext>,
    runtime: RuntimeContext,
    max_in_flight_per_partition: usize,
    end_offset: Option<i64>,
) -> Result<()> {
    let mut inflight = FuturesOrdered::new();
    let producer = match plan.kind {
        TransferKind::Clone => runtime.producer.clone(),
        TransferKind::Stream => runtime
            .stream_producer
            .clone()
            .context("stream producer is unavailable outside run mode")?,
    };

    loop {
        while inflight.len() >= max_in_flight_per_partition {
            if let Some(result) = inflight.next().await {
                handle_delivery_result(&plan, result, &runtime).await?;
            }
        }

        let message = match partition_queue.recv().await {
            Ok(message) => {
                if end_offset.is_some_and(|end| message.offset() >= end) {
                    finish_partition_inflight(
                        &plan,
                        partition_id,
                        &runtime,
                        &mut inflight,
                        end_offset,
                    )
                    .await?;
                    return Ok(());
                }
                message
            }
            Err(rdkafka::error::KafkaError::PartitionEOF(_)) if end_offset.is_some() => {
                finish_partition_inflight(&plan, partition_id, &runtime, &mut inflight, end_offset)
                    .await?;
                return Ok(());
            }
            Err(err) => {
                set_partition_error(
                    &runtime.status,
                    &plan.destination_topic,
                    partition_id,
                    format!(
                        "consumer error on source {} partition {}: {}",
                        plan.source.instance, partition_id, err
                    ),
                )
                .await;
                continue;
            }
        };
        inflight.push_back(send_owned_message(
            producer.clone(),
            plan.clone(),
            message.detach(),
        ));
    }
}

async fn finish_partition_inflight(
    plan: &TransferPlan,
    partition_id: i32,
    runtime: &RuntimeContext,
    inflight: &mut FuturesOrdered<impl std::future::Future<Output = DeliveryResult>>,
    end_offset: Option<i64>,
) -> Result<()> {
    while let Some(result) = inflight.next().await {
        handle_delivery_result(plan, result, runtime).await?;
    }
    if let Some(end_offset) = end_offset {
        let topic_id = runtime
            .destination_topic_ids
            .lock()
            .await
            .get(&plan.destination_topic)
            .cloned()
            .context("missing destination topic identity")?;
        let source = runtime
            .source_identities
            .get(&plan.source.to_string())
            .context("missing source topic identity")?;
        let mut state = runtime.state.lock().await;
        let current = state
            .next_offset(&plan.destination_topic, partition_id)
            .unwrap_or(i64::MIN);
        if current < end_offset {
            state.update_next_offset(
                &plan.destination_topic,
                &topic_id,
                source,
                partition_id,
                end_offset,
            );
            runtime.state_dirty.store(true, Ordering::Release);
            drop(state);
            update_partition_offset(
                &runtime.status,
                &plan.destination_topic,
                partition_id,
                end_offset,
            )
            .await;
        }
    }
    Ok(())
}

async fn run_event_pump(
    source_name: String,
    transfer_map: HashMap<String, TransferPlan>,
    consumer: Arc<StreamConsumer>,
    runtime: RuntimeContext,
) -> Result<()> {
    loop {
        match consumer.recv().await {
            Ok(message) => {
                if let Some(plan) = transfer_map.get(message.topic()) {
                    set_partition_error(
                        &runtime.status,
                        &plan.destination_topic,
                        message.partition(),
                        format!(
                            "unexpected message on shared consumer queue for source {}:{} partition {}",
                            plan.source.instance,
                            message.topic(),
                            message.partition()
                        ),
                    )
                    .await;
                } else {
                    set_source_error(
                        &runtime.status,
                        &source_name,
                        format!(
                            "received message for unconfigured source topic {} on source {}",
                            message.topic(),
                            source_name
                        ),
                    )
                    .await;
                }
            }
            Err(err) => {
                set_source_error(
                    &runtime.status,
                    &source_name,
                    format!("consumer error on source {source_name}: {err}"),
                )
                .await;
            }
        }
    }
}

struct DeliveryResult {
    source_topic: String,
    destination_topic: String,
    partition: i32,
    next_offset: i64,
    produce_result: Result<(), String>,
}

async fn send_owned_message(
    producer: FutureProducer,
    plan: TransferPlan,
    message: OwnedMessage,
) -> DeliveryResult {
    let destination_topic = plan.destination_topic.clone();
    let next_offset = message.offset() + 1;

    let payload = message.payload().map(|payload| payload.to_vec());
    let key = message.key().map(|key| key.to_vec());
    let headers = message.headers().cloned();
    let partition = message.partition();
    let source_topic = message.topic().to_owned();
    let timestamp = message.timestamp().to_millis();

    let mut record = FutureRecord::to(destination_topic.as_str()).partition(partition);
    if let Some(ref payload) = payload {
        record = record.payload(payload.as_slice());
    }
    if let Some(ref key) = key {
        record = record.key(key.as_slice());
    }
    if let Some(headers) = headers {
        record = record.headers(headers);
    }
    if let Some(timestamp) = timestamp {
        record = record.timestamp(timestamp);
    }

    let produce_result = match plan.kind {
        TransferKind::Clone => producer.send(record, Duration::from_secs(30)).await,
        TransferKind::Stream => producer.send(record, Timeout::Never).await,
    }
    .map(|_| ())
    .map_err(|(err, _)| err.to_string());

    DeliveryResult {
        source_topic,
        destination_topic,
        partition,
        next_offset,
        produce_result,
    }
}

async fn handle_delivery_result(
    plan: &TransferPlan,
    result: DeliveryResult,
    runtime: &RuntimeContext,
) -> Result<()> {
    match result.produce_result {
        Ok(()) => {
            if plan.kind == TransferKind::Clone {
                let topic_id = runtime
                    .destination_topic_ids
                    .lock()
                    .await
                    .get(&result.destination_topic)
                    .cloned()
                    .context("missing destination topic identity")?;
                let source = runtime
                    .source_identities
                    .get(&plan.source.to_string())
                    .context("missing source topic identity")?;
                let mut state = runtime.state.lock().await;
                state.update_next_offset(
                    &result.destination_topic,
                    &topic_id,
                    source,
                    result.partition,
                    result.next_offset,
                );
                drop(state);
                runtime.state_dirty.store(true, Ordering::Release);
            }

            update_partition_offset(
                &runtime.status,
                &result.destination_topic,
                result.partition,
                result.next_offset,
            )
            .await;
            Ok(())
        }
        Err(err) => {
            let detail = format!(
                "produce error from source {}:{} to destination {}: {}",
                plan.source.instance, result.source_topic, result.destination_topic, err
            );
            set_partition_error(
                &runtime.status,
                &plan.destination_topic,
                result.partition,
                detail.clone(),
            )
            .await;
            bail!(detail)
        }
    }
}

async fn run_status_reporter(runtime: RuntimeContext) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;
        let snapshot = {
            let status = runtime.status.lock().await;
            render_status_board(&status)
        };

        let mut stdout = io::stdout().lock();
        write!(stdout, "\x1b[2J\x1b[H{snapshot}")?;
        stdout.flush()?;
    }
}

async fn run_state_flusher(runtime: RuntimeContext) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;
        flush_state(&runtime, false).await?;
    }
}

async fn flush_state(runtime: &RuntimeContext, force: bool) -> Result<()> {
    if !force && !runtime.state_dirty.swap(false, Ordering::AcqRel) {
        return Ok(());
    }

    if force {
        runtime.state_dirty.store(false, Ordering::Release);
    }

    let snapshot = {
        let state = runtime.state.lock().await;
        state.clone()
    };

    if let Err(err) = runtime.state_store.persist(&snapshot) {
        runtime.state_dirty.store(true, Ordering::Release);
        return Err(err);
    }

    Ok(())
}

fn load_config(path: &Path) -> Result<AppConfig> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read config file {}", path.display()))?;
    let config = serde_yaml::from_slice(&bytes)
        .with_context(|| format!("failed to parse YAML config {}", path.display()))?;
    Ok(config)
}

fn resolve_config_path(config_path: &Path, value: &Path) -> PathBuf {
    if value.is_absolute() {
        value.to_owned()
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(value)
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(env::current_dir()
            .context("failed to resolve current directory")?
            .join(path))
    }
}

fn validate_config(config: &AppConfig) -> Result<()> {
    const SOURCE_RESERVED: &[&str] = &[
        "bootstrap.servers",
        "client.id",
        "group.id",
        "security.protocol",
        "sasl.mechanism",
        "sasl.username",
        "sasl.password",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "enable.partition.eof",
    ];
    const DESTINATION_RESERVED: &[&str] = &[
        "bootstrap.servers",
        "client.id",
        "group.id",
        "security.protocol",
        "sasl.mechanism",
        "sasl.username",
        "sasl.password",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "acks",
        "enable.idempotence",
        "message.send.max.retries",
        "retries",
        "message.timeout.ms",
        "delivery.timeout.ms",
        "max.in.flight.requests.per.connection",
    ];

    for (name, source) in &config.sources {
        require_nonblank(name, "source name")?;
        require_nonblank(
            &source.bootstrap_servers,
            &format!("source {name} bootstrap_servers"),
        )?;
        require_nonblank(&source.group_id, &format!("source {name} group_id"))?;
        if source.max_in_flight_per_partition == 0 {
            bail!("source {name} max_in_flight_per_partition must be greater than zero");
        }
        validate_optional_nonblank(
            source.client_id.as_deref(),
            &format!("source {name} client_id"),
        )?;
        validate_optional_nonblank(
            source.security_protocol.as_deref(),
            &format!("source {name} security_protocol"),
        )?;
        validate_sasl(source.sasl.as_ref(), &format!("source {name}"))?;
        validate_properties(
            &source.properties,
            SOURCE_RESERVED,
            &format!("source {name}"),
        )?;
    }

    if let Some(destination) = &config.destination {
        require_nonblank(
            &destination.bootstrap_servers,
            "destination bootstrap_servers",
        )?;
        validate_optional_nonblank(destination.client_id.as_deref(), "destination client_id")?;
        validate_optional_nonblank(
            destination.security_protocol.as_deref(),
            "destination security_protocol",
        )?;
        validate_sasl(destination.sasl.as_ref(), "destination")?;
        validate_properties(&destination.properties, DESTINATION_RESERVED, "destination")?;
    }

    for topic in &config.topics {
        require_nonblank(&topic.name, "destination topic name")?;
        for (mode, static_topic) in [
            ("manage", topic.manage.as_ref()),
            ("empty", topic.empty.as_ref()),
        ] {
            if static_topic.is_some_and(|config| config.partitions <= 0) {
                bail!(
                    "destination topic {} {mode}.partitions must be greater than zero",
                    topic.name
                );
            }
        }
        if topic.replication_factor.is_some_and(|value| value <= 0) {
            bail!(
                "destination topic {} replication_factor must be greater than zero",
                topic.name
            );
        }
        validate_properties(
            &topic.config,
            &["message.timestamp.type"],
            &format!("destination topic {} config", topic.name),
        )?;
        if topic
            .restore
            .as_ref()
            .is_some_and(|restore| restore.archive.as_os_str().is_empty())
        {
            bail!(
                "destination topic {} restore.archive must not be empty",
                topic.name
            );
        }
    }

    Ok(())
}

fn require_nonblank(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be blank");
    }
    Ok(())
}

fn validate_optional_nonblank(value: Option<&str>, field: &str) -> Result<()> {
    if let Some(value) = value {
        require_nonblank(value, field)?;
    }
    Ok(())
}

fn validate_sasl(sasl: Option<&SaslConfig>, scope: &str) -> Result<()> {
    if let Some(sasl) = sasl {
        require_nonblank(&sasl.mechanism, &format!("{scope} sasl.mechanism"))?;
        require_nonblank(&sasl.username, &format!("{scope} sasl.username"))?;
        require_nonblank(&sasl.password_env, &format!("{scope} sasl.password_env"))?;
    }
    Ok(())
}

fn validate_properties(
    properties: &BTreeMap<String, String>,
    reserved: &[&str],
    scope: &str,
) -> Result<()> {
    for key in properties.keys() {
        require_nonblank(key, &format!("{scope} property name"))?;
        if reserved.contains(&key.as_str()) {
            bail!("{scope} property {key} is owned by fransson and cannot be configured");
        }
    }
    Ok(())
}

async fn dump_topic(args: DumpArgs) -> Result<()> {
    if args.archive.exists() && !args.force {
        bail!(
            "output archive {} already exists; use --force to replace it",
            args.archive.display()
        );
    }

    let config = load_config(&args.config)?;
    validate_config(&config)?;
    let _ = resolve_topics(&config, &args.config)?;
    let source = parse_source_topic_ref(&args.source)
        .with_context(|| format!("invalid dump source reference {}", args.source))?;
    let source_config = config
        .sources
        .get(&source.instance)
        .ok_or_else(|| anyhow!("dump references unknown source {}", source.instance))?;
    let group_id = source_consumer_group_id(source_config);
    let consumer = build_dump_consumer(source_config)?;
    let metadata = consumer
        .fetch_metadata(Some(&source.topic), Duration::from_secs(10))
        .with_context(|| {
            format!(
                "failed to fetch metadata for dump source {}:{}",
                source.instance, source.topic
            )
        })?;
    let metadata_topic = metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == source.topic)
        .ok_or_else(|| anyhow!("source topic {} not found", source.topic))?;
    if let Some(error) = metadata_topic.error() {
        bail!("source topic {} metadata error: {:?}", source.topic, error);
    }

    let mut partitions: Vec<i32> = metadata_topic
        .partitions()
        .iter()
        .map(|partition| partition.id())
        .collect();
    partitions.sort_unstable();
    let mut watermarks = HashMap::new();
    for partition in &partitions {
        let bounds = consumer
            .fetch_watermarks(&source.topic, *partition, Duration::from_secs(10))
            .with_context(|| {
                format!(
                    "failed to fetch watermarks for {} partition {}",
                    source.topic, partition
                )
            })?;
        watermarks.insert(*partition, bounds);
    }

    let temp_path = args.archive.with_extension(format!(
        "{}.{}.tmp",
        args.archive
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("archive"),
        std::process::id()
    ));
    if temp_path.exists() {
        fs::remove_file(&temp_path).with_context(|| {
            format!(
                "failed to remove stale temporary file {}",
                temp_path.display()
            )
        })?;
    }

    let result = dump_topic_to_path(
        &consumer,
        &source.topic,
        &partitions,
        &watermarks,
        group_id,
        &temp_path,
    )
    .await;
    if let Err(error) = result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    fs::rename(&temp_path, &args.archive).with_context(|| {
        format!(
            "failed to move completed archive {} to {}",
            temp_path.display(),
            args.archive.display()
        )
    })?;

    println!(
        "dumped {}:{} to {} ({})",
        source.instance,
        source.topic,
        args.archive.display(),
        archive::fingerprint(&args.archive)?
    );
    Ok(())
}

async fn dump_topic_to_path(
    consumer: &StreamConsumer,
    topic: &str,
    partitions: &[i32],
    watermarks: &HashMap<i32, (i64, i64)>,
    group_id: &str,
    output: &Path,
) -> Result<()> {
    let mut writer = ArchiveWriter::create(output, partitions)?;
    for partition in partitions {
        writer.begin_partition(*partition)?;
        let (low, high) = watermarks
            .get(partition)
            .copied()
            .ok_or_else(|| anyhow!("missing captured watermarks for partition {partition}"))?;
        if low < high {
            let mut assignment = TopicPartitionList::new();
            assignment.add_partition_offset(topic, *partition, Offset::Offset(low))?;
            consumer.assign(&assignment)?;
            loop {
                match consumer.recv().await {
                    Ok(message) if message.partition() != *partition => {
                        bail!(
                            "received unexpected partition {} while dumping partition {}",
                            message.partition(),
                            partition
                        );
                    }
                    Ok(message) if message.offset() >= high => break,
                    Ok(message) => writer.write_record(&archive_record_from_message(&message))?,
                    Err(rdkafka::error::KafkaError::PartitionEOF(id)) if id == *partition => break,
                    Err(error) => bail!(
                        "consumer error while dumping {} partition {} using group {}: {}",
                        topic,
                        partition,
                        group_id,
                        error
                    ),
                }
            }
        }
        writer.end_partition()?;
    }
    consumer.unassign()?;
    writer.finish()
}

fn archive_record_from_message(message: &impl Message) -> ArchiveRecord {
    let headers = message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| ArchiveHeader {
                    key: header.key.to_owned(),
                    value: header.value.map(ToOwned::to_owned),
                })
                .collect()
        })
        .unwrap_or_default();
    ArchiveRecord {
        timestamp: message.timestamp().to_millis(),
        key: message.key().map(ToOwned::to_owned),
        payload: message.payload().map(ToOwned::to_owned),
        headers,
    }
}

fn resolve_topics(config: &AppConfig, config_path: &Path) -> Result<Vec<ManagedTopic>> {
    let mut seen_destinations = HashMap::new();
    let mut seen_sources = HashMap::new();
    let mut resolved = Vec::with_capacity(config.topics.len());

    for topic in &config.topics {
        if seen_destinations.insert(topic.name.as_str(), ()).is_some() {
            bail!("duplicate destination topic definition for {}", topic.name);
        }

        let configured_modes = usize::from(topic.manage.is_some())
            + usize::from(topic.empty.is_some())
            + usize::from(topic.clone.is_some())
            + usize::from(topic.stream.is_some())
            + usize::from(topic.restore.is_some());
        if configured_modes != 1 {
            bail!(
                "destination topic {} must configure exactly one of manage, empty, clone, stream, and restore",
                topic.name
            );
        }

        let static_topic = topic
            .manage
            .as_ref()
            .map(|manage| StaticTopicPlan {
                partitions: manage.partitions,
                kind: StaticTopicKind::Manage,
            })
            .or_else(|| {
                topic.empty.as_ref().map(|empty| StaticTopicPlan {
                    partitions: empty.partitions,
                    kind: StaticTopicKind::Empty,
                })
            });

        let transfer = if let Some((source_ref, kind, label)) = topic
            .clone
            .as_ref()
            .map(|clone| (&clone.source, TransferKind::Clone, "clone"))
            .or_else(|| {
                topic
                    .stream
                    .as_ref()
                    .map(|stream| (&stream.source, TransferKind::Stream, "stream"))
            }) {
            let source = parse_source_topic_ref(source_ref)
                .with_context(|| format!("invalid {label} source reference {source_ref}"))?;
            if !config.sources.contains_key(&source.instance) {
                bail!("{label} references unknown source {}", source.instance);
            }
            let source_key = source.to_string();
            if let Some(existing) = seen_sources.insert(source_key.clone(), topic.name.as_str()) {
                bail!(
                    "source topic {source_key} cannot feed both destination topics {existing} and {}",
                    topic.name
                );
            }
            Some(TransferPlan {
                destination_topic: topic.name.clone(),
                source,
                kind,
            })
        } else {
            None
        };

        let restore = topic.restore.as_ref().map(|restore| {
            let archive = resolve_config_path(config_path, &restore.archive);
            RestorePlan { archive }
        });

        resolved.push(ManagedTopic {
            name: topic.name.clone(),
            force: topic.force,
            replication_factor: topic.replication_factor,
            config: topic.config.clone(),
            static_topic,
            transfer,
            restore,
        });
    }

    Ok(resolved)
}

fn parse_source_topic_ref(value: &str) -> Result<SourceTopicRef> {
    let (instance, topic) = value.split_once(SOURCE_REF_DELIMITER).ok_or_else(|| {
        anyhow!("expected format <source>{SOURCE_REF_DELIMITER}<topic>, got {value}")
    })?;

    if instance.is_empty() || topic.is_empty() {
        bail!("expected non-empty <source>{SOURCE_REF_DELIMITER}<topic>, got {value}");
    }

    Ok(SourceTopicRef {
        instance: instance.to_owned(),
        topic: topic.to_owned(),
    })
}

fn collect_transfer_topics_by_source(
    topics: &[ManagedTopic],
) -> HashMap<String, Vec<TransferPlan>> {
    collect_topics_by_source(topics, None)
}

fn validate_state_sources(
    state: &OffsetState,
    topics: &[ManagedTopic],
    identities: &HashMap<String, SourceIdentity>,
) -> Result<()> {
    for topic in topics {
        let Some(transfer) = topic
            .transfer
            .as_ref()
            .filter(|transfer| transfer.kind == TransferKind::Clone)
        else {
            continue;
        };
        if let Some(TopicState {
            state: TopicModeState::Clone(clone_state),
            ..
        }) = state.topics.get(&topic.name)
        {
            let configured_source = identities
                .get(&transfer.source.to_string())
                .context("missing configured source identity")?;
            if &clone_state.source != configured_source {
                bail!(
                    "state for destination topic {} belongs to source cluster/topic {}/{} ({}) but config uses {}/{} ({})",
                    topic.name,
                    clone_state.source.cluster_id,
                    clone_state.source.topic,
                    clone_state.source.topic_id,
                    configured_source.cluster_id,
                    configured_source.topic,
                    configured_source.topic_id,
                );
            }
        }
    }
    Ok(())
}

fn clone_state_mismatch_reason(
    saved: Option<&TopicState>,
    destination_topic_id: &str,
    source: &SourceIdentity,
) -> Option<String> {
    match saved {
        None => Some("existing clone topic has no trusted state".to_owned()),
        Some(saved) if saved.topic_id != destination_topic_id => Some(format!(
            "saved destination topic UUID is {} but broker reports {}",
            saved.topic_id, destination_topic_id
        )),
        Some(TopicState {
            state: TopicModeState::Clone(clone),
            ..
        }) if &clone.source != source => {
            Some("saved clone source identity differs from the configured source".to_owned())
        }
        Some(TopicState {
            state: TopicModeState::Clone(_),
            ..
        }) => None,
        Some(_) => Some("saved state belongs to a different data mode".to_owned()),
    }
}

fn collect_clone_topics_by_source(topics: &[ManagedTopic]) -> HashMap<String, Vec<TransferPlan>> {
    collect_topics_by_source(topics, Some(TransferKind::Clone))
}

fn fetch_source_identities(
    sources: &HashMap<String, SourceKafkaConfig>,
    grouped: &HashMap<String, Vec<TransferPlan>>,
) -> Result<HashMap<String, SourceIdentity>> {
    let mut identities = HashMap::new();
    for (source_name, plans) in grouped {
        let config = sources
            .get(source_name)
            .ok_or_else(|| anyhow!("missing source configuration for {source_name}"))?;
        let consumer = build_source_consumer(config, true)?;
        let cluster_id = kafka_identity::cluster_id(consumer.client())
            .with_context(|| format!("failed to identify source Kafka cluster {source_name}"))?;
        for plan in plans {
            let topic_id = kafka_identity::topic_id(consumer.client(), &plan.source.topic)
                .with_context(|| format!("failed to identify source topic {}", plan.source))?;
            identities.insert(
                plan.source.to_string(),
                SourceIdentity {
                    cluster_id: cluster_id.clone(),
                    topic: plan.source.topic.clone(),
                    topic_id,
                },
            );
        }
    }
    Ok(identities)
}

fn collect_topics_by_source(
    topics: &[ManagedTopic],
    kind: Option<TransferKind>,
) -> HashMap<String, Vec<TransferPlan>> {
    let mut grouped = HashMap::<String, Vec<TransferPlan>>::new();

    for topic in topics {
        if let Some(transfer) = &topic.transfer {
            if kind.is_some_and(|kind| transfer.kind != kind) {
                continue;
            }
            grouped
                .entry(transfer.source.instance.clone())
                .or_default()
                .push(transfer.clone());
        }
    }

    grouped
}

fn build_transfer_map(plans: &[TransferPlan]) -> HashMap<String, TransferPlan> {
    plans
        .iter()
        .map(|plan| (plan.source.topic.clone(), plan.clone()))
        .collect()
}

fn initialize_status_lines(
    status: &mut StatusBoard,
    metadata: &Metadata,
    plans: &[TransferPlan],
    state: &OffsetState,
) -> Result<()> {
    for plan in plans {
        let metadata_topic = metadata
            .topics()
            .iter()
            .find(|topic| topic.name() == plan.source.topic)
            .ok_or_else(|| anyhow!("missing metadata for source topic {}", plan.source.topic))?;

        for partition in metadata_topic.partitions() {
            let key = status_key(&plan.destination_topic, partition.id());
            if status.lines.contains_key(&key) {
                continue;
            }
            status.order.push(key.clone());
            status.lines.insert(
                key,
                StatusLine {
                    destination_topic: plan.destination_topic.clone(),
                    partition: partition.id(),
                    next_offset: if plan.kind == TransferKind::Clone {
                        state.next_offset(&plan.destination_topic, partition.id())
                    } else {
                        None
                    },
                    last_error: None,
                },
            );
        }
    }

    status.order.sort();
    Ok(())
}

async fn update_partition_offset(
    status: &Arc<Mutex<StatusBoard>>,
    destination_topic: &str,
    partition: i32,
    next_offset: i64,
) {
    let key = status_key(destination_topic, partition);
    let mut status = status.lock().await;
    if let Some(line) = status.lines.get_mut(&key) {
        line.next_offset = Some(next_offset);
        line.last_error = None;
    }
}

async fn set_partition_error(
    status: &Arc<Mutex<StatusBoard>>,
    destination_topic: &str,
    partition: i32,
    error: String,
) {
    let key = status_key(destination_topic, partition);
    let mut status = status.lock().await;
    if let Some(line) = status.lines.get_mut(&key) {
        line.last_error = Some(error);
    }
}

async fn set_source_error(status: &Arc<Mutex<StatusBoard>>, source_name: &str, error: String) {
    let mut status = status.lock().await;
    for line in status.lines.values_mut() {
        if line
            .last_error
            .as_deref()
            .is_some_and(|existing| existing.contains(source_name))
            || line.last_error.is_none()
        {
            line.last_error = Some(error.clone());
        }
    }
}

fn render_status_board(status: &StatusBoard) -> String {
    const RESET: &str = "\x1b[0m";
    const BOLD: &str = "\x1b[1m";
    const DIM: &str = "\x1b[2m";
    const GREEN: &str = "\x1b[32m";
    const RED: &str = "\x1b[31m";
    const YELLOW: &str = "\x1b[33m";

    let mut output = String::new();
    output.push_str(&format!("{BOLD}fransson status{RESET}\n"));
    output.push_str(&format!(
        "{DIM}{:<56} {:>12}  {}{RESET}\n",
        "DESTINATION TOPIC - PARTITION", "NEXT OFFSET", "STATUS"
    ));

    for key in &status.order {
        if let Some(line) = status.lines.get(key) {
            let offset = line
                .next_offset
                .map(|offset| offset.to_string())
                .unwrap_or_else(|| "-".to_owned());
            let destination = format!("{} - {}", line.destination_topic, line.partition);
            let (status_label, color, detail) = match line.last_error.as_deref() {
                Some(error) => ("error", RED, error),
                None if line.next_offset.is_some() => ("ok", GREEN, ""),
                None => ("starting", YELLOW, ""),
            };
            output.push_str(&format!(
                "{:<56} {:>12}  {}{:<8}{}",
                destination, offset, color, status_label, RESET
            ));
            if !detail.is_empty() {
                output.push_str(&format!("  {DIM}{detail}{RESET}"));
            }
            output.push('\n');
        }
    }

    output
}

fn status_key(destination_topic: &str, partition: i32) -> String {
    format!("{destination_topic}:{partition}")
}

fn build_consumer(config: &SourceKafkaConfig) -> Result<StreamConsumer> {
    build_source_consumer(config, false)
}

fn build_source_consumer(
    config: &SourceKafkaConfig,
    partition_eof: bool,
) -> Result<StreamConsumer> {
    let mut client = ClientConfig::new();
    let group_id = source_consumer_group_id(config);
    client
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("group.id", group_id);

    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }

    apply_security_config(&mut client, &config.security_protocol, config.sasl.as_ref())?;

    for (key, value) in &config.properties {
        client.set(key, value);
    }

    if partition_eof {
        client.set("enable.partition.eof", "true");
    }

    client.create().context("failed to create Kafka consumer")
}

fn build_dump_consumer(config: &SourceKafkaConfig) -> Result<StreamConsumer> {
    let mut client = ClientConfig::new();
    let group_id = source_consumer_group_id(config);
    client
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("group.id", group_id);

    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }
    apply_security_config(&mut client, &config.security_protocol, config.sasl.as_ref())?;
    for (key, value) in &config.properties {
        client.set(key, value);
    }
    client
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true");
    client
        .create()
        .context("failed to create Kafka dump consumer")
}

fn source_consumer_group_id(config: &SourceKafkaConfig) -> &str {
    &config.group_id
}

fn build_admin_client(
    config: &DestinationKafkaConfig,
) -> Result<AdminClient<DefaultClientContext>> {
    let mut client = ClientConfig::new();
    client.set("bootstrap.servers", &config.bootstrap_servers);

    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }

    apply_security_config(&mut client, &config.security_protocol, config.sasl.as_ref())?;

    for (key, value) in &config.properties {
        client.set(key, value);
    }

    client
        .create()
        .context("failed to create Kafka admin client")
}

fn build_producer(config: &DestinationKafkaConfig) -> Result<FutureProducer> {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("compression.type", "lz4");

    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }

    apply_security_config(&mut client, &config.security_protocol, config.sasl.as_ref())?;

    for (key, value) in &config.properties {
        client.set(key, value);
    }

    client.create().context("failed to create Kafka producer")
}

fn build_stream_producer(config: &DestinationKafkaConfig) -> Result<FutureProducer> {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("compression.type", "lz4");

    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }

    apply_security_config(&mut client, &config.security_protocol, config.sasl.as_ref())?;

    for (key, value) in &config.properties {
        client.set(key, value);
    }

    // These settings are the stream contract, so raw destination options must
    // not weaken them.
    client
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.send.max.retries", i32::MAX.to_string())
        .set("message.timeout.ms", "0")
        .set("max.in.flight.requests.per.connection", "5");

    client
        .create()
        .context("failed to create Kafka stream producer")
}

async fn reconcile_destination_topics(
    sources: &HashMap<String, SourceKafkaConfig>,
    destination: &DestinationKafkaConfig,
    topics: &[ManagedTopic],
    transfer_topics: &HashMap<String, Vec<TransferPlan>>,
    runtime: &RuntimeContext,
    force: bool,
) -> Result<()> {
    let admin_client = build_admin_client(destination)?;
    let metadata_consumer = build_consumer_for_destination_metadata(destination)?;
    let destination_metadata = metadata_consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .context("failed to fetch destination metadata")?;

    let source_partition_counts = fetch_source_partition_counts(sources, transfer_topics)?;

    let mut plans = Vec::with_capacity(topics.len());
    let mut force_required = Vec::new();

    for (topic_index, topic) in topics.iter().enumerate() {
        let desired_partitions = desired_partition_count(topic, &source_partition_counts)?;
        let restore_fingerprint = if let Some(restore) = &topic.restore {
            let reader = ArchiveReader::open(&restore.archive)?;
            let archive_partitions = i32::try_from(reader.partitions().len())
                .context("archive partition count does not fit Kafka")?;
            if archive_partitions != desired_partitions {
                bail!(
                    "restore archive {} has {} partitions but destination topic {} configures {}",
                    restore.archive.display(),
                    archive_partitions,
                    topic.name,
                    desired_partitions
                );
            }
            Some(archive::fingerprint(&restore.archive)?)
        } else {
            None
        };

        let existing = destination_metadata
            .topics()
            .iter()
            .find(|metadata_topic| metadata_topic.name() == topic.name);
        if let Some(error) = existing.and_then(MetadataTopic::error) {
            bail!(
                "destination topic {} metadata error: {:?}",
                topic.name,
                error
            );
        }
        let existing_topic_id = if existing.is_some() {
            let topic_id = kafka_identity::topic_id(metadata_consumer.client(), &topic.name)
                .with_context(|| format!("failed to identify destination topic {}", topic.name))?;
            runtime
                .destination_topic_ids
                .lock()
                .await
                .insert(topic.name.clone(), topic_id.clone());
            Some(topic_id)
        } else {
            None
        };
        let mismatch_reason = if topic
            .static_topic
            .as_ref()
            .is_some_and(|static_topic| static_topic.kind == StaticTopicKind::Empty)
            && existing.is_some()
        {
            Some("configured empty topic must be recreated".to_owned())
        } else if let Some(existing) = existing {
            topic_mismatch_reason(&admin_client, topic, desired_partitions, existing)
                .await
                .with_context(|| {
                    format!(
                        "failed to compare configuration for destination topic {}",
                        topic.name
                    )
                })?
        } else {
            None
        };
        let restore_complete = if let (Some(fingerprint), Some(topic_id)) =
            (restore_fingerprint.as_deref(), existing_topic_id.as_deref())
        {
            let state = runtime.state.lock().await;
            state.restore_matches(&topic.name, topic_id, fingerprint)
        } else {
            topic.restore.is_none()
        };
        let needs_restore = topic.restore.is_some() && !restore_complete;
        let clone_state_mismatch = if let Some(transfer) = topic
            .transfer
            .as_ref()
            .filter(|transfer| transfer.kind == TransferKind::Clone)
        {
            if let Some(topic_id) = existing_topic_id.as_deref() {
                let state = runtime.state.lock().await;
                let configured_source = runtime
                    .source_identities
                    .get(&transfer.source.to_string())
                    .context("missing configured source identity")?;
                clone_state_mismatch_reason(
                    state.topics.get(&topic.name),
                    topic_id,
                    configured_source,
                )
            } else {
                None
            }
        } else {
            None
        };
        let reason = mismatch_reason
            .or_else(|| {
                needs_restore
                    .then(|| "configured archive has not completed successfully".to_owned())
            })
            .or(clone_state_mismatch);
        let action = plan_reconcile_action(existing.is_some(), reason);
        if let ReconcileAction::Recreate(reason) = &action {
            if !recreation_authorized(force, topic.force) {
                force_required.push(format!("{}: {reason}", topic.name));
            }
        }
        plans.push(ReconcilePlan {
            topic_index,
            desired_partitions,
            restore_fingerprint,
            action,
        });
    }

    if !force_required.is_empty() {
        bail!(
            "destination topic recreation requires --force or topic force: true:\n- {}",
            force_required.join("\n- ")
        );
    }

    for plan in plans {
        let topic = &topics[plan.topic_index];
        let recreation_deadline = match plan.action {
            ReconcileAction::Recreate(_) => Some(Instant::now() + TOPIC_RECONCILIATION_TIMEOUT),
            ReconcileAction::None | ReconcileAction::Create => None,
        };
        match plan.action {
            ReconcileAction::None => continue,
            ReconcileAction::Create => {
                clear_topic_state(runtime, &topic.name).await?;
            }
            ReconcileAction::Recreate(_) => {
                clear_topic_state(runtime, &topic.name).await?;
                delete_destination_topic(
                    &admin_client,
                    &metadata_consumer,
                    &topic.name,
                    recreation_deadline.expect("recreation action has a deadline"),
                )
                .await?;
            }
        }
        create_destination_topic(
            &admin_client,
            &metadata_consumer,
            topic,
            plan.desired_partitions,
            recreation_deadline,
        )
        .await?;
        wait_for_destination_topic_state(
            &metadata_consumer,
            &topic.name,
            Some(plan.desired_partitions),
            recreation_deadline.unwrap_or_else(|| Instant::now() + TOPIC_RECONCILIATION_TIMEOUT),
        )
        .await?;

        let topic_id = kafka_identity::topic_id(metadata_consumer.client(), &topic.name)
            .with_context(|| {
                format!(
                    "failed to identify created destination topic {}",
                    topic.name
                )
            })?;
        runtime
            .destination_topic_ids
            .lock()
            .await
            .insert(topic.name.clone(), topic_id.clone());

        if let Some(transfer) = topic
            .transfer
            .as_ref()
            .filter(|transfer| transfer.kind == TransferKind::Clone)
        {
            let source = runtime
                .source_identities
                .get(&transfer.source.to_string())
                .context("missing configured source identity")?;
            {
                let mut state = runtime.state.lock().await;
                state.topics.insert(
                    topic.name.clone(),
                    TopicState {
                        topic_id: topic_id.clone(),
                        state: TopicModeState::Clone(CloneState {
                            source: source.clone(),
                            next_offsets: BTreeMap::new(),
                        }),
                    },
                );
            }
            runtime.state_dirty.store(true, Ordering::Release);
            flush_state(runtime, true).await?;
        }

        if let (Some(restore), Some(fingerprint)) = (&topic.restore, plan.restore_fingerprint) {
            {
                let mut state = runtime.state.lock().await;
                state.mark_restore(
                    &topic.name,
                    topic_id.clone(),
                    fingerprint.clone(),
                    RestoreStatus::InProgress,
                );
            }
            runtime.state_dirty.store(true, Ordering::Release);
            flush_state(runtime, true).await?;
            restore_archive(&restore.archive, &topic.name, &runtime.producer).await?;
            {
                let mut state = runtime.state.lock().await;
                state.mark_restore(&topic.name, topic_id, fingerprint, RestoreStatus::Complete);
            }
            runtime.state_dirty.store(true, Ordering::Release);
            flush_state(runtime, true).await?;
        }
    }

    Ok(())
}

struct ReconcilePlan {
    topic_index: usize,
    desired_partitions: i32,
    restore_fingerprint: Option<String>,
    action: ReconcileAction,
}

enum ReconcileAction {
    None,
    Create,
    Recreate(String),
}

fn plan_reconcile_action(existing: bool, drift_reason: Option<String>) -> ReconcileAction {
    match (existing, drift_reason) {
        (false, _) => ReconcileAction::Create,
        (true, Some(reason)) => ReconcileAction::Recreate(reason),
        (true, None) => ReconcileAction::None,
    }
}

fn recreation_authorized(command_force: bool, topic_force: bool) -> bool {
    command_force || topic_force
}

async fn clear_topic_state(runtime: &RuntimeContext, topic: &str) -> Result<()> {
    {
        let mut state = runtime.state.lock().await;
        state.clear_topic(topic);
    }
    runtime.state_dirty.store(true, Ordering::Release);
    flush_state(runtime, true).await
}

async fn clear_non_stateful_topic_state(
    topics: &[ManagedTopic],
    runtime: &RuntimeContext,
) -> Result<()> {
    let mut changed = false;
    {
        let mut state = runtime.state.lock().await;
        for topic in topics {
            let is_clone = topic
                .transfer
                .as_ref()
                .is_some_and(|transfer| transfer.kind == TransferKind::Clone);
            if !is_clone && topic.restore.is_none() {
                changed |= state.clear_topic(&topic.name);
            }
        }
    }
    if changed {
        runtime.state_dirty.store(true, Ordering::Release);
        flush_state(runtime, true).await?;
    }
    Ok(())
}

async fn delete_destination_topic(
    admin_client: &AdminClient<DefaultClientContext>,
    metadata_consumer: &StreamConsumer,
    topic: &str,
    deadline: Instant,
) -> Result<()> {
    let timeout = remaining_reconciliation_time(deadline, topic)?;
    let options = AdminOptions::new()
        .request_timeout(Some(timeout))
        .operation_timeout(Some(timeout));
    let results = admin_client
        .delete_topics(&[topic], &options)
        .await
        .with_context(|| format!("failed to delete destination topic {topic}"))?;
    match results.into_iter().next() {
        Some(Ok(_)) => {}
        Some(Err((name, code))) => bail!("failed to delete destination topic {name}: {code}"),
        None => bail!("Kafka returned no delete result for destination topic {topic}"),
    }
    wait_for_destination_topic_state(metadata_consumer, topic, None, deadline).await
}

async fn create_destination_topic(
    admin_client: &AdminClient<DefaultClientContext>,
    metadata_consumer: &StreamConsumer,
    topic: &ManagedTopic,
    partitions: i32,
    recreation_deadline: Option<Instant>,
) -> Result<()> {
    let mut new_topic = NewTopic::new(
        topic.name.as_str(),
        partitions,
        TopicReplication::Fixed(topic.replication_factor.unwrap_or(-1)),
    )
    .set("message.timestamp.type", "CreateTime");
    for (key, value) in &topic.config {
        new_topic = new_topic.set(key, value);
    }
    loop {
        let options = if let Some(deadline) = recreation_deadline {
            let timeout = remaining_reconciliation_time(deadline, &topic.name)?;
            AdminOptions::new()
                .request_timeout(Some(timeout))
                .operation_timeout(Some(timeout))
        } else {
            AdminOptions::new()
        };
        let results = admin_client
            .create_topics([&new_topic], &options)
            .await
            .with_context(|| format!("failed to create destination topic {}", topic.name))?;
        match results.into_iter().next() {
            Some(Ok(_)) => return Ok(()),
            Some(Err((name, RDKafkaErrorCode::TopicAlreadyExists)))
                if recreation_deadline.is_some() =>
            {
                let deadline = recreation_deadline.expect("guarded by is_some");
                match create_collision_action(destination_topic_presence(
                    metadata_consumer,
                    &topic.name,
                    deadline,
                )?) {
                    CreateCollisionAction::FailConcurrent => bail!(
                        "destination topic {name} was recreated by another client during reconciliation; stop destination applications or disable Kafka topic auto-creation before retrying"
                    ),
                    CreateCollisionAction::Retry => {
                        sleep_until_reconciliation_retry(deadline, &topic.name).await?;
                    }
                }
            }
            Some(Err((name, code))) => {
                bail!("failed to create destination topic {name}: {code}")
            }
            None => bail!(
                "Kafka returned no create result for destination topic {}",
                topic.name
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestinationTopicPresence {
    Missing,
    Live,
    Errored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateCollisionAction {
    Retry,
    FailConcurrent,
}

fn create_collision_action(presence: DestinationTopicPresence) -> CreateCollisionAction {
    match presence {
        DestinationTopicPresence::Missing | DestinationTopicPresence::Errored => {
            CreateCollisionAction::Retry
        }
        DestinationTopicPresence::Live => CreateCollisionAction::FailConcurrent,
    }
}

fn classify_destination_topic_presence(found: bool, has_error: bool) -> DestinationTopicPresence {
    match (found, has_error) {
        (false, _) => DestinationTopicPresence::Missing,
        (true, false) => DestinationTopicPresence::Live,
        (true, true) => DestinationTopicPresence::Errored,
    }
}

fn destination_topic_presence(
    metadata_consumer: &StreamConsumer,
    topic: &str,
    deadline: Instant,
) -> Result<DestinationTopicPresence> {
    let timeout = remaining_reconciliation_time(deadline, topic)?.min(Duration::from_secs(2));
    let metadata = metadata_consumer
        .fetch_metadata(None, timeout)
        .with_context(|| format!("failed to refresh destination metadata for {topic}"))?;
    let metadata_topic = metadata.topics().iter().find(|item| item.name() == topic);
    Ok(classify_destination_topic_presence(
        metadata_topic.is_some(),
        metadata_topic.and_then(MetadataTopic::error).is_some(),
    ))
}

fn remaining_reconciliation_time(deadline: Instant, topic: &str) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| anyhow!("timed out reconciling destination topic {topic}"))
}

async fn sleep_until_reconciliation_retry(deadline: Instant, topic: &str) -> Result<()> {
    let remaining = remaining_reconciliation_time(deadline, topic)?;
    tokio::time::sleep(remaining.min(TOPIC_RECONCILIATION_POLL_INTERVAL)).await;
    remaining_reconciliation_time(deadline, topic).map(|_| ())
}

async fn wait_for_destination_topic_state(
    metadata_consumer: &StreamConsumer,
    topic: &str,
    expected_partitions: Option<i32>,
    deadline: Instant,
) -> Result<()> {
    loop {
        let timeout = remaining_reconciliation_time(deadline, topic)?.min(Duration::from_secs(2));
        let metadata = metadata_consumer
            .fetch_metadata(None, timeout)
            .with_context(|| format!("failed to refresh destination metadata for {topic}"))?;
        let metadata_topic = metadata.topics().iter().find(|item| item.name() == topic);
        let presence = classify_destination_topic_presence(
            metadata_topic.is_some(),
            metadata_topic.and_then(MetadataTopic::error).is_some(),
        );
        let live_partitions = metadata_topic
            .filter(|_| presence == DestinationTopicPresence::Live)
            .map(|item| item.partitions().len() as i32);
        let ready = match expected_partitions {
            Some(expected) => live_partitions == Some(expected),
            None => presence == DestinationTopicPresence::Missing,
        };
        if ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            match expected_partitions {
                Some(expected) => bail!(
                    "timed out waiting for destination topic {topic} to have {expected} partitions; observed {live_partitions:?}"
                ),
                None => bail!("timed out waiting for destination topic {topic} to be deleted"),
            }
        }
        sleep_until_reconciliation_retry(deadline, topic).await?;
    }
}

async fn restore_archive(path: &Path, topic: &str, producer: &FutureProducer) -> Result<()> {
    const MAX_IN_FLIGHT_PER_PARTITION: usize = 1024;

    let mut reader = ArchiveReader::open(path)?;
    let mut partition = None;
    let mut inflight = FuturesOrdered::new();
    while let Some(event) = reader.next_event()? {
        match event {
            ArchiveEvent::PartitionStart(id) => partition = Some(id),
            ArchiveEvent::PartitionEnd(id) => {
                if partition.take() != Some(id) {
                    bail!("archive partition boundary mismatch for partition {id}");
                }
                while let Some(result) = inflight.next().await {
                    result?;
                }
            }
            ArchiveEvent::Record(record) => {
                let partition = partition.context("archive record appeared outside a partition")?;
                inflight.push_back(send_archive_record(
                    producer.clone(),
                    topic.to_owned(),
                    partition,
                    record,
                ));
                if inflight.len() >= MAX_IN_FLIGHT_PER_PARTITION {
                    inflight
                        .next()
                        .await
                        .context("restore delivery queue ended unexpectedly")??;
                }
            }
        }
    }
    if partition.is_some() {
        bail!("archive ended inside a partition");
    }
    Ok(())
}

async fn send_archive_record(
    producer: FutureProducer,
    topic: String,
    partition: i32,
    record: ArchiveRecord,
) -> Result<()> {
    let mut future_record = FutureRecord::to(&topic).partition(partition);
    if let Some(payload) = record.payload.as_deref() {
        future_record = future_record.payload(payload);
    }
    if let Some(key) = record.key.as_deref() {
        future_record = future_record.key(key);
    }
    if let Some(timestamp) = record.timestamp {
        future_record = future_record.timestamp(timestamp);
    }
    if !record.headers.is_empty() {
        let mut headers = OwnedHeaders::new_with_capacity(record.headers.len());
        for header in &record.headers {
            headers = headers.insert(Header {
                key: &header.key,
                value: header.value.as_deref(),
            });
        }
        future_record = future_record.headers(headers);
    }
    producer
        .send(future_record, Duration::from_secs(30))
        .await
        .map(|_| ())
        .map_err(|(error, _)| {
            anyhow!("restore delivery failed for {topic} partition {partition}: {error}")
        })
}

fn fetch_source_partition_counts(
    sources: &HashMap<String, SourceKafkaConfig>,
    transfer_topics: &HashMap<String, Vec<TransferPlan>>,
) -> Result<HashMap<String, i32>> {
    let mut counts = HashMap::new();

    for (source_name, plans) in transfer_topics {
        if plans.is_empty() {
            continue;
        }
        let source_config = sources
            .get(source_name)
            .ok_or_else(|| anyhow!("missing source configuration for {source_name}"))?;
        let consumer = build_consumer(source_config)?;
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .with_context(|| {
                format!(
                    "failed to fetch source metadata for Kafka source {}",
                    source_name
                )
            })?;

        let mut seen_topics = HashMap::new();
        for plan in plans {
            if seen_topics.insert(plan.source.topic.as_str(), ()).is_some() {
                continue;
            }

            let metadata_topic = metadata
                .topics()
                .iter()
                .find(|topic| topic.name() == plan.source.topic)
                .ok_or_else(|| {
                    anyhow!(
                        "source topic {} not found in metadata for source {}",
                        plan.source.topic,
                        source_name
                    )
                })?;
            if let Some(error) = metadata_topic.error() {
                bail!(
                    "source topic {} metadata error on source {}: {:?}",
                    plan.source.topic,
                    source_name,
                    error
                );
            }

            counts.insert(
                format!("{source_name}:{topic}", topic = plan.source.topic),
                metadata_topic.partitions().len() as i32,
            );
        }
    }

    Ok(counts)
}

fn fetch_clone_end_offsets(
    sources: &HashMap<String, SourceKafkaConfig>,
    clone_topics: &HashMap<String, Vec<TransferPlan>>,
) -> Result<CloneEndOffsets> {
    let mut boundaries = HashMap::new();
    for (source_name, plans) in clone_topics {
        let source_config = sources
            .get(source_name)
            .ok_or_else(|| anyhow!("missing source configuration for {source_name}"))?;
        let consumer = build_consumer(source_config)?;
        let metadata = fetch_metadata(&consumer, plans)?;
        for plan in plans {
            let topic = metadata
                .topics()
                .iter()
                .find(|topic| topic.name() == plan.source.topic)
                .ok_or_else(|| {
                    anyhow!("missing metadata for source topic {}", plan.source.topic)
                })?;
            for partition in topic.partitions() {
                let (_, high) = consumer
                    .fetch_watermarks(&plan.source.topic, partition.id(), Duration::from_secs(10))
                    .with_context(|| {
                        format!(
                            "failed to fetch startup watermark for {}:{} partition {}",
                            source_name,
                            plan.source.topic,
                            partition.id()
                        )
                    })?;
                boundaries.insert(
                    clone_boundary_key(source_name, &plan.source.topic, partition.id()),
                    high,
                );
            }
        }
    }
    Ok(boundaries)
}

fn clone_boundary_key(source: &str, topic: &str, partition: i32) -> String {
    format!("{source}:{topic}:{partition}")
}

fn desired_partition_count(
    topic: &ManagedTopic,
    source_partition_counts: &HashMap<String, i32>,
) -> Result<i32> {
    if let Some(transfer) = &topic.transfer {
        let key = format!("{}:{}", transfer.source.instance, transfer.source.topic);
        if let Some(count) = source_partition_counts.get(&key) {
            return Ok(*count);
        }
        bail!(
            "missing source partition count for transfer {} to destination topic {}",
            key,
            topic.name
        );
    }

    if let Some(restore) = &topic.restore {
        let reader = ArchiveReader::open(&restore.archive)?;
        return i32::try_from(reader.partitions().len())
            .context("archive partition count does not fit Kafka");
    }

    topic
        .static_topic
        .as_ref()
        .map(|static_topic| static_topic.partitions)
        .ok_or_else(|| anyhow!("destination topic {} has no partition source", topic.name))
}

async fn topic_mismatch_reason(
    admin_client: &AdminClient<DefaultClientContext>,
    topic: &ManagedTopic,
    desired_partitions: i32,
    live_topic: &MetadataTopic,
) -> Result<Option<String>> {
    let live_partition_count = live_topic.partitions().len() as i32;
    if live_partition_count != desired_partitions {
        return Ok(Some(format!(
            "partition count is {} but expected {}",
            live_partition_count, desired_partitions
        )));
    }

    if let Some(expected) = topic.replication_factor {
        if let Some(partition) = live_topic
            .partitions()
            .iter()
            .find(|partition| partition.replicas().len() as i32 != expected)
        {
            return Ok(Some(format!(
                "partition {} has replication factor {} but expected {}",
                partition.id(),
                partition.replicas().len(),
                expected
            )));
        }
    }

    let specifier = ResourceSpecifier::Topic(topic.name.as_str());
    let resources = admin_client
        .describe_configs([&specifier], &AdminOptions::new())
        .await
        .context("describe_configs failed")?;
    let resource = resources
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("missing describe_configs result for topic {}", topic.name))?
        .map_err(|code| {
            anyhow!(
                "describe_configs returned error for topic {}: {}",
                topic.name,
                code
            )
        })?;
    let entries = resource.entry_map();

    let timestamp_type = entries
        .get("message.timestamp.type")
        .and_then(|entry| entry.value.as_deref());
    if timestamp_type != Some("CreateTime") {
        return Ok(Some(format!(
            "config message.timestamp.type is {:?} but expected \"CreateTime\"",
            timestamp_type
        )));
    }

    for (key, expected_value) in &topic.config {
        let actual_value = entries
            .get(key.as_str())
            .and_then(|entry| entry.value.as_deref());
        if actual_value != Some(expected_value.as_str()) {
            return Ok(Some(format!(
                "config {} is {:?} but expected {:?}",
                key, actual_value, expected_value
            )));
        }
    }

    Ok(None)
}

fn apply_security_config(
    client: &mut ClientConfig,
    security_protocol: &Option<String>,
    sasl: Option<&SaslConfig>,
) -> Result<()> {
    if let Some(protocol) = security_protocol {
        client.set("security.protocol", protocol);
    }

    if let Some(sasl) = sasl {
        let password = env::var(&sasl.password_env).with_context(|| {
            format!(
                "failed to read SASL password from environment variable {}",
                sasl.password_env
            )
        })?;
        client
            .set("sasl.mechanism", &sasl.mechanism)
            .set("sasl.username", &sasl.username)
            .set("sasl.password", password);
    }

    Ok(())
}

fn fetch_metadata(consumer: &StreamConsumer, plans: &[TransferPlan]) -> Result<Metadata> {
    let topic_names: Vec<&str> = plans
        .iter()
        .map(|plan| plan.source.topic.as_str())
        .collect();

    consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .context("failed to fetch Kafka metadata")
        .and_then(|metadata| {
            for topic in &topic_names {
                let item = metadata
                    .topics()
                    .iter()
                    .find(|item| item.name() == *topic)
                    .ok_or_else(|| anyhow!("source topic {topic} not found in cluster metadata"))?;
                if let Some(error) = item.error() {
                    bail!("source topic {topic} metadata error: {error:?}");
                }
            }
            Ok(metadata)
        })
}

fn build_assignment(
    metadata: &Metadata,
    plans: &[TransferPlan],
    state: &OffsetState,
    end_offsets: &CloneEndOffsets,
) -> Result<TopicPartitionList> {
    let mut assignment = TopicPartitionList::new();

    for plan in plans {
        let metadata_topic = metadata
            .topics()
            .iter()
            .find(|topic| topic.name() == plan.source.topic)
            .ok_or_else(|| anyhow!("missing metadata for source topic {}", plan.source.topic))?;

        for partition in metadata_topic.partitions() {
            let saved = if plan.kind == TransferKind::Clone {
                state.next_offset(&plan.destination_topic, partition.id())
            } else {
                None
            };
            let end_offset = end_offsets.get(&clone_boundary_key(
                &plan.source.instance,
                &plan.source.topic,
                partition.id(),
            ));
            if let (Some(saved), Some(end)) = (saved, end_offset) {
                if saved > *end {
                    bail!(
                        "saved offset {} is beyond captured end offset {} for {}:{} partition {}",
                        saved,
                        end,
                        plan.source.instance,
                        plan.source.topic,
                        partition.id()
                    );
                }
            }
            let offset = initial_offset(plan, saved);

            assignment
                .add_partition_offset(&plan.source.topic, partition.id(), offset)
                .with_context(|| {
                    format!(
                        "failed to assign {}:{} partition {}",
                        plan.source.instance,
                        plan.source.topic,
                        partition.id()
                    )
                })?;
        }
    }

    Ok(assignment)
}

fn initial_offset(plan: &TransferPlan, saved: Option<i64>) -> Offset {
    match plan.kind {
        TransferKind::Clone => saved.map(Offset::Offset).unwrap_or(Offset::Beginning),
        TransferKind::Stream => Offset::End,
    }
}

fn build_consumer_for_destination_metadata(
    config: &DestinationKafkaConfig,
) -> Result<StreamConsumer> {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("group.id", "fransson-destination-metadata");

    if let Some(client_id) = &config.client_id {
        client.set("client.id", client_id);
    }

    apply_security_config(&mut client, &config.security_protocol, config.sasl.as_ref())?;

    for (key, value) in &config.properties {
        client.set(key, value);
    }

    client
        .create()
        .context("failed to create destination metadata consumer")
}

fn default_max_in_flight_per_partition() -> usize {
    64
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use rdkafka::mocking::MockCluster;

    fn app_config_yaml(topic_fields: &str) -> String {
        format!(
            r#"sources:
  primary:
    bootstrap_servers: localhost:9092
    group_id: primary-test
destination:
  bootstrap_servers: localhost:9093
topics:
  - name: destination
{topic_fields}
"#
        )
    }

    fn test_source_identity() -> SourceIdentity {
        SourceIdentity {
            cluster_id: "source-cluster".to_owned(),
            topic: "source".to_owned(),
            topic_id: "source-topic-id".to_owned(),
        }
    }

    fn mock_consumer(bootstrap_servers: &str, group: &str) -> StreamConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .create()
            .unwrap()
    }

    fn mock_producer(bootstrap_servers: &str) -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.idempotence", "true")
            .create()
            .unwrap()
    }

    async fn snapshot_topic(bootstrap_servers: &str, topic: &str, output: &Path) -> Result<()> {
        let consumer = mock_consumer(bootstrap_servers, &format!("snapshot-{topic}"));
        let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(5))?;
        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|item| item.name() == topic)
            .unwrap();
        let mut partitions: Vec<i32> = topic_metadata
            .partitions()
            .iter()
            .map(|partition| partition.id())
            .collect();
        partitions.sort_unstable();
        let mut watermarks = HashMap::new();
        for partition in &partitions {
            watermarks.insert(
                *partition,
                consumer.fetch_watermarks(topic, *partition, Duration::from_secs(5))?,
            );
        }
        tokio::time::timeout(
            Duration::from_secs(10),
            dump_topic_to_path(
                &consumer,
                topic,
                &partitions,
                &watermarks,
                "snapshot-test",
                output,
            ),
        )
        .await
        .context("snapshot timed out")?
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "librdkafka mock cluster requires local sockets"]
    async fn kafka_dump_restore_dump_is_byte_identical() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("source", 2, 1).unwrap();
        cluster.create_topic("destination", 2, 1).unwrap();
        let bootstrap_servers = cluster.bootstrap_servers();
        let producer = mock_producer(&bootstrap_servers);

        let key = b"key";
        let payload = b"payload";
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "duplicate",
                value: Some(&b"one"[..]),
            })
            .insert(Header {
                key: "duplicate",
                value: None::<&[u8]>,
            });
        producer
            .send(
                FutureRecord::to("source")
                    .partition(1)
                    .key(key)
                    .payload(payload)
                    .timestamp(1234)
                    .headers(headers),
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        let directory = std::env::temp_dir();
        let first = directory.join(format!("fransson-kafka-{}-1", std::process::id()));
        let second = directory.join(format!("fransson-kafka-{}-2", std::process::id()));
        snapshot_topic(&bootstrap_servers, "source", &first)
            .await
            .unwrap();
        restore_archive(&first, "destination", &producer)
            .await
            .unwrap();
        snapshot_topic(&bootstrap_servers, "destination", &second)
            .await
            .unwrap();
        assert_eq!(fs::read(&first).unwrap(), fs::read(&second).unwrap());
        let _ = fs::remove_file(first);
        let _ = fs::remove_file(second);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "librdkafka mock cluster requires local sockets"]
    async fn kafka_stream_assignment_skips_existing_records() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("source", 1, 1).unwrap();
        let bootstrap_servers = cluster.bootstrap_servers();
        let producer = mock_producer(&bootstrap_servers);
        producer
            .send(
                FutureRecord::to("source")
                    .partition(0)
                    .key(&b"key"[..])
                    .payload(&b"before"[..]),
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        let consumer = Arc::new(mock_consumer(&bootstrap_servers, "stream-end-test"));
        let plan = TransferPlan {
            destination_topic: "destination".to_owned(),
            source: SourceTopicRef {
                instance: "primary".to_owned(),
                topic: "source".to_owned(),
            },
            kind: TransferKind::Stream,
        };
        let metadata = fetch_metadata(consumer.as_ref(), std::slice::from_ref(&plan)).unwrap();
        let mut state = OffsetState::default();
        state.update_next_offset(
            "destination",
            "destination-topic-id",
            &test_source_identity(),
            0,
            0,
        );
        let assignment = build_assignment(
            &metadata,
            std::slice::from_ref(&plan),
            &state,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            assignment.find_partition("source", 0).unwrap().offset(),
            Offset::End
        );
        consumer.assign(&assignment).unwrap();
        let queue = consumer.split_partition_queue("source", 0).unwrap();
        let eof = tokio::time::timeout(Duration::from_secs(10), queue.recv())
            .await
            .expect("stream never reached its startup end")
            .unwrap_err();
        assert!(matches!(eof, rdkafka::error::KafkaError::PartitionEOF(0)));

        producer
            .send(
                FutureRecord::to("source")
                    .partition(0)
                    .key(&b"key"[..])
                    .payload(&b"after"[..]),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(10), queue.recv())
            .await
            .expect("stream did not receive a new record")
            .unwrap();
        assert_eq!(message.payload(), Some(&b"after"[..]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "librdkafka mock cluster requires local sockets"]
    async fn kafka_stream_producer_waits_through_destination_outage() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("destination", 1, 1).unwrap();
        let bootstrap_servers = cluster.bootstrap_servers();
        let config = DestinationKafkaConfig {
            bootstrap_servers,
            client_id: None,
            security_protocol: None,
            sasl: None,
            properties: BTreeMap::new(),
        };
        let producer = build_stream_producer(&config).unwrap();
        producer
            .send(
                FutureRecord::to("destination")
                    .partition(0)
                    .key(&b"key"[..])
                    .payload(&b"warmup"[..]),
                Timeout::Never,
            )
            .await
            .unwrap();

        cluster.broker_down(1).unwrap();
        let delivery_producer = producer.clone();
        let delivery = tokio::spawn(async move {
            delivery_producer
                .send(
                    FutureRecord::to("destination")
                        .partition(0)
                        .key(&b"key"[..])
                        .payload(&b"after-outage"[..]),
                    Timeout::Never,
                )
                .await
                .map(|_| ())
                .map_err(|(error, _)| error.to_string())
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!delivery.is_finished());

        cluster.broker_up(1).unwrap();
        tokio::time::timeout(Duration::from_secs(10), delivery)
            .await
            .expect("stream delivery did not recover")
            .expect("stream delivery task panicked")
            .expect("stream delivery failed");
    }

    #[test]
    fn clone_rejects_removed_start_offset() {
        for offset in ["earliest", "latest"] {
            let error = serde_yaml::from_str::<AppConfig>(&app_config_yaml(&format!(
                "    clone:\n      source: primary:source\n      start_offset: {offset}"
            )))
            .unwrap_err();
            assert!(error.to_string().contains("unknown field `start_offset`"));
        }
    }

    #[test]
    fn topic_modes_are_required_and_mutually_exclusive() {
        let modes = [
            "    manage:\n      partitions: 1",
            "    empty:\n      partitions: 1",
            "    clone:\n      source: primary:source",
            "    stream:\n      source: primary:source",
            "    restore:\n      archive: dump.zst",
        ];
        for (index, first) in modes.iter().enumerate() {
            for second in &modes[index + 1..] {
                let topic_fields = format!("{first}\n{second}");
                let config: AppConfig =
                    serde_yaml::from_str(&app_config_yaml(&topic_fields)).unwrap();
                let error = resolve_topics(&config, Path::new("config.yaml")).unwrap_err();
                assert!(error.to_string().contains("must configure exactly one"));
            }
        }

        let config: AppConfig = serde_yaml::from_str(&app_config_yaml("")).unwrap();
        assert!(resolve_topics(&config, Path::new("config.yaml"))
            .unwrap_err()
            .to_string()
            .contains("must configure exactly one"));
    }

    #[test]
    fn stream_is_active_only_for_run() {
        let config: AppConfig = serde_yaml::from_str(&app_config_yaml(
            "    stream:\n      source: primary:source",
        ))
        .unwrap();
        let topics = resolve_topics(&config, Path::new("config.yaml")).unwrap();
        let run_topics = collect_transfer_topics_by_source(&topics);
        let restore_topics = collect_clone_topics_by_source(&topics);

        assert_eq!(run_topics["primary"].len(), 1);
        assert_eq!(run_topics["primary"][0].kind, TransferKind::Stream);
        assert!(restore_topics.is_empty());
    }

    #[test]
    fn clone_defaults_to_beginning_and_stream_to_end() {
        let config: AppConfig = serde_yaml::from_str(&app_config_yaml(
            "    stream:\n      source: primary:source",
        ))
        .unwrap();
        let topics = resolve_topics(&config, Path::new("config.yaml")).unwrap();
        let stream = topics[0].transfer.as_ref().unwrap();
        assert_eq!(initial_offset(stream, None), Offset::End);

        let config: AppConfig =
            serde_yaml::from_str(&app_config_yaml("    clone:\n      source: primary:source"))
                .unwrap();
        let topics = resolve_topics(&config, Path::new("config.yaml")).unwrap();
        let clone = topics[0].transfer.as_ref().unwrap();
        assert_eq!(initial_offset(clone, None), Offset::Beginning);
        assert_eq!(initial_offset(clone, Some(42)), Offset::Offset(42));
    }

    #[test]
    fn clearing_a_destination_removes_old_state() {
        let mut state = OffsetState::default();
        state.update_next_offset(
            "destination",
            "destination-topic-id",
            &test_source_identity(),
            0,
            42,
        );
        state.mark_restore(
            "destination",
            "destination-topic-id".to_owned(),
            "fingerprint".to_owned(),
            RestoreStatus::Complete,
        );

        assert!(state.clear_topic("destination"));
        assert!(state.next_offset("destination", 0).is_none());
        assert!(!state.restore_matches("destination", "destination-topic-id", "fingerprint"));
    }

    #[test]
    fn source_uses_explicit_group_id() {
        let config = SourceKafkaConfig {
            bootstrap_servers: "localhost:9092".to_owned(),
            client_id: None,
            group_id: "authorized-group".to_owned(),
            max_in_flight_per_partition: 64,
            security_protocol: None,
            sasl: None,
            properties: BTreeMap::new(),
        };
        assert_eq!(source_consumer_group_id(&config), "authorized-group");
    }

    #[test]
    fn cli_commands_use_the_final_archive_and_force_options() {
        let cli = Cli::try_parse_from([
            "fransson",
            "dump",
            "--config",
            "config.yaml",
            "--source",
            "primary:source",
            "--archive",
            "dump.fransson.zst",
            "--force",
        ])
        .unwrap();
        let Command::Dump(args) = cli.command else {
            panic!("expected dump command");
        };
        assert_eq!(args.archive, PathBuf::from("dump.fransson.zst"));
        assert!(args.force);

        assert!(Cli::try_parse_from([
            "fransson",
            "dump",
            "--config",
            "config.yaml",
            "--source",
            "primary:source",
            "--output",
            "dump.zst",
        ])
        .is_err());

        let cli = Cli::try_parse_from([
            "fransson",
            "restore",
            "--config",
            "config.yaml",
            "--state-dir",
            "/var/lib/fransson",
            "--force",
        ])
        .unwrap();
        assert!(matches!(cli.command, Command::Restore(args)
            if args.force && args.state_dir == Path::new("/var/lib/fransson")));

        let cli = Cli::try_parse_from(["fransson", "state", "show"]).unwrap();
        assert!(matches!(cli.command, Command::State(StateArgs {
            command: StateCommand::Show(StateShowArgs { state_dir }),
        }) if state_dir == Path::new(".fransson")));

        assert!(
            Cli::try_parse_from(["fransson", "state", "reset", "--config", "config.yaml"]).is_err()
        );
    }

    #[test]
    fn yaml_rejects_removed_public_fields() {
        let removed = [
            format!(
                "state_file: .state/fransson.json\n{}",
                app_config_yaml("    manage:\n      partitions: 1")
            ),
            app_config_yaml("    max_message_bytes: 1048576\n    partitions: 1"),
            app_config_yaml("    restore:\n      file: dump.zst"),
            app_config_yaml("    options:\n      cleanup.policy: compact\n    partitions: 1"),
            r#"sources:
  primary:
    bootstrap_servers: localhost:9092
    consumer_group_id: old
"#
            .to_owned(),
        ];
        for yaml in removed {
            let error = serde_yaml::from_str::<AppConfig>(&yaml).unwrap_err();
            assert!(error.to_string().contains("unknown field"), "{error}");
        }
    }

    #[test]
    fn manage_and_empty_own_their_partition_count() {
        for (mode, kind) in [
            ("manage", StaticTopicKind::Manage),
            ("empty", StaticTopicKind::Empty),
        ] {
            let fields = format!("    {mode}:\n      partitions: 3");
            let config: AppConfig = serde_yaml::from_str(&app_config_yaml(&fields)).unwrap();
            validate_config(&config).unwrap();
            let topics = resolve_topics(&config, Path::new("config.yaml")).unwrap();
            let static_topic = topics[0].static_topic.as_ref().unwrap();
            assert_eq!(static_topic.partitions, 3);
            assert_eq!(static_topic.kind, kind);
            assert!(topics[0].transfer.is_none());
            assert!(topics[0].restore.is_none());

            let fields = format!("    {mode}:\n      partitions: 0");
            let config: AppConfig = serde_yaml::from_str(&app_config_yaml(&fields)).unwrap();
            assert!(validate_config(&config)
                .unwrap_err()
                .to_string()
                .contains(&format!("{mode}.partitions must be greater than zero")));
        }

        let missing = serde_yaml::from_str::<AppConfig>(&app_config_yaml("    empty: {}"));
        assert!(missing.is_err());

        let obsolete = serde_yaml::from_str::<AppConfig>(&app_config_yaml(
            "    partitions: 1\n    manage:\n      partitions: 1",
        ));
        assert!(obsolete
            .unwrap_err()
            .to_string()
            .contains("unknown field `partitions`"));
    }

    #[test]
    fn raw_properties_cannot_override_fransson_fields() {
        let config: AppConfig = serde_yaml::from_str(
            r#"sources:
  primary:
    bootstrap_servers: localhost:9092
    group_id: group
    properties:
      group.id: another-group
"#,
        )
        .unwrap();
        assert!(validate_config(&config)
            .unwrap_err()
            .to_string()
            .contains("property group.id is owned by fransson"));

        let config: AppConfig = serde_yaml::from_str(
            r#"destination:
  bootstrap_servers: localhost:9092
  properties:
    enable.idempotence: "false"
"#,
        )
        .unwrap();
        assert!(validate_config(&config)
            .unwrap_err()
            .to_string()
            .contains("property enable.idempotence is owned by fransson"));
    }

    #[test]
    fn state_schema_is_strict_and_binds_clone_source() {
        let mut state = OffsetState::default();
        let mut registry = StateRegistry::default();
        state.update_next_offset(
            "destination",
            "destination-topic-id",
            &test_source_identity(),
            0,
            42,
        );
        registry.clusters.insert(
            "destination-cluster".to_owned(),
            ClusterState {
                topics: state.topics.clone(),
            },
        );
        let json = serde_json::to_string(&registry).unwrap();
        let decoded: StateRegistry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.format_version, STATE_FORMAT_VERSION);
        let TopicModeState::Clone(clone) =
            &decoded.clusters["destination-cluster"].topics["destination"].state
        else {
            panic!("expected clone state")
        };
        assert_eq!(clone.source, test_source_identity());
        assert!(!state.restore_matches("destination", "other-topic-id", "fingerprint"));
        assert!(serde_json::from_str::<StateRegistry>(r#"{"clusters":{}}"#).is_err());
    }

    #[test]
    fn clone_state_fails_closed_on_missing_or_mismatched_identity() {
        let source = test_source_identity();
        assert_eq!(
            clone_state_mismatch_reason(None, "destination-id", &source).as_deref(),
            Some("existing clone topic has no trusted state")
        );
        let saved = TopicState {
            topic_id: "old-destination-id".to_owned(),
            state: TopicModeState::Clone(CloneState {
                source: source.clone(),
                next_offsets: BTreeMap::new(),
            }),
        };
        assert!(
            clone_state_mismatch_reason(Some(&saved), "destination-id", &source)
                .unwrap()
                .contains("UUID")
        );
        let trusted = TopicState {
            topic_id: "destination-id".to_owned(),
            state: TopicModeState::Clone(CloneState {
                source: source.clone(),
                next_offsets: BTreeMap::new(),
            }),
        };
        assert_eq!(
            clone_state_mismatch_reason(Some(&trusted), "destination-id", &source),
            None
        );
        let mut other_source = source.clone();
        other_source.topic_id = "replacement-source-id".to_owned();
        assert!(
            clone_state_mismatch_reason(Some(&trusted), "destination-id", &other_source)
                .unwrap()
                .contains("source identity")
        );
    }

    #[test]
    fn restore_state_matches_only_completed_archive_and_topic_identity() {
        let mut state = OffsetState::default();
        state.mark_restore(
            "destination",
            "topic-id".to_owned(),
            "archive-hash".to_owned(),
            RestoreStatus::InProgress,
        );
        assert!(!state.restore_matches("destination", "topic-id", "archive-hash"));
        state.mark_restore(
            "destination",
            "topic-id".to_owned(),
            "archive-hash".to_owned(),
            RestoreStatus::Complete,
        );
        assert!(state.restore_matches("destination", "topic-id", "archive-hash"));
        assert!(!state.restore_matches("destination", "replacement-id", "archive-hash"));
        assert!(!state.restore_matches("destination", "topic-id", "other-hash"));
    }

    #[test]
    fn state_registry_persists_atomically() {
        let directory = env::temp_dir().join(format!(
            "fransson-state-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = directory.join(STATE_FILE_NAME);
        let mut registry = StateRegistry::default();
        registry.clusters.insert(
            "cluster".to_owned(),
            ClusterState {
                topics: BTreeMap::from([(
                    "destination".to_owned(),
                    TopicState {
                        topic_id: "topic-id".to_owned(),
                        state: TopicModeState::Restore(RestoreMarker {
                            archive_sha256: "hash".to_owned(),
                            archive_format_version: archive::format_version(),
                            status: RestoreStatus::InProgress,
                        }),
                    },
                )]),
            },
        );
        registry.persist(&path).unwrap();
        let loaded = StateRegistry::load(&path).unwrap();
        assert_eq!(
            loaded.clusters["cluster"].topics["destination"].topic_id,
            "topic-id"
        );
        assert!(!path.with_extension("tmp").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn concurrent_state_stores_merge_disjoint_topics() {
        let directory = env::temp_dir().join(format!(
            "fransson-state-merge-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let topic = |name: &str| ManagedTopic {
            name: name.to_owned(),
            force: false,
            replication_factor: None,
            config: BTreeMap::new(),
            static_topic: Some(StaticTopicPlan {
                partitions: 1,
                kind: StaticTopicKind::Manage,
            }),
            transfer: None,
            restore: None,
        };
        let (store_a, mut state_a) =
            StateStore::open(directory.clone(), "cluster".to_owned(), &[topic("a")]).unwrap();
        let (store_b, mut state_b) =
            StateStore::open(directory.clone(), "cluster".to_owned(), &[topic("b")]).unwrap();
        let collision = StateStore::open(directory.clone(), "cluster".to_owned(), &[topic("a")])
            .err()
            .expect("same topic lock should fail");
        assert!(collision.to_string().contains("already managed"));
        state_a.mark_restore(
            "a",
            "a-id".to_owned(),
            "a-hash".to_owned(),
            RestoreStatus::Complete,
        );
        store_a.persist(&state_a).unwrap();
        state_b.mark_restore(
            "b",
            "b-id".to_owned(),
            "b-hash".to_owned(),
            RestoreStatus::Complete,
        );
        store_b.persist(&state_b).unwrap();

        let registry = StateRegistry::load(&directory.join(STATE_FILE_NAME)).unwrap();
        assert_eq!(registry.clusters["cluster"].topics.len(), 2);
        drop((store_a, store_b));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn reconciliation_creates_missing_topics_and_recreates_drift() {
        assert!(matches!(
            plan_reconcile_action(false, Some("stale state".to_owned())),
            ReconcileAction::Create
        ));
        assert!(matches!(
            plan_reconcile_action(true, Some("config drift".to_owned())),
            ReconcileAction::Recreate(reason) if reason == "config drift"
        ));
        assert!(matches!(
            plan_reconcile_action(true, None),
            ReconcileAction::None
        ));
        assert!(matches!(
            plan_reconcile_action(
                true,
                Some("configured empty topic must be recreated".to_owned())
            ),
            ReconcileAction::Recreate(reason)
                if reason == "configured empty topic must be recreated"
        ));
        assert!(!recreation_authorized(false, false));
        assert!(recreation_authorized(true, false));
        assert!(recreation_authorized(false, true));
    }

    #[test]
    fn destination_topic_presence_distinguishes_errors_from_deletion() {
        assert_eq!(
            classify_destination_topic_presence(false, false),
            DestinationTopicPresence::Missing
        );
        assert_eq!(
            classify_destination_topic_presence(true, false),
            DestinationTopicPresence::Live
        );
        assert_eq!(
            classify_destination_topic_presence(true, true),
            DestinationTopicPresence::Errored
        );
        assert_eq!(
            create_collision_action(DestinationTopicPresence::Missing),
            CreateCollisionAction::Retry
        );
        assert_eq!(
            create_collision_action(DestinationTopicPresence::Errored),
            CreateCollisionAction::Retry
        );
        assert_eq!(
            create_collision_action(DestinationTopicPresence::Live),
            CreateCollisionAction::FailConcurrent
        );
    }

    fn integration_destination_config() -> DestinationKafkaConfig {
        DestinationKafkaConfig {
            bootstrap_servers: env::var("FRANSSON_TEST_KAFKA_BOOTSTRAP_SERVERS")
                .expect("set FRANSSON_TEST_KAFKA_BOOTSTRAP_SERVERS to run this test"),
            client_id: Some("fransson-reconciliation-test".to_owned()),
            security_protocol: None,
            sasl: None,
            properties: BTreeMap::new(),
        }
    }

    fn integration_topic(prefix: &str) -> String {
        format!(
            "fransson-{prefix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    fn empty_managed_topic(name: String) -> ManagedTopic {
        ManagedTopic {
            name,
            force: true,
            replication_factor: Some(1),
            config: BTreeMap::from([("retention.ms".to_owned(), "120000".to_owned())]),
            static_topic: Some(StaticTopicPlan {
                partitions: 6,
                kind: StaticTopicKind::Empty,
            }),
            transfer: None,
            restore: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires FRANSSON_TEST_KAFKA_BOOTSTRAP_SERVERS pointing at Kafka 4.2.0"]
    async fn kafka_reconciliation_repeatedly_recreates_forced_topic() {
        let config = integration_destination_config();
        let admin = build_admin_client(&config).unwrap();
        let metadata = build_consumer_for_destination_metadata(&config).unwrap();
        let topic = empty_managed_topic(integration_topic("recreate"));

        create_destination_topic(&admin, &metadata, &topic, 1, None)
            .await
            .unwrap();
        wait_for_destination_topic_state(
            &metadata,
            &topic.name,
            Some(1),
            Instant::now() + TOPIC_RECONCILIATION_TIMEOUT,
        )
        .await
        .unwrap();

        let mut previous_topic_id =
            kafka_identity::topic_id(metadata.client(), &topic.name).unwrap();

        for _ in 0..25 {
            let deadline = Instant::now() + TOPIC_RECONCILIATION_TIMEOUT;
            delete_destination_topic(&admin, &metadata, &topic.name, deadline)
                .await
                .unwrap();
            create_destination_topic(&admin, &metadata, &topic, 6, Some(deadline))
                .await
                .unwrap();
            wait_for_destination_topic_state(&metadata, &topic.name, Some(6), deadline)
                .await
                .unwrap();
            let topic_id = kafka_identity::topic_id(metadata.client(), &topic.name).unwrap();
            assert_ne!(topic_id, previous_topic_id);
            previous_topic_id = topic_id;

            let live_metadata = metadata
                .fetch_metadata(Some(&topic.name), Duration::from_secs(5))
                .unwrap();
            let live_topic = live_metadata
                .topics()
                .iter()
                .find(|candidate| candidate.name() == topic.name)
                .unwrap();
            assert_eq!(live_topic.partitions().len(), 6);
            assert_eq!(
                topic_mismatch_reason(&admin, &topic, 6, live_topic)
                    .await
                    .unwrap(),
                None
            );
        }

        delete_destination_topic(
            &admin,
            &metadata,
            &topic.name,
            Instant::now() + TOPIC_RECONCILIATION_TIMEOUT,
        )
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires FRANSSON_TEST_KAFKA_BOOTSTRAP_SERVERS pointing at Kafka 4.2.0"]
    async fn kafka_reconciliation_rejects_concurrent_topic_recreation() {
        let config = integration_destination_config();
        let admin = build_admin_client(&config).unwrap();
        let competing_admin = build_admin_client(&config).unwrap();
        let metadata = build_consumer_for_destination_metadata(&config).unwrap();
        let topic = empty_managed_topic(integration_topic("concurrent-recreate"));

        create_destination_topic(&admin, &metadata, &topic, 1, None)
            .await
            .unwrap();
        let deadline = Instant::now() + TOPIC_RECONCILIATION_TIMEOUT;
        delete_destination_topic(&admin, &metadata, &topic.name, deadline)
            .await
            .unwrap();
        create_destination_topic(&competing_admin, &metadata, &topic, 1, None)
            .await
            .unwrap();
        wait_for_destination_topic_state(&metadata, &topic.name, Some(1), deadline)
            .await
            .unwrap();

        let error = create_destination_topic(&admin, &metadata, &topic, 6, Some(deadline))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("recreated by another client"));

        delete_destination_topic(
            &admin,
            &metadata,
            &topic.name,
            Instant::now() + TOPIC_RECONCILIATION_TIMEOUT,
        )
        .await
        .unwrap();
    }

    #[test]
    fn published_examples_match_the_strict_schema() {
        for yaml in [
            include_str!("../examples/config.example.yaml"),
            include_str!("../examples/config.no-auth-dst.example.yaml"),
        ] {
            let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
            validate_config(&config).unwrap();
            resolve_topics(&config, Path::new("config.yaml")).unwrap();
        }
    }
}
