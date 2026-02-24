//! Test runner wrapper for engine tests.
//!
//! This module provides a `TestRunnerBuilder` that wraps the standalone `rivet-engine-runner`
//! package, adding test-specific functionality like building from a `TestDatacenter`.

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use rivet_runner_protocol::mk2 as rp;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast};

// Re-export from the standalone package
pub use rivet_engine_runner::{
	ActorLifecycleEvent, PROTOCOL_VERSION, Runner, RunnerBuilder, RunnerConfig, RunnerHandle,
	protocol_types,
};

// Re-export test behavior actors from the local behaviors module
pub use super::behaviors::{
	CountingCrashActor, CrashNTimesThenSucceedActor, CrashOnStartActor, CustomActor,
	CustomActorBuilder, DelayedStartActor, EchoActor, NotifyOnStartActor, SleepImmediatelyActor,
	StopImmediatelyActor, TimeoutActor, VerifyInputActor,
};

#[derive(Clone)]
pub struct TestRunner {
	runner: Runner,
	handle: RunnerHandle,
}

impl TestRunner {
	pub fn name(&self) -> &str {
		self.runner.name()
	}

	pub async fn start(&self) -> Result<()> {
		self.runner.start().await
	}

	pub async fn wait_ready(&self) -> String {
		self.runner
			.wait_ready()
			.await
			.expect("runner should become ready")
	}

	pub async fn has_actor(&self, actor_id: &str) -> bool {
		self.handle.has_actor(actor_id, None).await
	}

	pub fn subscribe_lifecycle_events(&self) -> broadcast::Receiver<ActorLifecycleEvent> {
		self.runner.subscribe_lifecycle_events()
	}

	pub async fn get_actor_ids(&self) -> Vec<String> {
		self.handle.get_actor_ids().await
	}

	pub async fn shutdown(&self) {
		if let Err(err) = self.runner.shutdown(false).await {
			tracing::error!(?err, "failed to shutdown test runner");
		}
	}

	pub async fn crash(&self) {
		if let Err(err) = self.runner.crash().await {
			tracing::error!(?err, "failed to crash test runner");
		}
	}
}

#[derive(Clone)]
pub struct ActorConfig {
	pub actor_id: String,
	pub generation: u32,
	pub actor_name: String,
	pub input: Option<Vec<u8>>,
	runner: RunnerHandle,
}

impl ActorConfig {
	fn from_context(ctx: &rivet_engine_runner::ActorContext, runner: RunnerHandle) -> Self {
		Self {
			actor_id: ctx.actor_id.clone(),
			generation: ctx.generation,
			actor_name: ctx.actor_name.clone(),
			input: ctx.config.input.clone(),
			runner,
		}
	}

	pub async fn send_kv_get(&self, keys: Vec<Vec<u8>>) -> Result<KvReadResponse> {
		let requested_keys = keys.clone();
		let values = self.runner.kv_get(&self.actor_id, keys).await?;

		let mut response_keys = Vec::new();
		let mut response_values = Vec::new();
		for (key, value) in requested_keys.into_iter().zip(values.into_iter()) {
			if let Some(value) = value {
				response_keys.push(key);
				response_values.push(value);
			}
		}

		Ok(KvReadResponse {
			keys: response_keys,
			values: response_values,
		})
	}

	pub async fn send_kv_put(&self, keys: Vec<Vec<u8>>, values: Vec<Vec<u8>>) -> Result<()> {
		if keys.len() != values.len() {
			bail!(
				"mismatched kv put payload lengths: keys={}, values={}",
				keys.len(),
				values.len()
			);
		}

		let entries = keys.into_iter().zip(values).collect::<Vec<_>>();
		self.runner.kv_put(&self.actor_id, entries).await
	}

	pub async fn send_kv_delete(&self, keys: Vec<Vec<u8>>) -> Result<()> {
		self.runner.kv_delete(&self.actor_id, keys).await
	}

	pub async fn send_kv_drop(&self) -> Result<()> {
		self.runner.kv_drop(&self.actor_id).await
	}

	pub async fn send_kv_list(
		&self,
		query: rp::KvListQuery,
		reverse: Option<bool>,
		limit: Option<u64>,
	) -> Result<KvReadResponse> {
		let entries = match query {
			rp::KvListQuery::KvListAllQuery => {
				self.runner
					.kv_list_all(&self.actor_id, reverse, limit)
					.await?
			}
			rp::KvListQuery::KvListPrefixQuery(prefix) => {
				self.runner
					.kv_list_prefix(&self.actor_id, prefix.key, reverse, limit)
					.await?
			}
			rp::KvListQuery::KvListRangeQuery(range) => {
				self.runner
					.kv_list_range(
						&self.actor_id,
						range.start,
						range.end,
						range.exclusive,
						reverse,
						limit,
					)
					.await?
			}
		};

		let (keys, values): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
		Ok(KvReadResponse { keys, values })
	}

	pub fn send_sleep_intent(&self) {
		let runner = self.runner.clone();
		let actor_id = self.actor_id.clone();
		let generation = self.generation;
		tokio::spawn(async move {
			if let Err(err) = runner.sleep_actor(&actor_id, Some(generation)).await {
				tracing::error!(?err, %actor_id, generation, "failed to send sleep intent");
			}
		});
	}

	pub fn send_stop_intent(&self) {
		let runner = self.runner.clone();
		let actor_id = self.actor_id.clone();
		let generation = self.generation;
		tokio::spawn(async move {
			if let Err(err) = runner.stop_actor(&actor_id, Some(generation)).await {
				tracing::error!(?err, %actor_id, generation, "failed to send stop intent");
			}
		});
	}

	pub fn send_set_alarm(&self, alarm_ts: i64) {
		let runner = self.runner.clone();
		let actor_id = self.actor_id.clone();
		let generation = self.generation;
		tokio::spawn(async move {
			if let Err(err) = runner.set_alarm(&actor_id, Some(alarm_ts), Some(generation)).await {
				tracing::error!(?err, %actor_id, generation, alarm_ts, "failed to set alarm");
			}
		});
	}

	pub fn send_clear_alarm(&self) {
		let runner = self.runner.clone();
		let actor_id = self.actor_id.clone();
		let generation = self.generation;
		tokio::spawn(async move {
			if let Err(err) = runner.clear_alarm(&actor_id, Some(generation)).await {
				tracing::error!(?err, %actor_id, generation, "failed to clear alarm");
			}
		});
	}
}

#[derive(Debug, Clone)]
pub struct KvReadResponse {
	pub keys: Vec<Vec<u8>>,
	pub values: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum ActorStartResult {
	Running,
	Crash { code: i32, message: String },
	Delay(Duration),
	Timeout,
}

#[derive(Debug, Clone)]
pub enum ActorStopResult {
	Success,
}

#[async_trait]
pub trait Actor: Send + 'static {
	async fn on_start(&mut self, config: ActorConfig) -> Result<ActorStartResult>;
	async fn on_stop(&mut self) -> Result<ActorStopResult>;
	fn name(&self) -> &str;
}

type ActorFactory = Arc<dyn Fn(ActorConfig) -> Box<dyn Actor> + Send + Sync>;

#[derive(Clone)]
struct LegacyRunnerApp {
	actor_factories: HashMap<String, ActorFactory>,
	actors: Arc<Mutex<HashMap<String, Box<dyn Actor>>>>,
}

impl LegacyRunnerApp {
	fn new(actor_factories: HashMap<String, ActorFactory>) -> Self {
		Self {
			actor_factories,
			actors: Arc::new(Mutex::new(HashMap::new())),
		}
	}
}

#[async_trait]
impl rivet_engine_runner::RunnerApp for LegacyRunnerApp {
	async fn on_actor_start(
		&self,
		runner: RunnerHandle,
		ctx: rivet_engine_runner::ActorContext,
	) -> Result<()> {
		let actor_factory = self
			.actor_factories
			.get(&ctx.actor_name)
			.ok_or_else(|| anyhow!("no actor behavior registered for '{}'", ctx.actor_name))?
			.clone();
		let legacy_config = ActorConfig::from_context(&ctx, runner);
		let mut actor = actor_factory(legacy_config.clone());

		match actor.on_start(legacy_config).await? {
			ActorStartResult::Running => {
				self.actors.lock().await.insert(ctx.actor_id, actor);
				Ok(())
			}
			ActorStartResult::Delay(delay) => {
				tokio::time::sleep(delay).await;
				self.actors.lock().await.insert(ctx.actor_id, actor);
				Ok(())
			}
			ActorStartResult::Crash { code, message } => {
				bail!("actor crashed with code {}: {}", code, message)
			}
			ActorStartResult::Timeout => {
				std::future::pending::<()>().await;
				unreachable!()
			}
		}
	}

	async fn on_actor_stop(
		&self,
		_runner: RunnerHandle,
		ctx: rivet_engine_runner::ActorContext,
	) -> Result<()> {
		let Some(mut actor) = self.actors.lock().await.remove(&ctx.actor_id) else {
			return Ok(());
		};

		match actor.on_stop().await? {
			ActorStopResult::Success => Ok(()),
		}
	}
}

/// Test-specific runner builder that integrates with TestDatacenter
pub struct TestRunnerBuilder {
	namespace: String,
	runner_name: String,
	runner_key: String,
	version: u32,
	total_slots: u32,
	actor_factories: HashMap<String, ActorFactory>,
}

impl TestRunnerBuilder {
	pub fn new(namespace: &str) -> Self {
		Self {
			namespace: namespace.to_string(),
			runner_name: "test-runner".to_string(),
			runner_key: format!("key-{:012x}", rand::random::<u64>()),
			version: 1,
			total_slots: 100,
			actor_factories: HashMap::new(),
		}
	}

	pub fn with_runner_name(mut self, name: &str) -> Self {
		self.runner_name = name.to_string();
		self
	}

	pub fn with_runner_key(mut self, key: &str) -> Self {
		self.runner_key = key.to_string();
		self
	}

	pub fn with_version(mut self, version: u32) -> Self {
		self.version = version;
		self
	}

	pub fn with_total_slots(mut self, total_slots: u32) -> Self {
		self.total_slots = total_slots;
		self
	}

	/// Register an actor factory for a specific actor name
	pub fn with_actor_behavior<F>(mut self, actor_name: &str, factory: F) -> Self
	where
		F: Fn(ActorConfig) -> Box<dyn Actor> + Send + Sync + 'static,
	{
		self.actor_factories
			.insert(actor_name.to_string(), Arc::new(factory));
		self
	}

	/// Build the runner using the TestDatacenter's guard port
	pub async fn build(self, dc: &super::TestDatacenter) -> Result<TestRunner> {
		let endpoint = format!("http://127.0.0.1:{}", dc.guard_port());
		let token = "dev".to_string();

		// Build the config using the new API
		let config = RunnerConfig::builder()
			.endpoint(&endpoint)
			.token(&token)
			.namespace(&self.namespace)
			.runner_name(&self.runner_name)
			.runner_key(&self.runner_key)
			.version(self.version)
			.total_slots(self.total_slots)
			.build()?;

		let app = LegacyRunnerApp::new(self.actor_factories);
		let runner = RunnerBuilder::new(config).app(app).build()?;
		let handle = runner.handle();

		Ok(TestRunner { runner, handle })
	}
}
