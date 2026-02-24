use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use axum::{Router, body::Body};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use http::{Request, Response};
use std::{collections::HashMap, future::Future, sync::Arc};
use tower::ServiceExt;

use crate::runner::RunnerHandle;

#[derive(Clone, Debug)]
pub struct ActorConfig {
	pub name: String,
	pub key: Option<String>,
	pub create_ts: i64,
	pub input: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct HibernatingRequest {
	pub gateway_id: [u8; 4],
	pub request_id: [u8; 4],
}

#[derive(Clone, Debug)]
pub struct ActorContext {
	pub actor_id: String,
	pub generation: u32,
	pub actor_name: String,
	pub config: ActorConfig,
	pub hibernating_requests: Vec<HibernatingRequest>,
}

#[derive(Clone, Debug)]
pub struct HttpContext {
	pub actor_id: String,
	pub generation: u32,
	pub actor_name: String,
	pub gateway_id: [u8; 4],
	pub request_id: [u8; 4],
}

#[derive(Clone, Debug)]
pub struct WebSocketContext {
	pub actor_id: String,
	pub generation: u32,
	pub actor_name: String,
	pub gateway_id: [u8; 4],
	pub request_id: [u8; 4],
	pub path: String,
	pub headers: HashMap<String, String>,
	pub is_hibernatable: bool,
	pub is_restoring_hibernatable: bool,
}

#[derive(Clone, Debug)]
pub struct HibernatingWebSocketMetadata {
	pub gateway_id: [u8; 4],
	pub request_id: [u8; 4],
	pub client_message_index: u16,
	pub server_message_index: u16,
	pub path: String,
	pub headers: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct WebSocketMessage {
	pub data: Vec<u8>,
	pub binary: bool,
	pub message_index: u16,
}

#[derive(Clone)]
pub struct ActorRequestContext {
	pub runner: RunnerHandle,
	pub actor_id: String,
	pub generation: u32,
	pub actor_name: String,
}

impl ActorRequestContext {
	pub async fn kv_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>> {
		self.runner.kv_get(&self.actor_id, keys).await
	}

	pub async fn kv_get_u64(&self, key: impl AsRef<[u8]>) -> Result<Option<u64>> {
		let values = self.kv_get(vec![key.as_ref().to_vec()]).await?;
		let Some(raw) = values.into_iter().next().flatten() else {
			return Ok(None);
		};

		if raw.len() != 8 {
			bail!("expected u64 value to be 8 bytes, got {}", raw.len());
		}

		let mut bytes = [0u8; 8];
		bytes.copy_from_slice(&raw);
		Ok(Some(u64::from_le_bytes(bytes)))
	}

	pub async fn kv_put(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
		self.runner.kv_put(&self.actor_id, entries).await
	}

	pub async fn kv_put_u64(&self, key: impl AsRef<[u8]>, value: u64) -> Result<()> {
		self.kv_put(vec![(key.as_ref().to_vec(), value.to_le_bytes().to_vec())])
			.await
	}

	pub async fn kv_delete(&self, keys: Vec<Vec<u8>>) -> Result<()> {
		self.runner.kv_delete(&self.actor_id, keys).await
	}

	pub async fn kv_drop(&self) -> Result<()> {
		self.runner.kv_drop(&self.actor_id).await
	}

	pub async fn sleep_actor(&self) -> Result<()> {
		self.runner
			.sleep_actor(&self.actor_id, Some(self.generation))
			.await
	}

	pub async fn stop_actor(&self) -> Result<()> {
		self.runner
			.stop_actor(&self.actor_id, Some(self.generation))
			.await
	}

	pub async fn set_alarm(&self, alarm_ts: Option<i64>) -> Result<()> {
		self.runner
			.set_alarm(&self.actor_id, alarm_ts, Some(self.generation))
			.await
	}

	pub async fn clear_alarm(&self) -> Result<()> {
		self.set_alarm(None).await
	}
}

#[async_trait]
pub trait RunnerApp: Send + Sync + 'static {
	async fn on_connected(&self, _runner: RunnerHandle) -> Result<()> {
		Ok(())
	}

	async fn on_disconnected(&self, _runner: RunnerHandle, _code: u16, _reason: String) -> Result<()> {
		Ok(())
	}

	async fn on_shutdown(&self, _runner: RunnerHandle) -> Result<()> {
		Ok(())
	}

	async fn on_actor_start(&self, _runner: RunnerHandle, _ctx: ActorContext) -> Result<()> {
		Ok(())
	}

	async fn on_actor_stop(&self, _runner: RunnerHandle, _ctx: ActorContext) -> Result<()> {
		Ok(())
	}

	async fn fetch(
		&self,
		_runner: RunnerHandle,
		_ctx: HttpContext,
		_request: Request<Bytes>,
	) -> Result<Response<Bytes>> {
		Ok(Response::builder()
			.status(501)
			.body(Bytes::from_static(b"Not Implemented"))?)
	}

	async fn websocket(
		&self,
		_runner: RunnerHandle,
		_ctx: WebSocketContext,
	) -> Result<()> {
		Ok(())
	}

	async fn websocket_message(
		&self,
		_runner: RunnerHandle,
		_ctx: WebSocketContext,
		_message: WebSocketMessage,
	) -> Result<()> {
		Ok(())
	}

	async fn websocket_close(
		&self,
		_runner: RunnerHandle,
		_ctx: WebSocketContext,
		_code: Option<u16>,
		_reason: Option<String>,
	) -> Result<()> {
		Ok(())
	}

	fn can_hibernate(&self, _ctx: &WebSocketContext) -> bool {
		false
	}
}

type LifecycleHook = Arc<dyn Fn(ActorContext) -> BoxFuture<'static, Result<()>> + Send + Sync>;

#[derive(Clone)]
pub struct AxumActorDefinition {
	router: Router<ActorRequestContext>,
	on_start: Option<LifecycleHook>,
	on_stop: Option<LifecycleHook>,
}

impl AxumActorDefinition {
	pub fn new(router: Router<ActorRequestContext>) -> Self {
		Self {
			router,
			on_start: None,
			on_stop: None,
		}
	}

	pub fn on_start<F, Fut>(mut self, hook: F) -> Self
	where
		F: Fn(ActorContext) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<()>> + Send + 'static,
	{
		self.on_start = Some(Arc::new(move |ctx| Box::pin(hook(ctx))));
		self
	}

	pub fn on_stop<F, Fut>(mut self, hook: F) -> Self
	where
		F: Fn(ActorContext) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<()>> + Send + 'static,
	{
		self.on_stop = Some(Arc::new(move |ctx| Box::pin(hook(ctx))));
		self
	}
}

#[derive(Clone, Default)]
pub struct AxumRunnerApp {
	actors: HashMap<String, AxumActorDefinition>,
}

impl AxumRunnerApp {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_actor(mut self, name: impl Into<String>, definition: AxumActorDefinition) -> Self {
		self.actors.insert(name.into(), definition);
		self
	}

	pub fn actor(&self, name: &str) -> Option<&AxumActorDefinition> {
		self.actors.get(name)
	}
}

#[async_trait]
impl RunnerApp for AxumRunnerApp {
	async fn on_actor_start(&self, _runner: RunnerHandle, ctx: ActorContext) -> Result<()> {
		let actor = self
			.actors
			.get(&ctx.actor_name)
			.with_context(|| format!("actor '{}' is not registered", ctx.actor_name))?;

		if let Some(on_start) = &actor.on_start {
			on_start(ctx).await?;
		}

		Ok(())
	}

	async fn on_actor_stop(&self, _runner: RunnerHandle, ctx: ActorContext) -> Result<()> {
		let actor = self
			.actors
			.get(&ctx.actor_name)
			.with_context(|| format!("actor '{}' is not registered", ctx.actor_name))?;

		if let Some(on_stop) = &actor.on_stop {
			on_stop(ctx).await?;
		}

		Ok(())
	}

	async fn fetch(
		&self,
		runner: RunnerHandle,
		ctx: HttpContext,
		request: Request<Bytes>,
	) -> Result<Response<Bytes>> {
		let actor = self
			.actors
			.get(&ctx.actor_name)
			.with_context(|| format!("actor '{}' is not registered", ctx.actor_name))?;

		let state = ActorRequestContext {
			runner,
			actor_id: ctx.actor_id.clone(),
			generation: ctx.generation,
			actor_name: ctx.actor_name,
		};

		let (parts, body) = request.into_parts();
		let request = Request::from_parts(parts, Body::from(body));

		let response = actor
			.router
			.clone()
			.with_state(state)
			.oneshot(request)
			.await
			.context("failed to serve axum actor route")?;

		let (parts, body) = response.into_parts();
		let body = axum::body::to_bytes(body, usize::MAX)
			.await
			.context("failed to collect actor response body")?;

		Ok(Response::from_parts(parts, body))
	}
}
