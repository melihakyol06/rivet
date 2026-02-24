use crate::{
	actor::{
		ActorConfig, ActorContext, HttpContext, HibernatingRequest, HibernatingWebSocketMetadata,
		RunnerApp, WebSocketContext, WebSocketMessage,
	},
	protocol,
};
use anyhow::{Context, Result, anyhow, bail};
use async_stream::stream;
use base64::Engine;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Request, Response, StatusCode};
use rivet_runner_protocol::mk2 as rp;
use std::{
	collections::HashMap,
	hash::{Hash, Hasher},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU32, Ordering},
	},
	time::Duration,
};
use tokio::{
	sync::{Mutex, Notify, broadcast, mpsc, oneshot},
	task::JoinHandle,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use urlencoding::encode;
use vbare::OwnedVersionedData;

const COMMAND_ACK_INTERVAL: Duration = Duration::from_secs(5 * 60);
const RECONNECT_INITIAL_DELAY_MS: u64 = 1_000;
const RECONNECT_MAX_DELAY_MS: u64 = 30_000;

#[derive(Debug, Clone)]
pub enum ActorLifecycleEvent {
	Started { actor_id: String, generation: u32 },
	Stopped { actor_id: String, generation: u32 },
}

#[derive(Clone, Debug, Default)]
pub struct PrepopulateActorName {
	pub metadata: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct RunnerConfig {
	pub endpoint: String,
	pub token: String,
	pub namespace: String,
	pub runner_name: String,
	pub runner_key: String,
	pub version: u32,
	pub total_slots: u32,
	pub pegboard_endpoint: Option<String>,
	pub prepopulate_actor_names: HashMap<String, PrepopulateActorName>,
	pub metadata: Option<serde_json::Value>,
}

impl RunnerConfig {
	pub fn builder() -> RunnerConfigBuilder {
		RunnerConfigBuilder::default()
	}
}

#[derive(Default)]
pub struct RunnerConfigBuilder {
	endpoint: Option<String>,
	token: Option<String>,
	namespace: Option<String>,
	runner_name: Option<String>,
	runner_key: Option<String>,
	version: Option<u32>,
	total_slots: Option<u32>,
	pegboard_endpoint: Option<String>,
	prepopulate_actor_names: HashMap<String, PrepopulateActorName>,
	metadata: Option<serde_json::Value>,
}

impl RunnerConfigBuilder {
	pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
		self.endpoint = Some(endpoint.into());
		self
	}

	pub fn token(mut self, token: impl Into<String>) -> Self {
		self.token = Some(token.into());
		self
	}

	pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
		self.namespace = Some(namespace.into());
		self
	}

	pub fn runner_name(mut self, runner_name: impl Into<String>) -> Self {
		self.runner_name = Some(runner_name.into());
		self
	}

	pub fn runner_key(mut self, runner_key: impl Into<String>) -> Self {
		self.runner_key = Some(runner_key.into());
		self
	}

	pub fn version(mut self, version: u32) -> Self {
		self.version = Some(version);
		self
	}

	pub fn total_slots(mut self, total_slots: u32) -> Self {
		self.total_slots = Some(total_slots);
		self
	}

	pub fn pegboard_endpoint(mut self, endpoint: impl Into<String>) -> Self {
		self.pegboard_endpoint = Some(endpoint.into());
		self
	}

	pub fn prepopulate_actor_name(
		mut self,
		name: impl Into<String>,
		metadata: serde_json::Value,
	) -> Self {
		self.prepopulate_actor_names
			.insert(name.into(), PrepopulateActorName { metadata });
		self
	}

	pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
		self.metadata = Some(metadata);
		self
	}

	pub fn build(self) -> Result<RunnerConfig> {
		Ok(RunnerConfig {
			endpoint: self.endpoint.context("endpoint is required")?,
			token: self.token.unwrap_or_else(|| "dev".to_string()),
			namespace: self.namespace.context("namespace is required")?,
			runner_name: self
				.runner_name
				.unwrap_or_else(|| "engine-runner".to_string()),
			runner_key: self
				.runner_key
				.unwrap_or_else(|| format!("key-{:012x}", rand::random::<u64>())),
			version: self.version.unwrap_or(1),
			total_slots: self.total_slots.unwrap_or(100),
			pegboard_endpoint: self.pegboard_endpoint,
			prepopulate_actor_names: self.prepopulate_actor_names,
			metadata: self.metadata,
		})
	}
}

pub struct RunnerBuilder {
	config: RunnerConfig,
	app: Option<Arc<dyn RunnerApp>>,
}

impl RunnerBuilder {
	pub fn new(config: RunnerConfig) -> Self {
		Self { config, app: None }
	}

	pub fn app<A>(mut self, app: A) -> Self
	where
		A: RunnerApp,
	{
		self.app = Some(Arc::new(app));
		self
	}

	pub fn app_arc(mut self, app: Arc<dyn RunnerApp>) -> Self {
		self.app = Some(app);
		self
	}

	pub fn build(self) -> Result<Runner> {
		let app = self
			.app
			.context("runner app is required; call RunnerBuilder::app")?;

		let (lifecycle_tx, _) = broadcast::channel(128);

		let inner = Arc::new(RunnerInner {
			config: self.config,
			app,
			runner_id: Arc::new(Mutex::new(None)),
			ready_notify: Arc::new(Notify::new()),
			shutdown_notify: Arc::new(Notify::new()),
			shutdown: Arc::new(AtomicBool::new(false)),
			started: Arc::new(AtomicBool::new(false)),
			ws_sender: Arc::new(Mutex::new(None)),
			actors: Arc::new(Mutex::new(HashMap::new())),
			lifecycle_tx,
			next_kv_request_id: Arc::new(AtomicU32::new(0)),
			pending_kv_requests: Arc::new(Mutex::new(HashMap::new())),
			pending_http_requests: Arc::new(Mutex::new(HashMap::new())),
			tunnel_message_indices: Arc::new(Mutex::new(HashMap::new())),
			websockets: Arc::new(Mutex::new(HashMap::new())),
		});

		Ok(Runner {
			inner,
			task: Arc::new(Mutex::new(None)),
		})
	}
}

#[derive(Clone)]
pub struct Runner {
	inner: Arc<RunnerInner>,
	task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Runner {
	pub fn builder(config: RunnerConfig) -> RunnerBuilder {
		RunnerBuilder::new(config)
	}

	pub fn handle(&self) -> RunnerHandle {
		RunnerHandle {
			inner: self.inner.clone(),
		}
	}

	pub fn subscribe_lifecycle_events(&self) -> broadcast::Receiver<ActorLifecycleEvent> {
		self.inner.lifecycle_tx.subscribe()
	}

	pub async fn start(&self) -> Result<()> {
		if self.inner.started.swap(true, Ordering::SeqCst) {
			bail!("runner.start called more than once")
		}

		self.inner.shutdown.store(false, Ordering::SeqCst);
		let inner = self.inner.clone();
		let join = tokio::spawn(async move {
			if let Err(err) = inner.run().await {
				tracing::error!(?err, "runner terminated with error");
			}
		});

		*self.task.lock().await = Some(join);
		Ok(())
	}

	pub async fn wait_ready(&self) -> Result<String> {
		loop {
			if let Some(runner_id) = self.inner.runner_id.lock().await.clone() {
				return Ok(runner_id);
			}
			self.inner.ready_notify.notified().await;
		}
	}

	pub async fn shutdown(&self, _immediate: bool) -> Result<()> {
		self.inner.shutdown.store(true, Ordering::SeqCst);
		self.inner.shutdown_notify.notify_waiters();

		if let Some(join) = self.task.lock().await.take() {
			let _ = join.await;
		}

		self.inner
			.app
			.on_shutdown(self.handle())
			.await
			.context("runner app shutdown callback failed")?;

		Ok(())
	}

	pub async fn crash(&self) -> Result<()> {
		self.inner.shutdown.store(true, Ordering::SeqCst);
		self.inner.shutdown_notify.notify_waiters();
		Ok(())
	}

	pub fn name(&self) -> &str {
		&self.inner.config.runner_name
	}

	pub async fn get_serverless_init_packet(&self) -> Result<Option<String>> {
		self.handle().get_serverless_init_packet().await
	}
}

#[derive(Clone)]
pub struct RunnerHandle {
	inner: Arc<RunnerInner>,
}

impl RunnerHandle {
	pub fn name(&self) -> &str {
		&self.inner.config.runner_name
	}

	pub async fn runner_id(&self) -> Option<String> {
		self.inner.runner_id.lock().await.clone()
	}

	pub async fn has_actor(&self, actor_id: &str, generation: Option<u32>) -> bool {
		let actors = self.inner.actors.lock().await;
		actors
			.get(actor_id)
			.map(|x| generation.map(|g| g == x.generation).unwrap_or(true))
			.unwrap_or(false)
	}

	pub async fn get_actor_ids(&self) -> Vec<String> {
		let actors = self.inner.actors.lock().await;
		actors.keys().cloned().collect()
	}

	pub async fn sleep_actor(&self, actor_id: &str, generation: Option<u32>) -> Result<()> {
		self.inner
			.send_actor_intent(actor_id, generation, rp::ActorIntent::ActorIntentSleep)
			.await
	}

	pub async fn stop_actor(&self, actor_id: &str, generation: Option<u32>) -> Result<()> {
		self.inner
			.send_actor_intent(actor_id, generation, rp::ActorIntent::ActorIntentStop)
			.await
	}

	pub async fn set_alarm(
		&self,
		actor_id: &str,
		alarm_ts: Option<i64>,
		generation: Option<u32>,
	) -> Result<()> {
		self.inner
			.send_alarm_event(actor_id, generation, alarm_ts)
			.await
	}

	pub async fn clear_alarm(&self, actor_id: &str, generation: Option<u32>) -> Result<()> {
		self.set_alarm(actor_id, None, generation).await
	}

	pub async fn kv_get(
		&self,
		actor_id: &str,
		keys: Vec<Vec<u8>>,
	) -> Result<Vec<Option<Vec<u8>>>> {
		let requested_keys = keys.clone();
		let response = self
			.inner
			.send_kv_request(
				actor_id,
				rp::KvRequestData::KvGetRequest(rp::KvGetRequest { keys }),
			)
			.await?;

		match response {
			rp::KvResponseData::KvGetResponse(resp) => {
				let mut values = Vec::with_capacity(requested_keys.len());
				for key in requested_keys {
					let mut value = None;
					for (idx, response_key) in resp.keys.iter().enumerate() {
						if *response_key == key {
							value = resp.values.get(idx).cloned();
							break;
						}
					}
					values.push(value);
				}
				Ok(values)
			}
			rp::KvResponseData::KvErrorResponse(err) => {
				bail!("kv get failed: {}", err.message)
			}
			other => bail!("unexpected kv get response: {:?}", other),
		}
	}

	pub async fn kv_list_all(
		&self,
		actor_id: &str,
		reverse: Option<bool>,
		limit: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		self.kv_list(
			actor_id,
			rp::KvListQuery::KvListAllQuery,
			reverse,
			limit,
		)
		.await
	}

	pub async fn kv_list_prefix(
		&self,
		actor_id: &str,
		prefix: Vec<u8>,
		reverse: Option<bool>,
		limit: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		self.kv_list(
			actor_id,
			rp::KvListQuery::KvListPrefixQuery(rp::KvListPrefixQuery { key: prefix }),
			reverse,
			limit,
		)
		.await
	}

	pub async fn kv_list_range(
		&self,
		actor_id: &str,
		start: Vec<u8>,
		end: Vec<u8>,
		exclusive: bool,
		reverse: Option<bool>,
		limit: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		self.kv_list(
			actor_id,
			rp::KvListQuery::KvListRangeQuery(rp::KvListRangeQuery {
				start,
				end,
				exclusive,
			}),
			reverse,
			limit,
		)
		.await
	}

	async fn kv_list(
		&self,
		actor_id: &str,
		query: rp::KvListQuery,
		reverse: Option<bool>,
		limit: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let response = self
			.inner
			.send_kv_request(
				actor_id,
				rp::KvRequestData::KvListRequest(rp::KvListRequest {
					query,
					reverse,
					limit,
				}),
			)
			.await?;

		match response {
			rp::KvResponseData::KvListResponse(resp) => Ok(resp.keys.into_iter().zip(resp.values).collect()),
			rp::KvResponseData::KvErrorResponse(err) => {
				bail!("kv list failed: {}", err.message)
			}
			other => bail!("unexpected kv list response: {:?}", other),
		}
	}

	pub async fn kv_put(&self, actor_id: &str, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
		let (keys, values): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
		let response = self
			.inner
			.send_kv_request(
				actor_id,
				rp::KvRequestData::KvPutRequest(rp::KvPutRequest { keys, values }),
			)
			.await?;

		match response {
			rp::KvResponseData::KvPutResponse => Ok(()),
			rp::KvResponseData::KvErrorResponse(err) => {
				bail!("kv put failed: {}", err.message)
			}
			other => bail!("unexpected kv put response: {:?}", other),
		}
	}

	pub async fn kv_delete(&self, actor_id: &str, keys: Vec<Vec<u8>>) -> Result<()> {
		let response = self
			.inner
			.send_kv_request(
				actor_id,
				rp::KvRequestData::KvDeleteRequest(rp::KvDeleteRequest { keys }),
			)
			.await?;

		match response {
			rp::KvResponseData::KvDeleteResponse => Ok(()),
			rp::KvResponseData::KvErrorResponse(err) => {
				bail!("kv delete failed: {}", err.message)
			}
			other => bail!("unexpected kv delete response: {:?}", other),
		}
	}

	pub async fn kv_drop(&self, actor_id: &str) -> Result<()> {
		let response = self
			.inner
			.send_kv_request(actor_id, rp::KvRequestData::KvDropRequest)
			.await?;

		match response {
			rp::KvResponseData::KvDropResponse => Ok(()),
			rp::KvResponseData::KvErrorResponse(err) => {
				bail!("kv drop failed: {}", err.message)
			}
			other => bail!("unexpected kv drop response: {:?}", other),
		}
	}

	pub async fn send_hibernatable_websocket_message_ack(
		&self,
		gateway_id: [u8; 4],
		request_id: [u8; 4],
		index: u16,
	) -> Result<()> {
		self.inner
			.send_tunnel_message(
				TunnelRequestKey {
					gateway_id,
					request_id,
				},
				rp::ToServerTunnelMessageKind::ToServerWebSocketMessageAck(
					rp::ToServerWebSocketMessageAck { index },
				),
			)
			.await
	}

	pub async fn send_websocket_message(
		&self,
		gateway_id: [u8; 4],
		request_id: [u8; 4],
		data: Vec<u8>,
		binary: bool,
	) -> Result<()> {
		self.inner
			.send_tunnel_message(
				TunnelRequestKey {
					gateway_id,
					request_id,
				},
				rp::ToServerTunnelMessageKind::ToServerWebSocketMessage(rp::ToServerWebSocketMessage {
					data,
					binary,
				}),
			)
			.await
	}

	pub async fn close_websocket(
		&self,
		gateway_id: [u8; 4],
		request_id: [u8; 4],
		code: Option<u16>,
		reason: Option<String>,
		hibernate: bool,
	) -> Result<()> {
		self.inner
			.send_tunnel_message(
				TunnelRequestKey {
					gateway_id,
					request_id,
				},
				rp::ToServerTunnelMessageKind::ToServerWebSocketClose(rp::ToServerWebSocketClose {
					code,
					reason,
					hibernate,
				}),
			)
			.await
	}

	pub async fn restore_hibernating_requests(
		&self,
		actor_id: &str,
		meta_entries: Vec<HibernatingWebSocketMetadata>,
	) -> Result<()> {
		self.inner
			.restore_hibernating_requests(actor_id, meta_entries)
			.await
	}

	pub async fn get_serverless_init_packet(&self) -> Result<Option<String>> {
		let Some(runner_id) = self.runner_id().await else {
			return Ok(None);
		};

		let payload = rivet_runner_protocol::versioned::ToServerlessServer::wrap_latest(
			rp::ToServerlessServer::ToServerlessServerInit(rp::ToServerlessServerInit {
				runner_id,
				runner_protocol_version: protocol::PROTOCOL_VERSION,
			}),
		)
		.serialize_with_embedded_version(protocol::PROTOCOL_VERSION)?;

		Ok(Some(base64::engine::general_purpose::STANDARD.encode(payload)))
	}
}

#[derive(Clone)]
struct RunnerInner {
	config: RunnerConfig,
	app: Arc<dyn RunnerApp>,
	runner_id: Arc<Mutex<Option<String>>>,
	ready_notify: Arc<Notify>,
	shutdown_notify: Arc<Notify>,
	shutdown: Arc<AtomicBool>,
	started: Arc<AtomicBool>,
	ws_sender: Arc<Mutex<Option<mpsc::UnboundedSender<rp::ToServer>>>>,
	actors: Arc<Mutex<HashMap<String, ActorRuntimeState>>>,
	lifecycle_tx: broadcast::Sender<ActorLifecycleEvent>,
	next_kv_request_id: Arc<AtomicU32>,
	pending_kv_requests: Arc<Mutex<HashMap<u32, oneshot::Sender<rp::KvResponseData>>>>,
	pending_http_requests: Arc<Mutex<HashMap<TunnelRequestKey, PendingHttpRequest>>>,
	tunnel_message_indices: Arc<Mutex<HashMap<TunnelRequestKey, u16>>>,
	websockets: Arc<Mutex<HashMap<TunnelRequestKey, WebSocketRuntimeState>>>,
}

#[derive(Clone, Debug)]
struct ActorRuntimeState {
	generation: u32,
	config: ActorConfig,
	started: bool,
	start_notify: Arc<Notify>,
	last_command_idx: i64,
	next_event_idx: i64,
	event_history: Vec<rp::EventWrapper>,
	hibernating_requests: Vec<HibernatingRequest>,
	hibernation_restored: bool,
}

#[derive(Clone, Debug)]
struct PendingHttpRequest {
	actor_id: String,
	method: String,
	path: String,
	headers: HashMap<String, String>,
	body: Vec<u8>,
}

#[derive(Clone, Debug)]
struct WebSocketRuntimeState {
	actor_id: String,
	generation: u32,
	actor_name: String,
	path: String,
	headers: HashMap<String, String>,
	is_hibernatable: bool,
	is_restoring_hibernatable: bool,
	server_message_index: u16,
}

#[derive(Clone, Copy, Eq)]
struct TunnelRequestKey {
	gateway_id: [u8; 4],
	request_id: [u8; 4],
}

impl PartialEq for TunnelRequestKey {
	fn eq(&self, other: &Self) -> bool {
		self.gateway_id == other.gateway_id && self.request_id == other.request_id
	}
}

impl Hash for TunnelRequestKey {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.gateway_id.hash(state);
		self.request_id.hash(state);
	}
}

impl RunnerInner {
	async fn run(self: Arc<Self>) -> Result<()> {
		let mut reconnect_delay_ms = RECONNECT_INITIAL_DELAY_MS;

		while !self.shutdown.load(Ordering::SeqCst) {
			let disconnect_info = self.run_connection_loop().await;

			match disconnect_info {
				Ok(Some((code, reason))) => {
					if let Err(err) = self
						.app
						.on_disconnected(
							RunnerHandle {
								inner: self.clone(),
							},
							code,
							reason,
						)
						.await
					{
						tracing::error!(?err, "runner app on_disconnected callback failed");
					}
				}
				Ok(None) => {}
				Err(err) => {
					tracing::warn!(?err, "connection loop failed");
				}
			}

			if self.shutdown.load(Ordering::SeqCst) {
				break;
			}

			tokio::time::sleep(Duration::from_millis(reconnect_delay_ms)).await;
			reconnect_delay_ms = (reconnect_delay_ms.saturating_mul(2)).min(RECONNECT_MAX_DELAY_MS);
		}

		Ok(())
	}

	async fn run_connection_loop(&self) -> Result<Option<(u16, String)>> {
		let ws_url = self.build_ws_url();
		let ws_stream = self.connect_websocket(&ws_url).await?;

		let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<rp::ToServer>();
		*self.ws_sender.lock().await = Some(ws_tx.clone());

		let (mut ws_write, mut ws_read) = ws_stream.split();

		let writer = tokio::spawn(async move {
			while let Some(message) = ws_rx.recv().await {
				let encoded = protocol::encode_to_server(message);
				if let Err(err) = ws_write.send(Message::Binary(encoded.into())).await {
					return Err::<(), anyhow::Error>(err.into());
				}
			}
			Ok(())
		});

		// Send init as soon as the socket is ready.
		ws_tx
			.send(self.build_init_message()?)
			.map_err(|_| anyhow!("failed to queue init message"))?;

		let mut ack_interval = tokio::time::interval(COMMAND_ACK_INTERVAL);
		let mut disconnect_info = None;

		loop {
			tokio::select! {
				_ = self.shutdown_notify.notified() => {
					let _ = ws_tx.send(rp::ToServer::ToServerStopping);
					break;
				}
				_ = ack_interval.tick() => {
					if let Err(err) = self.send_command_acknowledgement().await {
						tracing::warn!(?err, "failed to send command acknowledgement");
					}
				}
				incoming = ws_read.next() => {
					match incoming {
						Some(Ok(Message::Binary(payload))) => {
							self.handle_incoming_message(&payload).await?;
						}
						Some(Ok(Message::Close(frame))) => {
							let (code, reason) = frame
								.map(|f| (u16::from(f.code), f.reason.to_string()))
								.unwrap_or((1000, String::new()));
							disconnect_info = Some((code, reason));
							break;
						}
						Some(Ok(_)) => {}
						Some(Err(err)) => {
							return Err(err.into());
						}
						None => {
							disconnect_info = Some((1006, "connection closed".to_string()));
							break;
						}
					}
				}
			}
		}

		*self.ws_sender.lock().await = None;
		drop(ws_tx);

		if let Err(err) = writer.await {
			tracing::warn!(?err, "writer task join error");
		}

		Ok(disconnect_info)
	}

	async fn connect_websocket(
		&self,
		ws_url: &str,
	) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
		use tokio_tungstenite::tungstenite::client::IntoClientRequest;

		let mut request = ws_url
			.into_client_request()
			.context("failed to build websocket request")?;

		let protocol_header = format!("rivet, rivet_token.{}", self.config.token);
		request.headers_mut().insert(
			"Sec-WebSocket-Protocol",
			protocol_header.parse().context("invalid protocol header")?,
		);

		let (ws_stream, _response) = connect_async(request)
			.await
			.context("failed to connect to pegboard websocket")?;
		Ok(ws_stream)
	}

	fn build_ws_url(&self) -> String {
		let endpoint = self
			.config
			.pegboard_endpoint
			.as_ref()
			.unwrap_or(&self.config.endpoint)
			.replace("http://", "ws://")
			.replace("https://", "wss://");

		let base = endpoint.trim_end_matches('/');
		format!(
			"{base}/runners/connect?protocol_version={}&namespace={}&runner_key={}",
			protocol::PROTOCOL_VERSION,
			encode(&self.config.namespace),
			encode(&self.config.runner_key)
		)
	}

	fn build_init_message(&self) -> Result<rp::ToServer> {
		let prepopulate_actor_names = if self.config.prepopulate_actor_names.is_empty() {
			None
		} else {
			let mut map = HashMap::new();
			for (name, entry) in &self.config.prepopulate_actor_names {
				map.insert(
					name.clone(),
					rp::ActorName {
				metadata: serde_json::to_string(&entry.metadata)?,
					},
				);
			}
			Some(map.into())
		};

		let metadata = self
			.config
			.metadata
			.as_ref()
			.map(serde_json::to_string)
			.transpose()?;

		Ok(rp::ToServer::ToServerInit(rp::ToServerInit {
			name: self.config.runner_name.clone(),
			version: self.config.version,
			total_slots: self.config.total_slots,
			prepopulate_actor_names,
			metadata,
		}))
	}

	async fn handle_incoming_message(&self, payload: &[u8]) -> Result<()> {
		let message = protocol::decode_to_client(payload, protocol::PROTOCOL_VERSION)?;

		match message {
			rp::ToClient::ToClientInit(init) => {
				*self.runner_id.lock().await = Some(init.runner_id);
				self.ready_notify.notify_waiters();
				self.resend_unacknowledged_events().await?;
				self.app
					.on_connected(RunnerHandle {
						inner: Arc::new(self.clone_for_handle()),
					})
					.await
					.context("runner app on_connected callback failed")?;
			}
			rp::ToClient::ToClientCommands(commands) => {
				self.handle_commands(commands).await?;
			}
			rp::ToClient::ToClientAckEvents(ack) => {
				self.handle_ack_events(ack).await;
			}
			rp::ToClient::ToClientKvResponse(response) => {
				self.handle_kv_response(response).await;
			}
			rp::ToClient::ToClientTunnelMessage(message) => {
				self.handle_tunnel_message(message).await?;
			}
			rp::ToClient::ToClientPing(ping) => {
				self.send_to_server(rp::ToServer::ToServerPong(rp::ToServerPong { ts: ping.ts }))
					.await?;
			}
		}

		Ok(())
	}

	fn clone_for_handle(&self) -> RunnerInner {
		self.clone()
	}

	async fn handle_commands(&self, commands: Vec<rp::CommandWrapper>) -> Result<()> {
		for command in commands {
			let checkpoint = command.checkpoint.clone();
			match command.inner {
				rp::Command::CommandStartActor(start) => {
					self.handle_command_start_actor(checkpoint, start).await?;
				}
				rp::Command::CommandStopActor => {
					self.handle_command_stop_actor(checkpoint).await?;
				}
			}
		}

		Ok(())
	}

	async fn handle_command_start_actor(
		&self,
		checkpoint: rp::ActorCheckpoint,
		start: rp::CommandStartActor,
	) -> Result<()> {
		let actor_id = checkpoint.actor_id.clone();
		let hibernating_requests = start
			.hibernating_requests
			.into_iter()
			.map(|x| HibernatingRequest {
				gateway_id: x.gateway_id,
				request_id: x.request_id,
			})
			.collect::<Vec<_>>();

		let actor_config = ActorConfig {
			name: start.config.name,
			key: start.config.key,
			create_ts: start.config.create_ts,
			input: start.config.input,
		};

		let ctx = ActorContext {
			actor_id: actor_id.clone(),
			generation: checkpoint.generation,
			actor_name: actor_config.name.clone(),
			config: actor_config.clone(),
			hibernating_requests: hibernating_requests.clone(),
		};

		let state = ActorRuntimeState {
			generation: checkpoint.generation,
			config: actor_config,
			started: false,
			start_notify: Arc::new(Notify::new()),
			last_command_idx: checkpoint.index,
			next_event_idx: 0,
			event_history: Vec::new(),
			hibernating_requests,
			hibernation_restored: false,
		};

		self.actors.lock().await.insert(actor_id.clone(), state);

		let runner_handle = RunnerHandle {
			inner: Arc::new(self.clone_for_handle()),
		};
		let app = self.app.clone();
		let inner = Arc::new(self.clone_for_handle());

		tokio::spawn(async move {
			let result = app.on_actor_start(runner_handle.clone(), ctx.clone()).await;

			match result {
				Ok(()) => {
					if let Err(err) = inner.mark_actor_started(&ctx.actor_id, ctx.generation).await {
						tracing::error!(?err, "failed to mark actor as started");
						return;
					}

					if let Err(err) = inner
						.send_actor_state_update(
							&ctx.actor_id,
							ctx.generation,
							rp::ActorState::ActorStateRunning,
						)
						.await
					{
						tracing::error!(?err, "failed to send actor running state");
					}

					let _ = inner.lifecycle_tx.send(ActorLifecycleEvent::Started {
						actor_id: ctx.actor_id.clone(),
						generation: ctx.generation,
					});
				}
				Err(err) => {
					tracing::error!(?err, actor_id = %ctx.actor_id, "actor start callback failed");

					let _ = inner
						.send_actor_state_update(
							&ctx.actor_id,
							ctx.generation,
							rp::ActorState::ActorStateStopped(rp::ActorStateStopped {
								code: rp::StopCode::Error,
								message: Some(err.to_string()),
							}),
						)
							.await;

					inner.remove_actor_websockets(&ctx.actor_id).await;
					inner.actors.lock().await.remove(&ctx.actor_id);
				}
			}
		});

		Ok(())
	}

	async fn handle_command_stop_actor(&self, checkpoint: rp::ActorCheckpoint) -> Result<()> {
		let (actor_name, config, hibernating_requests) = {
			let actors = self.actors.lock().await;
			let state = actors
				.get(&checkpoint.actor_id)
				.with_context(|| format!("actor {} not found", checkpoint.actor_id))?;
			(
				state.config.name.clone(),
				state.config.clone(),
				state.hibernating_requests.clone(),
			)
		};

		let ctx = ActorContext {
			actor_id: checkpoint.actor_id.clone(),
			generation: checkpoint.generation,
			actor_name,
			config,
			hibernating_requests,
		};

		self.app
			.on_actor_stop(
				RunnerHandle {
					inner: Arc::new(self.clone_for_handle()),
				},
				ctx.clone(),
			)
			.await
			.context("actor stop callback failed")?;

		self.send_actor_state_update(
			&checkpoint.actor_id,
			checkpoint.generation,
			rp::ActorState::ActorStateStopped(rp::ActorStateStopped {
				code: rp::StopCode::Ok,
				message: None,
			}),
		)
		.await?;

		self.remove_actor_websockets(&checkpoint.actor_id).await;
		self.actors.lock().await.remove(&checkpoint.actor_id);

		let _ = self.lifecycle_tx.send(ActorLifecycleEvent::Stopped {
			actor_id: checkpoint.actor_id,
			generation: checkpoint.generation,
		});

		Ok(())
	}

	async fn mark_actor_started(&self, actor_id: &str, generation: u32) -> Result<()> {
		let mut actors = self.actors.lock().await;
		let state = actors
			.get_mut(actor_id)
			.with_context(|| format!("actor {actor_id} not found"))?;
		if state.generation != generation {
			bail!("actor generation mismatch");
		}
		state.started = true;
		state.start_notify.notify_waiters();
		Ok(())
	}

	async fn wait_for_actor_started(
		&self,
		actor_id: &str,
	) -> Option<(u32, ActorConfig)> {
		loop {
			let notify = {
				let actors = self.actors.lock().await;
				let state = actors.get(actor_id)?;
				if state.started {
					return Some((state.generation, state.config.clone()));
				}
				state.start_notify.clone()
			};

			notify.notified().await;
		}
	}

	async fn send_actor_intent(
		&self,
		actor_id: &str,
		generation: Option<u32>,
		intent: rp::ActorIntent,
	) -> Result<()> {
		let generation = self.resolve_actor_generation(actor_id, generation).await?;
		self.emit_event(
			actor_id,
			generation,
			rp::Event::EventActorIntent(rp::EventActorIntent { intent }),
		)
		.await
	}

	async fn send_alarm_event(
		&self,
		actor_id: &str,
		generation: Option<u32>,
		alarm_ts: Option<i64>,
	) -> Result<()> {
		let generation = self.resolve_actor_generation(actor_id, generation).await?;
		self.emit_event(
			actor_id,
			generation,
			rp::Event::EventActorSetAlarm(rp::EventActorSetAlarm { alarm_ts }),
		)
		.await
	}

	async fn resolve_actor_generation(
		&self,
		actor_id: &str,
		generation: Option<u32>,
	) -> Result<u32> {
		let actors = self.actors.lock().await;
		let actor = actors
			.get(actor_id)
			.with_context(|| format!("actor {actor_id} not found"))?;
		let generation = generation.unwrap_or(actor.generation);
		if generation != actor.generation {
			bail!(
				"actor generation mismatch, expected {}, got {}",
				actor.generation,
				generation
			)
		}
		Ok(generation)
	}

	async fn send_actor_state_update(
		&self,
		actor_id: &str,
		generation: u32,
		state: rp::ActorState,
	) -> Result<()> {
		self.emit_event(
			actor_id,
			generation,
			rp::Event::EventActorStateUpdate(rp::EventActorStateUpdate { state }),
		)
		.await
	}

	async fn emit_event(&self, actor_id: &str, generation: u32, event: rp::Event) -> Result<()> {
		let wrapper = {
			let mut actors = self.actors.lock().await;
			let actor = actors
				.get_mut(actor_id)
				.with_context(|| format!("actor {actor_id} not found"))?;
			let index = actor.next_event_idx;
			actor.next_event_idx += 1;

			let wrapper = rp::EventWrapper {
				checkpoint: rp::ActorCheckpoint {
					actor_id: actor_id.to_string(),
					generation,
					index,
				},
				inner: event,
			};
			actor.event_history.push(wrapper.clone());
			wrapper
		};

		self.send_to_server(rp::ToServer::ToServerEvents(vec![wrapper]))
			.await
	}

	async fn send_command_acknowledgement(&self) -> Result<()> {
		let checkpoints = {
			let actors = self.actors.lock().await;
			actors
				.iter()
				.filter(|(_, actor)| actor.last_command_idx >= 0)
				.map(|(actor_id, actor)| rp::ActorCheckpoint {
					actor_id: actor_id.clone(),
					generation: actor.generation,
					index: actor.last_command_idx,
				})
				.collect::<Vec<_>>()
		};

		self.send_to_server(rp::ToServer::ToServerAckCommands(rp::ToServerAckCommands {
			last_command_checkpoints: checkpoints,
		}))
		.await
	}

	async fn resend_unacknowledged_events(&self) -> Result<()> {
		let events = {
			let actors = self.actors.lock().await;
			actors
				.values()
				.flat_map(|x| x.event_history.clone())
				.collect::<Vec<_>>()
		};

		if events.is_empty() {
			return Ok(());
		}

		self.send_to_server(rp::ToServer::ToServerEvents(events)).await
	}

	async fn handle_ack_events(&self, ack: rp::ToClientAckEvents) {
		let mut actors = self.actors.lock().await;
		for (actor_id, actor) in actors.iter_mut() {
			if let Some(checkpoint) = ack
				.last_event_checkpoints
				.iter()
				.find(|x| x.actor_id == *actor_id)
			{
				actor.event_history.retain(|entry| {
					entry.checkpoint.generation != checkpoint.generation
						|| entry.checkpoint.index > checkpoint.index
				});
			}
		}
	}

	async fn send_kv_request(&self, actor_id: &str, data: rp::KvRequestData) -> Result<rp::KvResponseData> {
		let request_id = self.next_kv_request_id.fetch_add(1, Ordering::SeqCst);
		let (tx, rx) = oneshot::channel();

		self.pending_kv_requests.lock().await.insert(request_id, tx);

		self.send_to_server(rp::ToServer::ToServerKvRequest(rp::ToServerKvRequest {
			actor_id: actor_id.to_string(),
			request_id,
			data,
		}))
		.await?;

		let response = tokio::time::timeout(Duration::from_secs(30), rx)
			.await
			.context("timed out waiting for kv response")?
			.context("kv response channel closed")?;

		Ok(response)
	}

	async fn handle_kv_response(&self, response: rp::ToClientKvResponse) {
		let sender = self
			.pending_kv_requests
			.lock()
			.await
			.remove(&response.request_id);
		if let Some(sender) = sender {
			let _ = sender.send(response.data);
		}
	}

	async fn remove_actor_websockets(&self, actor_id: &str) {
		let mut websockets = self.websockets.lock().await;
		websockets.retain(|_, ws| ws.actor_id != actor_id);
	}

	async fn handle_tunnel_message(&self, message: rp::ToClientTunnelMessage) -> Result<()> {
		let incoming_message_index = message.message_id.message_index;
		let key = TunnelRequestKey {
			gateway_id: message.message_id.gateway_id,
			request_id: message.message_id.request_id,
		};

		self.ensure_tunnel_index(key, incoming_message_index)
			.await;

		match message.message_kind {
			rp::ToClientTunnelMessageKind::ToClientRequestStart(req) => {
				self.handle_request_start(key, req).await?;
			}
			rp::ToClientTunnelMessageKind::ToClientRequestChunk(chunk) => {
				self.handle_request_chunk(key, chunk).await?;
			}
			rp::ToClientTunnelMessageKind::ToClientRequestAbort => {
				self.pending_http_requests.lock().await.remove(&key);
			}
			rp::ToClientTunnelMessageKind::ToClientWebSocketOpen(ws) => {
				self.handle_websocket_open(key, ws).await?;
			}
			rp::ToClientTunnelMessageKind::ToClientWebSocketMessage(message) => {
				self.handle_websocket_message(key, incoming_message_index, message)
					.await?;
			}
			rp::ToClientTunnelMessageKind::ToClientWebSocketClose(close) => {
				self.handle_websocket_close(key, close).await?;
			}
		}

		Ok(())
	}

	async fn handle_request_start(
		&self,
		key: TunnelRequestKey,
		req: rp::ToClientRequestStart,
	) -> Result<()> {
		if !req.stream {
			let pending = PendingHttpRequest {
				actor_id: req.actor_id,
				method: req.method,
				path: req.path,
				headers: req.headers.into(),
				body: req.body.unwrap_or_default(),
			};
			self.spawn_http_request(key, pending);
			return Ok(());
		}

		self.pending_http_requests.lock().await.insert(
			key,
			PendingHttpRequest {
				actor_id: req.actor_id,
				method: req.method,
				path: req.path,
				headers: req.headers.into(),
				body: req.body.unwrap_or_default(),
			},
		);
		Ok(())
	}

	async fn handle_request_chunk(
		&self,
		key: TunnelRequestKey,
		chunk: rp::ToClientRequestChunk,
	) -> Result<()> {
		let mut requests = self.pending_http_requests.lock().await;
		let Some(request) = requests.get_mut(&key) else {
			return Ok(());
		};

		request.body.extend_from_slice(&chunk.body);
		if chunk.finish {
			let request = requests.remove(&key).context("request removed unexpectedly")?;
			drop(requests);
			self.spawn_http_request(key, request);
		}

		Ok(())
	}

	fn spawn_http_request(&self, key: TunnelRequestKey, request: PendingHttpRequest) {
		let inner = Arc::new(self.clone_for_handle());
		tokio::spawn(async move {
			if let Err(err) = inner.process_http_request(key, request).await {
				tracing::error!(?err, "http tunnel request failed");
			}
		});
	}

	async fn process_http_request(&self, key: TunnelRequestKey, pending: PendingHttpRequest) -> Result<()> {
		let Some((generation, actor_config)) = self.wait_for_actor_started(&pending.actor_id).await else {
			self.send_response_error(key, 503, "runner.actor_not_found", "Actor not found")
				.await?;
			return Ok(());
		};

		let request = build_http_request(
			pending.method.clone(),
			pending.path.clone(),
			pending.headers.clone(),
			pending.body,
		)?;

		let ctx = HttpContext {
			actor_id: pending.actor_id.clone(),
			generation,
			actor_name: actor_config.name.clone(),
			gateway_id: key.gateway_id,
			request_id: key.request_id,
		};

		let response = match self
			.app
			.fetch(
				RunnerHandle {
					inner: Arc::new(self.clone_for_handle()),
				},
				ctx,
				request,
			)
			.await
		{
			Ok(response) => response,
			Err(err) => {
				tracing::error!(?err, "fetch callback failed");
				self.send_response_error(key, 500, "runner.internal", "Internal Server Error")
					.await?;
				return Ok(());
			}
		};

		self.send_response_start(key, response).await?;
		Ok(())
	}

	async fn handle_websocket_open(
		&self,
		key: TunnelRequestKey,
		open: rp::ToClientWebSocketOpen,
	) -> Result<()> {
		let Some((generation, actor_config)) = self.wait_for_actor_started(&open.actor_id).await else {
			self.send_tunnel_message(
				key,
				rp::ToServerTunnelMessageKind::ToServerWebSocketClose(rp::ToServerWebSocketClose {
					code: Some(1011),
					reason: Some("runner.actor_not_found".to_string()),
					hibernate: false,
				}),
			)
				.await?;
			return Ok(());
		};

		let ws_ctx = WebSocketContext {
			actor_id: open.actor_id.clone(),
			generation,
			actor_name: actor_config.name.clone(),
			gateway_id: key.gateway_id,
			request_id: key.request_id,
			path: open.path.clone(),
			headers: open.headers.clone().into(),
			is_hibernatable: false, // Set after can_hibernate is computed.
			is_restoring_hibernatable: false,
		};

		let can_hibernate = self.app.can_hibernate(&ws_ctx);
		let mut ws_ctx = ws_ctx;
		ws_ctx.is_hibernatable = can_hibernate;

		self.websockets.lock().await.insert(
			key,
			WebSocketRuntimeState {
				actor_id: open.actor_id,
				generation,
				actor_name: actor_config.name,
				path: open.path,
				headers: open.headers.into(),
				is_hibernatable: can_hibernate,
				is_restoring_hibernatable: false,
				server_message_index: 0,
			},
		);

		if let Err(err) = self
			.app
			.websocket(
				RunnerHandle {
					inner: Arc::new(self.clone_for_handle()),
				},
				ws_ctx,
			)
			.await
		{
			self.websockets.lock().await.remove(&key);
			self.send_tunnel_message(
				key,
				rp::ToServerTunnelMessageKind::ToServerWebSocketClose(rp::ToServerWebSocketClose {
					code: Some(1011),
					reason: Some("ws.open_error".to_string()),
					hibernate: false,
				}),
			)
				.await?;
			bail!("websocket callback failed: {err}");
		}

		self.send_tunnel_message(
			key,
			rp::ToServerTunnelMessageKind::ToServerWebSocketOpen(rp::ToServerWebSocketOpen {
				can_hibernate,
			}),
		)
		.await?;

		Ok(())
	}

	async fn handle_websocket_message(
		&self,
		key: TunnelRequestKey,
		message_index: u16,
		message: rp::ToClientWebSocketMessage,
	) -> Result<()> {
		let maybe_state = {
			let mut websockets = self.websockets.lock().await;
			let Some(state) = websockets.get_mut(&key) else {
				return Ok(());
			};

			if state.is_hibernatable {
				if wrapping_lte_u16(message_index, state.server_message_index) {
					return Ok(());
				}

				let expected = wrapping_add_u16(state.server_message_index, 1);
				if message_index != expected {
					let _ = self
						.send_tunnel_message(
							key,
							rp::ToServerTunnelMessageKind::ToServerWebSocketClose(
								rp::ToServerWebSocketClose {
									code: Some(1008),
									reason: Some("ws.message_index_skip".to_string()),
									hibernate: false,
								},
							),
						)
						.await;
					websockets.remove(&key);
					return Ok(());
				}

				state.server_message_index = message_index;
			}

			Some(state.clone())
		};

		let Some(state) = maybe_state else {
			return Ok(());
		};

		let ctx = self.websocket_context_from_state(key, &state);
		let callback_result = self
			.app
			.websocket_message(
				RunnerHandle {
					inner: Arc::new(self.clone_for_handle()),
				},
				ctx,
				WebSocketMessage {
					data: message.data,
					binary: message.binary,
					message_index,
				},
			)
			.await;

		if let Err(err) = callback_result {
			tracing::warn!(?err, "websocket message callback failed");
			self.websockets.lock().await.remove(&key);
			self.send_tunnel_message(
				key,
				rp::ToServerTunnelMessageKind::ToServerWebSocketClose(rp::ToServerWebSocketClose {
					code: Some(1011),
					reason: Some("ws.message_error".to_string()),
					hibernate: false,
				}),
			)
			.await?;
		}

		Ok(())
	}

	async fn handle_websocket_close(
		&self,
		key: TunnelRequestKey,
		close: rp::ToClientWebSocketClose,
	) -> Result<()> {
		let state = self.websockets.lock().await.remove(&key);
		let Some(state) = state else {
			return Ok(());
		};

		let ctx = self.websocket_context_from_state(key, &state);
		self.app
			.websocket_close(
				RunnerHandle {
					inner: Arc::new(self.clone_for_handle()),
				},
				ctx,
				close.code,
				close.reason,
			)
			.await
			.context("websocket close callback failed")
	}

	fn websocket_context_from_state(
		&self,
		key: TunnelRequestKey,
		state: &WebSocketRuntimeState,
	) -> WebSocketContext {
		WebSocketContext {
			actor_id: state.actor_id.clone(),
			generation: state.generation,
			actor_name: state.actor_name.clone(),
			gateway_id: key.gateway_id,
			request_id: key.request_id,
			path: state.path.clone(),
			headers: state.headers.clone(),
			is_hibernatable: state.is_hibernatable,
			is_restoring_hibernatable: state.is_restoring_hibernatable,
		}
	}

	async fn restore_hibernating_requests(
		&self,
		actor_id: &str,
		meta_entries: Vec<HibernatingWebSocketMetadata>,
	) -> Result<()> {
		let (generation, actor_name, connected_requests) = {
			let mut actors = self.actors.lock().await;
			let state = actors
				.get_mut(actor_id)
				.with_context(|| format!("actor {actor_id} not found"))?;
			if state.hibernation_restored {
				bail!("actor {actor_id} already restored hibernating requests");
			}
			state.hibernation_restored = true;
			(
				state.generation,
				state.config.name.clone(),
				state.hibernating_requests.clone(),
			)
		};

		for connected in &connected_requests {
			let key = TunnelRequestKey {
				gateway_id: connected.gateway_id,
				request_id: connected.request_id,
			};

			let meta = meta_entries.iter().find(|entry| {
				entry.gateway_id == connected.gateway_id && entry.request_id == connected.request_id
			});

			let Some(meta) = meta else {
				self.send_tunnel_message(
					key,
					rp::ToServerTunnelMessageKind::ToServerWebSocketClose(rp::ToServerWebSocketClose {
						code: Some(1000),
						reason: Some("ws.meta_not_found_during_restore".to_string()),
						hibernate: false,
					}),
				)
				.await?;
				continue;
			};

			self.set_tunnel_index(key, meta.client_message_index).await;
			let ws_state = WebSocketRuntimeState {
				actor_id: actor_id.to_string(),
				generation,
				actor_name: actor_name.clone(),
				path: meta.path.clone(),
				headers: meta.headers.clone(),
				is_hibernatable: true,
				is_restoring_hibernatable: true,
				server_message_index: meta.server_message_index,
			};
			self.websockets.lock().await.insert(key, ws_state.clone());

			let ws_ctx = self.websocket_context_from_state(key, &ws_state);
			if let Err(err) = self
				.app
				.websocket(
					RunnerHandle {
						inner: Arc::new(self.clone_for_handle()),
					},
					ws_ctx,
				)
				.await
			{
				tracing::warn!(?err, actor_id, "error restoring websocket");
				self.websockets.lock().await.remove(&key);
				self.send_tunnel_message(
					key,
					rp::ToServerTunnelMessageKind::ToServerWebSocketClose(rp::ToServerWebSocketClose {
						code: Some(1011),
						reason: Some("ws.restore_error".to_string()),
						hibernate: false,
					}),
				)
				.await?;
			}
		}

		for meta in &meta_entries {
			let is_connected = connected_requests.iter().any(|request| {
				request.gateway_id == meta.gateway_id && request.request_id == meta.request_id
			});
			if is_connected {
				continue;
			}

			let key = TunnelRequestKey {
				gateway_id: meta.gateway_id,
				request_id: meta.request_id,
			};
			let ws_ctx = WebSocketContext {
				actor_id: actor_id.to_string(),
				generation,
				actor_name: actor_name.clone(),
				gateway_id: meta.gateway_id,
				request_id: meta.request_id,
				path: meta.path.clone(),
				headers: meta.headers.clone(),
				is_hibernatable: true,
				is_restoring_hibernatable: true,
			};
			self.app
				.websocket_close(
					RunnerHandle {
						inner: Arc::new(self.clone_for_handle()),
					},
					ws_ctx,
					Some(1000),
					Some("ws.stale_metadata".to_string()),
				)
				.await
				.context("stale websocket close callback failed")?;
			self.websockets.lock().await.remove(&key);
		}

		Ok(())
	}

	async fn send_response_start(&self, key: TunnelRequestKey, response: Response<Bytes>) -> Result<()> {
		let status = response.status().as_u16();
		let (parts, body) = response.into_parts();

		let mut headers = HashMap::new();
		for (name, value) in &parts.headers {
			if let Ok(value) = value.to_str() {
				headers.insert(name.to_string(), value.to_string());
			}
		}

		if !headers.contains_key("content-length") {
			headers.insert("content-length".to_string(), body.len().to_string());
		}

		self.send_tunnel_message(
			key,
			rp::ToServerTunnelMessageKind::ToServerResponseStart(rp::ToServerResponseStart {
				status,
				headers: headers.into(),
				body: Some(body.to_vec()),
				stream: false,
			}),
		)
		.await
	}

	async fn send_response_error(
		&self,
		key: TunnelRequestKey,
		status: u16,
		error_code: &str,
		message: &str,
	) -> Result<()> {
		let mut headers = HashMap::new();
		headers.insert("content-type".to_string(), "text/plain".to_string());
		headers.insert("x-rivet-error".to_string(), error_code.to_string());

		self.send_tunnel_message(
			key,
			rp::ToServerTunnelMessageKind::ToServerResponseStart(rp::ToServerResponseStart {
				status,
				headers: headers.into(),
				body: Some(message.as_bytes().to_vec()),
				stream: false,
			}),
		)
		.await
	}

	async fn ensure_tunnel_index(&self, key: TunnelRequestKey, incoming_index: u16) {
		let mut indices = self.tunnel_message_indices.lock().await;
		indices.entry(key).or_insert(incoming_index);
	}

	async fn set_tunnel_index(&self, key: TunnelRequestKey, next_index: u16) {
		let mut indices = self.tunnel_message_indices.lock().await;
		indices.insert(key, next_index);
	}

	async fn send_tunnel_message(
		&self,
		key: TunnelRequestKey,
		message_kind: rp::ToServerTunnelMessageKind,
	) -> Result<()> {
		let message_index = {
			let mut indices = self.tunnel_message_indices.lock().await;
			let idx = indices.entry(key).or_insert(0);
			let current = *idx;
			*idx = idx.wrapping_add(1);
			current
		};

		self.send_to_server(rp::ToServer::ToServerTunnelMessage(rp::ToServerTunnelMessage {
			message_id: rp::MessageId {
				gateway_id: key.gateway_id,
				request_id: key.request_id,
				message_index,
			},
			message_kind,
		}))
		.await
	}

	async fn send_to_server(&self, message: rp::ToServer) -> Result<()> {
		let sender = self.ws_sender.lock().await.clone();
		let Some(sender) = sender else {
			bail!("runner websocket sender unavailable")
		};

		sender
			.send(message)
			.map_err(|_| anyhow!("failed to queue message to websocket writer"))
	}
}

fn build_http_request(
	method: String,
	path: String,
	headers: HashMap<String, String>,
	body: Vec<u8>,
) -> Result<Request<Bytes>> {
	let uri = if path.starts_with('/') {
		format!("http://actor{path}")
	} else {
		format!("http://actor/{path}")
	};

	let mut builder = Request::builder()
		.method(method.parse::<http::Method>().context("invalid method")?)
		.uri(uri.parse::<http::Uri>().context("invalid uri")?);

	for (name, value) in headers {
		builder = builder.header(name, value);
	}

	Ok(builder.body(Bytes::from(body))?)
}

fn wrapping_add_u16(a: u16, b: u16) -> u16 {
	a.wrapping_add(b)
}

fn wrapping_sub_u16(a: u16, b: u16) -> u16 {
	a.wrapping_sub(b)
}

fn wrapping_lt_u16(a: u16, b: u16) -> bool {
	a != b && wrapping_sub_u16(b, a) < (u16::MAX / 2)
}

fn wrapping_lte_u16(a: u16, b: u16) -> bool {
	a == b || wrapping_lt_u16(a, b)
}

#[derive(Clone, Debug)]
pub struct ServerlessConfig {
	pub runner: RunnerConfig,
	pub max_runners: u32,
	pub slots_per_runner: u32,
	pub request_lifespan: u32,
}

impl ServerlessConfig {
	pub fn builder() -> ServerlessConfigBuilder {
		ServerlessConfigBuilder::default()
	}
}

#[derive(Default)]
pub struct ServerlessConfigBuilder {
	runner: RunnerConfigBuilder,
	max_runners: Option<u32>,
	slots_per_runner: Option<u32>,
	request_lifespan: Option<u32>,
}

impl ServerlessConfigBuilder {
	pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
		self.runner = self.runner.endpoint(endpoint);
		self
	}

	pub fn token(mut self, token: impl Into<String>) -> Self {
		self.runner = self.runner.token(token);
		self
	}

	pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
		self.runner = self.runner.namespace(namespace);
		self
	}

	pub fn runner_name(mut self, runner_name: impl Into<String>) -> Self {
		self.runner = self.runner.runner_name(runner_name);
		self
	}

	pub fn runner_key(mut self, runner_key: impl Into<String>) -> Self {
		self.runner = self.runner.runner_key(runner_key);
		self
	}

	pub fn version(mut self, version: u32) -> Self {
		self.runner = self.runner.version(version);
		self
	}

	pub fn total_slots(mut self, total_slots: u32) -> Self {
		self.runner = self.runner.total_slots(total_slots);
		self
	}

	pub fn pegboard_endpoint(mut self, endpoint: impl Into<String>) -> Self {
		self.runner = self.runner.pegboard_endpoint(endpoint);
		self
	}

	pub fn prepopulate_actor_name(
		mut self,
		name: impl Into<String>,
		metadata: serde_json::Value,
	) -> Self {
		self.runner = self.runner.prepopulate_actor_name(name, metadata);
		self
	}

	pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
		self.runner = self.runner.metadata(metadata);
		self
	}

	pub fn max_runners(mut self, max_runners: u32) -> Self {
		self.max_runners = Some(max_runners);
		self
	}

	pub fn slots_per_runner(mut self, slots_per_runner: u32) -> Self {
		self.slots_per_runner = Some(slots_per_runner);
		self
	}

	pub fn request_lifespan(mut self, request_lifespan: u32) -> Self {
		self.request_lifespan = Some(request_lifespan);
		self
	}

	pub fn build(self) -> Result<ServerlessConfig> {
		Ok(ServerlessConfig {
			runner: self.runner.build()?,
			max_runners: self.max_runners.unwrap_or(1000),
			slots_per_runner: self.slots_per_runner.unwrap_or(1),
			request_lifespan: self.request_lifespan.unwrap_or(300),
		})
	}
}

pub struct ServerlessRunnerBuilder {
	config: ServerlessConfig,
	app: Option<Arc<dyn RunnerApp>>,
}

impl ServerlessRunnerBuilder {
	pub fn app<A>(mut self, app: A) -> Self
	where
		A: RunnerApp,
	{
		self.app = Some(Arc::new(app));
		self
	}

	pub fn app_arc(mut self, app: Arc<dyn RunnerApp>) -> Self {
		self.app = Some(app);
		self
	}

	pub fn build(self) -> Result<ServerlessRunner> {
		let app = self
			.app
			.context("runner app is required; call ServerlessRunnerBuilder::app")?;
		let runner = Runner::builder(self.config.runner.clone())
			.app_arc(app)
			.build()?;
		Ok(ServerlessRunner {
			runner,
			config: self.config,
		})
	}
}

#[derive(Clone)]
pub struct ServerlessRunner {
	runner: Runner,
	config: ServerlessConfig,
}

impl ServerlessRunner {
	pub fn builder(config: ServerlessConfig) -> ServerlessRunnerBuilder {
		ServerlessRunnerBuilder { config, app: None }
	}

	pub fn runner(&self) -> Runner {
		self.runner.clone()
	}

	pub fn axum_routes(self: Arc<Self>) -> axum::Router {
		let state = self.clone();
		axum::Router::new()
			.route("/api/rivet/start", axum::routing::get(serverless_start))
			.route("/start", axum::routing::get(serverless_start))
			.route("/api/rivet/metadata", axum::routing::get(serverless_metadata))
			.route("/metadata", axum::routing::get(serverless_metadata))
			.with_state(state)
	}

	pub async fn upsert_serverless_runner_config(&self, public_url: &str) -> Result<()> {
		let endpoint = self.config.runner.endpoint.trim_end_matches('/');
		let url = format!(
			"{endpoint}/runner-configs/{}?namespace={}",
			encode(&self.config.runner.runner_name),
			encode(&self.config.runner.namespace)
		);

		let client = reqwest::Client::new();
		let response = client
			.put(url)
			.bearer_auth(&self.config.runner.token)
			.json(&self.serverless_runner_config_body(public_url, "default"))
			.send()
			.await
			.context("failed to send serverless runner config request")?;

		if response.status().is_success() {
			return Ok(());
		}

		let status = response.status();
		let body = response.text().await.unwrap_or_default();

		if status == reqwest::StatusCode::BAD_REQUEST {
			if let Some(datacenter_name) = self.resolve_datacenter_name(&client, endpoint).await? {
				if datacenter_name != "default" {
					let retry_response = client
						.put(format!(
							"{endpoint}/runner-configs/{}?namespace={}",
							encode(&self.config.runner.runner_name),
							encode(&self.config.runner.namespace)
						))
						.bearer_auth(&self.config.runner.token)
						.json(&self.serverless_runner_config_body(public_url, &datacenter_name))
						.send()
						.await
						.context("failed to retry serverless runner config request")?;

					if retry_response.status().is_success() {
						return Ok(());
					}

					let retry_status = retry_response.status();
					let retry_body = retry_response.text().await.unwrap_or_default();
					bail!("serverless runner config upsert failed: {retry_status} {retry_body}");
				}
			}
		}

		bail!("serverless runner config upsert failed: {status} {body}");
	}

	fn serverless_runner_config_body(
		&self,
		public_url: &str,
		datacenter_name: &str,
	) -> serde_json::Value {
		serde_json::json!({
			"datacenters": {
				datacenter_name: {
					"serverless": {
						"url": public_url,
						"max_runners": self.config.max_runners,
						"slots_per_runner": self.config.slots_per_runner,
						"request_lifespan": self.config.request_lifespan,
					}
				}
			}
		})
	}

	async fn resolve_datacenter_name(
		&self,
		client: &reqwest::Client,
		endpoint: &str,
	) -> Result<Option<String>> {
		let response = client
			.get(format!("{endpoint}/datacenters"))
			.bearer_auth(&self.config.runner.token)
			.send()
			.await
			.context("failed to fetch datacenters")?;

		if !response.status().is_success() {
			return Ok(None);
		}

		let body: serde_json::Value = response
			.json()
			.await
			.context("failed to decode datacenters response")?;
		let datacenter_name = body
			.get("datacenters")
			.and_then(serde_json::Value::as_array)
			.and_then(|x| x.first())
			.and_then(|x| x.get("name"))
			.and_then(serde_json::Value::as_str)
			.map(ToString::to_string);

		Ok(datacenter_name)
	}
}

async fn serverless_start(
	axum::extract::State(runner): axum::extract::State<Arc<ServerlessRunner>>,
) -> Result<
	axum::response::sse::Sse<impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>>,
	(StatusCode, String),
> {
	use axum::response::sse::{Event, Sse};

	if !runner.runner.inner.started.load(Ordering::SeqCst) {
		runner.runner.start().await.map_err(internal_error)?;
	}

	runner.runner.wait_ready().await.map_err(internal_error)?;
	let packet = runner
		.runner
		.get_serverless_init_packet()
		.await
		.map_err(internal_error)?
		.context("runner init packet unavailable")
		.map_err(internal_error)?;

	let stream = stream! {
		yield Ok::<Event, std::convert::Infallible>(Event::default().event("message").data(packet));
		let mut interval = tokio::time::interval(Duration::from_secs(15));
		loop {
			interval.tick().await;
			yield Ok::<Event, std::convert::Infallible>(Event::default().event("ping").data(""));
		}
	};

	Ok(Sse::new(stream))
}

async fn serverless_metadata(
	axum::extract::State(runner): axum::extract::State<Arc<ServerlessRunner>>,
) -> axum::Json<serde_json::Value> {
	let actor_names = runner
		.config
		.runner
		.prepopulate_actor_names
		.iter()
		.map(|(name, entry)| (name.clone(), entry.metadata.clone()))
		.collect::<serde_json::Map<String, serde_json::Value>>();

	axum::Json(serde_json::json!({
		"runtime": "rivetkit",
		"version": "1",
		"actorNames": actor_names,
		"runner": {
			"version": runner.config.runner.version,
		}
	}))
}

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
	(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
