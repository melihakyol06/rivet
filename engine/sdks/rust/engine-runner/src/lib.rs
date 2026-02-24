//! Rust engine runner SDK.
//!
//! This crate mirrors the TypeScript engine runner semantics with Rust-idiomatic
//! types and async traits.

mod actor;
mod protocol;
mod runner;

pub use actor::{
	ActorConfig, ActorContext, ActorRequestContext, AxumActorDefinition, AxumRunnerApp,
	HibernatingRequest, HibernatingWebSocketMetadata, HttpContext, RunnerApp, WebSocketContext,
	WebSocketMessage,
};
pub use protocol::PROTOCOL_VERSION;
pub use runner::{
	ActorLifecycleEvent, PrepopulateActorName, Runner, RunnerBuilder, RunnerConfig,
	RunnerConfigBuilder, RunnerHandle, ServerlessConfig, ServerlessConfigBuilder, ServerlessRunner,
	ServerlessRunnerBuilder,
};

pub use rivet_runner_protocol::mk2 as protocol_types;
