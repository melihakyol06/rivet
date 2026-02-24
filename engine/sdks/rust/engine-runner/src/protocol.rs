use anyhow::Result;
use rivet_runner_protocol as rp;
use rivet_runner_protocol::mk2 as rp2;
use vbare::OwnedVersionedData;

pub const PROTOCOL_VERSION: u16 = rp::PROTOCOL_MK2_VERSION;

/// Helper to decode messages from server (MK2)
pub fn decode_to_client(buf: &[u8], protocol_version: u16) -> Result<rp2::ToClient> {
	// Use versioned deserialization to handle protocol version properly
	<rp::versioned::ToClientMk2 as OwnedVersionedData>::deserialize(buf, protocol_version)
}

/// Helper to encode messages to server (MK2)
pub fn encode_to_server(msg: rp2::ToServer) -> Vec<u8> {
	rp::versioned::ToServerMk2::wrap_latest(msg)
		.serialize(PROTOCOL_VERSION)
		.expect("failed to serialize ToServer")
}
