use std::fs;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ed25519_consensus::SigningKey;
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tendermint_proto::p2p::packet::Sum;

#[derive(Debug, Clone)]
pub struct NodeID {
    pub priv_key: SigningKey, // Obtain the public key with priv_key.verification_key().
    pub id: String,
}

use tendermint_p2p::secret_connection::{DATA_MAX_SIZE, SecretConnection, Version};
use tendermint_proto::p2p::{DefaultNodeInfo, Packet};

use crate::message::CompleteMessage;
use crate::node::Node;
use crate::resource_manager::ResourceManager;

pub type PeerConnection = BufReader<SecretConnection<TcpStream>>;

// SecretConnection is bidirectional, but blocking reads for p0->p1 would otherwise block p1->p0 forwarding.
// Split it into read/write halves so their ownership can be managed independently.
pub fn split_connection(
    connection: PeerConnection, // Take ownership here.
) -> Result<(impl Read + Send, impl Write + Send)> {
    // Read provides methods such as read_exact(); Write provides write_all().
    // Preserve unread buffered data: BufReader may read ahead beyond the current request.
    // For example, reading NodeInfo may also pull in part of the following Packet.
    let buffered = connection.buffer().to_vec();
    let (writer, reader) = connection.into_inner().split()?; // Two handles sharing one TCP connection.
    // Prepend the old buffered data to the new reader.
    let reader =
        std::io::Cursor::new(buffered).chain(BufReader::with_capacity(DATA_MAX_SIZE, reader));
    Ok((reader, writer))
}

impl NodeID {
    // Example: cluster-id/config/node0/node_key.json
    /*
    {"priv_key":{"type":"tendermint/PrivKeyEd25519","value":"XZBDKAvX2f2kR8+DM4hJi6t9+19lNtenPikwQLvKnR1iCqsrle+wKkKlP+BatppXAF47Qk5vqVxR8kDbdBoKFQ=="}}
    */
    // implementation: https://github.com/cometbft/cometbft/blob/v0.40.0/crypto/ed25519/ed25519.go
    // In this JSON, priv_key contains 64 bytes: the first 32 are the private seed, from which
    // a 32-byte public key can be derived; the last 32 store that public key for comparison.

    pub fn load(path: &Path) -> Result<Self> {
        // Read the file into a string.
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read {}", path.display()))?;

        // Parse the string as a JSON value.
        let json: Value = content.parse()?;
        ensure!(
            json["priv_key"]["type"].as_str() == Some("tendermint/PrivKeyEd25519"),
            "Currently only support Ed25519"
        );

        // Read the private key.
        let bytes = STANDARD.decode(
            json["priv_key"]["value"]
                .as_str()
                .context("Missing priv_key.value")?,
        )?; // as_str returns Option; context turns None into Err("Missing...") so ? can propagate it.
        ensure!(bytes.len() == 64, "Expected a 64-byte Ed25519 key");

        // Take the first 32 bytes.
        let seed: [u8; 32] = bytes[..32].try_into()?;
        let priv_key = SigningKey::from(seed);

        // Derive the public key from the 32-byte seed.
        let pub_key = priv_key.verification_key();

        ensure!(
            pub_key.as_bytes().as_slice() == &bytes[32..],
            "Public key != priv_key.verification_key()"
        );

        // node id = sha256(pub_key)[..20] -> lowercase hex
        let hash = Sha256::digest(pub_key.as_bytes());
        let id = hex::encode(&hash[..20]);

        Ok(Self { priv_key, id })
    }
}

// A public key is public, so presenting it does not prove identity; the peer must prove possession of its private key.
// During the handshake, each side signs handshake-specific data and the other verifies the signature.
// Verify that:
// - The peer presents the expected public key, not just any key pair it owns.
// - The peer possesses the private key corresponding to that expected public key.
pub fn make_secret_connection(
    stream: TcpStream,
    local: &NodeID,
    expected_remote_id: &str,
) -> Result<PeerConnection> {
    // Establish an encrypted connection.
    let connection = SecretConnection::new(stream, local.priv_key.clone(), Version::V0_34)?; // Prove our identity and authenticate the peer while establishing encryption.

    let remote_id = connection.remote_pubkey().peer_id().to_string();

    ensure!(remote_id == expected_remote_id, "Unexpected peer");
    Ok(BufReader::with_capacity(DATA_MAX_SIZE, connection))
}

pub fn send_local_node_info(
    connection: &mut PeerConnection,
    local_info: &DefaultNodeInfo,
) -> Result<()> {
    let bytes = local_info.encode_length_delimited_to_vec(); // Encode as length prefix + contents.
    connection.get_mut().write_all(&bytes)?;
    connection.get_mut().flush()?;
    Ok(())
}

fn receive_message<T>(reader: &mut impl Read, max_bytes: usize) -> Result<T>
where
    T: Message + Default,
{
    // The peer sends a length prefix followed by contents. The length is a variable-length integer
    // (varint), potentially longer than one byte, so read it one byte at a time.
    // Each byte's high bit indicates whether another byte follows; it is not part of the numeric value.
    // This varint length prefix occupies at most 10 bytes.
    let mut prefix = [0u8; 10];
    let mut len: Option<usize> = None;

    for i in 0..prefix.len() {
        // Read one byte.
        reader.read_exact(&mut prefix[i..(i + 1)])?; // read_exact reads enough bytes to fill the supplied buffer.

        let hi_bit = prefix[i] & 0x80;
        if hi_bit == 0 {
            len = Some(prost::decode_length_delimiter(&prefix[..=i])?);
            break;
        }
    }
    let len: usize = len.context("cannot read length prefix")?;
    ensure!(len <= max_bytes, "len exceeds 10kB");

    let mut buffer = vec![0u8; len];

    reader.read_exact(&mut buffer)?;

    Ok(T::decode(&buffer[..len])?)
}

pub fn receive_remote_node_info(connection: &mut PeerConnection) -> Result<DefaultNodeInfo> {
    const MAX_BYTES: usize = 10240; // https://github.com/cometbft/cometbft/blob/v0.40.0/p2p/node_info.go#L16
    receive_message::<DefaultNodeInfo>(connection, MAX_BYTES)
}

// Once connected, send our NodeInfo so the peer can check compatibility and other metadata.
pub fn exchanged_node_info(
    connection: &mut PeerConnection,
    local_info: &DefaultNodeInfo,
) -> Result<DefaultNodeInfo> {
    // Send our NodeInfo, receive the peer's NodeInfo, and return it.
    send_local_node_info(connection, local_info)?;
    receive_remote_node_info(connection)
}

// Receive a message packet.
pub fn receive_packet(reader: &mut impl Read) -> Result<Packet> {
    // Similar to the receiver above.
    // Upstream computes this at runtime by marshaling a PacketMsg with a 1024-byte payload;
    // use the resulting size as a constant here.
    const MAX_BYTES: usize = 1034; // https://github.com/cometbft/cometbft/blob/v0.40.0/p2p/conn/connection.go#L705
    receive_message::<Packet>(reader, MAX_BYTES)
}

// Send a packet.
pub fn send_packet(writer: &mut impl Write, packet: &Packet) -> Result<()> {
    let bytes = packet.encode_length_delimited_to_vec();
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

pub fn send_complete_message(writer: &mut impl Write, message: &CompleteMessage) -> Result<()> {
    for p in message.to_packets()? {
        let packet = Packet {
            sum: Some(Sum::PacketMsg(p)),
        };
        send_packet(writer, &packet)?;
    }
    Ok(())
}

// Packet has three variants: Msg, Ping, and Pong. PacketMsg carries a message fragment.

// Important: connecting to node1 using node0's identity creates a bidirectional interceptor-node1
// connection, NOT a direct node0-node1 connection.
// Without the interceptor, the topology is a full mesh. With it, each node has multiple connections
// to the central interceptor, each representing a different peer.
pub async fn connect_as(
    as_node: &Node,
    remote: &Node,
    rm: &mut ResourceManager,
) -> Result<PeerConnection> {
    let other_addr = remote.p2p_addr;
    let expect_other_id = remote.identity.id.clone();
    let my_identity = as_node.identity.clone();
    let my_info = as_node.node_info.clone();
    let (connection, shutdown_handle) = tokio::task::spawn_blocking(move || {
        // Create a TcpStream with three-second read/write timeouts during the handshake;
        // clear these timeouts when leaving the handshake setup.
        let timeout = Duration::from_secs(3);
        let stream = TcpStream::connect_timeout(&other_addr, timeout)?; // Timeout for establishing the TCP connection.
        // Read/write timeouts, for example if the peer never sends NodeInfo.
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;

        let mut connection =
            make_secret_connection(stream.try_clone()?, &my_identity, &expect_other_id)?;

        let other_info = exchanged_node_info(&mut connection, &my_info)?;
        ensure!(
            other_info.default_node_id == expect_other_id,
            "NodeInfo ID does not match authenticated peer"
        );
        // Update settings on the same socket through the original handle.
        stream.set_read_timeout(None)?;
        stream.set_write_timeout(None)?;
        Ok::<_, anyhow::Error>((connection, stream))
    })
    .await??;
    rm.register_socket(shutdown_handle);
    Ok(connection)
}
