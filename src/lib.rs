//! Time naming conventions:
//!   Suffixes: _at      Wall-clock time (DateTime<Utc>; stored as RFC3339 in CSV without renaming).
//!             _instant Monotonic time (Instant; used only for differences, not persisted).
//!             _us      Microseconds since the start of this run (persisted).
//!   Prefixes (past participles, chosen for the domain):
//!         started     The start of this run.
//!         observed    Blocks: when the interceptor reads a node's block notification frame.
//!         intercepted Messages: when the interceptor receives the final fragment of a full P2P message.
//!         forwarded   Messages: immediately before the interceptor writes to the peer.
//!         decided     (Future) When the controller makes its decision.
//!
//!
//! Channel (wire) naming conventions:
//!   Choose names by type, using three categories:
//!   1) One-way I/O halves: impl Read / impl Write (the halves returned by split_connection).
//!        reader / writer: the type already fixes the direction; do not use tx/rx here.
//!        For multiple directions, identify the peer: reader_i reads from i, writer_j writes to j.
//!   2) Full-duplex connections: PeerConnection / TcpStream / WebSocketStream.
//!        connection / socket
//!   3) mpsc channels: Sender / Receiver.
//!        At creation (the channel() call), use the channel's perspective: <downstream>_tx / <downstream>_rx.
//!        For fields/parameters, use the owner's perspective: inbox for one input, from_<upstream> for multiple inputs, to_<downstream> for outputs.
//!        new() switches perspectives: parameter names match component fields; arguments use channel-oriented names.
//!   Exception: LogWriter<T> is a facade over the sender and background writing (the receiver is hidden).
//!         Use purpose-specific names returned by the factory: blocks_writer / message_writer (matching the record types), not _tx.
//!   Downstream anchors (used at creation and traced from other sites): controller / forwarder / blocks / message.
//!   Node ID convention: (from, to) means original sender -> original recipient, not interceptor connection endpoints.
//!        In connect_as, reader_X reads from X and writer_Y writes to Y; pair them as (reader_X, writer_Y, X, Y).

pub mod block_monitor;
pub mod configs;
pub mod controller;
pub mod docker;
pub mod filepath;
pub mod forwarder;
pub mod logger;
pub mod message;
pub mod network;
pub mod node;
pub mod p2p;
pub mod resource_manager;
pub mod testnet;
pub mod transaction;
