//! 时间命名规范：
//!   后缀：_at      墙钟时刻（DateTime<Utc>；写进CSV是RFC3339字符串，名字不变）
//!         _instant 单调时刻（Instant；只用于相减，不落盘）
//!         _us      从本轮起点算起的微秒数（落盘用）
//!   前缀（过去分词，按领域取动词）：
//!         started     本轮运行起点
//!         observed    block维度：拦截器看到节点的出块通知（帧读完那一刻）
//!         intercepted message维度：拦截器收齐一条完整P2P消息（最后一片到达那一刻）
//!         forwarded   message维度：拦截器写向对端之前那一刻
//!         （将来）decided  controller做出决定的时刻

pub mod block_monitor;
pub mod configs;
pub mod docker;
pub mod filepath;
pub mod logger;
pub mod message;
pub mod network;
pub mod node;
pub mod p2p;
pub mod resource_manager;
pub mod testnet;
