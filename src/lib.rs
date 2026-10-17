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
//!
//!
//! 管道（wire）命名规范：
//!   先看类型，再起名，分三套：
//!   1) 单向IO半端：impl Read / impl Write（split_connection 拆出来的两半）
//!        reader / writer —— 类型本身锁死了方向，不借用 tx/rx
//!        多个方向时前缀是朝向的节点：reader_i = 从i读，writer_j = 写给j
//!   2) 双向完整连接：PeerConnection / TcpStream / WebSocketStream
//!        connection / socket
//!   3) mpsc 管道：Sender / Receiver
//!        出生地（channel() 那一行，主体是管子）：<下游>_tx / <下游>_rx
//!        持有者（字段/形参，主体是自己）：唯一入口叫 inbox，多入口叫 from_<上游>；发出叫 to_<下游>
//!        new() 是视角切换点：形参名用组件视角（=字段名），实参用管道本体视角
//!   例外：LogWriter<T> 是把 tx 和后台写任务打包好的门面（拿不到 rx），用工厂返回的用途名
//!         blocks_writer / message_writer（镜像类型名），不写成 _tx
//!   下游锚点（创建点用，其他位置靠它反查）：controller / forwarder / blocks / message
//!   节点id约定：(from, to) = 原始发送方 → 原始接收方，不是 interceptor 的哪一端
//!        所以 connect_as 里 reader_X = 从X读，writer_Y = 写给Y，配对成 (reader_X, writer_Y, X, Y)

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
