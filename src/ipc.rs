// ipc = inter-process communication

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::controller::Intercepted;
use crate::forwarder::{Origin, SendJob};

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

// python和rust的通信，消息格式：先用4字节表示数据长度，然后后面是json数据，用大端序
pub async fn send_json(writer: &mut (impl AsyncWrite + Unpin), message: &Value) -> Result<()> {
    let data = serde_json::to_vec(message)?;
    ensure!(data.len() <= MAX_FRAME_BYTES, "IPC frame too large");

    // 4字节头，用u32，记录长度
    let len = u32::try_from(data.len())?;
    // 先写长度头
    writer.write_u32(len).await?;
    // 再写json字节数据
    writer.write_all(&data).await?;
    writer.flush().await?;

    Ok(())
}

pub async fn receive_json(reader: &mut (impl AsyncRead + Unpin)) -> Result<Value> {
    let len = reader.read_u32().await? as usize; // 先读长度头
    ensure!(len <= MAX_FRAME_BYTES, "IPC frame too large");

    let mut data = vec![0; len];
    reader.read_exact(&mut data).await?;

    Ok(serde_json::from_slice(&data)?)
}

// 实现一个简单的握手，rust端发送要给hello，python看到后回复ready
pub async fn connect(path: &Path) -> Result<UnixStream> {
    let mut socket = UnixStream::connect(path).await?;

    send_json(&mut socket, &json!({"type": "hello", "version": 1})).await?;

    let response = receive_json(&mut socket).await?;

    ensure!(
        response == json!({"type": "ready", "version": 1}),
        "Unexpected controller handshake response: {response}"
    );

    println!("Python controller connected");
    Ok(socket)
}

// 将拦截到的消息转换为json发送给python端
pub fn encode_intercepted(
    message: Intercepted,
    started: Instant,
    current_height: u64,
) -> Result<Value> {
    // 由于Instant不能跨进程，所以要从python端返回的数据重建这个Instant，这里先转为ns，后续根据started+ns算出对应instant
    let intercepted_ns = u64::try_from(
        message
            .intercepted_instant
            .duration_since(started)
            .as_nanos(),
    )?;
    Ok(json!({
        "type": "intercepted",
        "from": message.from,
        "to": message.to,
        "channel_id": message.channel_id,
        "current_height": current_height,   // 从block monitor获取
        "data_base64": STANDARD.encode(message.data),
        "received": {
            "packets": message.packets, // packet数
            "intercepted_at": message.intercepted_at.to_rfc3339_opts(SecondsFormat::Nanos, true),
            "intercepted_ns": intercepted_ns,
            "data_base64": null,    // 如果python端篡改，则把原始字节放在这。（这样设计是为了支持无中生有的语义）
        }
    }))
}

// wire format：数据的序列化/传输格式
// Intercepted是拦截到的消息，是controller.rs模块用的
// 而SendJob和Origin是forwarder.rs模块用的，其中Origin是SendJob中的一个Option。这两个模块不公用结构体

// 下面需要将从python端返回来的数据反序列化为SendJob（以及其中的Origin）

// rust发给python只需要发一个json即可，但是从python端收的话，需要将收到的json构建为rust中的对象，这就是为什么要声明这些结构体。而且不能直接转换为SendJob，因为有些字段要重新算，例如要重建Instant，Duration，data_base64解码（从base64字符串解码为字节数组）
// 而这些Wire结构体是和json结构严格对应的

#[derive(Deserialize)]
struct SendJobWire {
    #[serde(rename = "type")] // type是关键字，不能作为成员名
    kind: String,
    from: usize,
    to: usize,
    channel_id: i32,
    data_base64: String,
    delay_us: u64,
    received: Option<OriginWire>,
}
#[derive(Deserialize)]
struct OriginWire {
    packets: usize,
    intercepted_at: String,
    intercepted_ns: u64,
    data_base64: Option<String>,
}

// 将从python端收到的消息解析为SendJob
pub fn decode_send_job(value: Value, started: Instant, num_nodes: usize) -> Result<SendJob> {
    let job: SendJobWire = serde_json::from_value(value)?;
    ensure!(job.kind == "send", "expected a send task from python");
    ensure!(
        job.from < num_nodes && job.to < num_nodes && job.from != job.to,
        "invalid send direction {} -> {}",
        job.from,
        job.to
    );
    ensure!(
        (0..=255).contains(&job.channel_id),
        "invalid channel id {}",
        job.channel_id
    );

    let received = job
        .received
        .map(|origin| -> Result<Origin> {
            Ok(Origin {
                packets: origin.packets,
                intercepted_at: DateTime::parse_from_rfc3339(&origin.intercepted_at)?
                    .with_timezone(&Utc),
                intercepted_instant: started
                    .checked_add(Duration::from_nanos(origin.intercepted_ns))
                    .with_context(|| format!("invalid intercepted_ns {}", origin.intercepted_ns))?,
                data: origin
                    .data_base64
                    .map(|data| STANDARD.decode(data))
                    .transpose()?,
            })
        })
        .transpose()?; // transpose将Option和Result的嵌套顺序翻转：Option<Result<T, E>> <-> Result<Option<T>, E>

    Ok(SendJob {
        from: job.from,
        to: job.to,
        channel_id: job.channel_id,
        data: STANDARD.decode(job.data_base64)?,
        received,
        delay: Duration::from_micros(job.delay_us),
    })
}
