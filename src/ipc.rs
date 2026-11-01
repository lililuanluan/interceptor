// ipc = inter-process communication

use std::path::Path;

use anyhow::{Result, ensure};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;

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
