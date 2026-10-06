use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_consensus::SigningKey;
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct NodeID {
    pub priv_key: SigningKey, // 用priv_key.verification_key()可以获取公钥
    pub id: String,
}

use tendermint_p2p::secret_connection::{SecretConnection, Version};

impl NodeID {
    // Example: cluster-id/config/node0/node_key.json
    /*
    {"priv_key":{"type":"tendermint/PrivKeyEd25519","value":"XZBDKAvX2f2kR8+DM4hJi6t9+19lNtenPikwQLvKnR1iCqsrle+wKkKlP+BatppXAF47Qk5vqVxR8kDbdBoKFQ=="}}
    */
    // implementation: https://github.com/cometbft/cometbft/blob/v0.38.21/crypto/ed25519/ed25519.go
    // 在这个json中，priv_key是64字节的，前32字节是私钥（seed），用这个私钥可以生成32位的公钥，而后32位就是公钥，可以验证二者是否相等

    pub fn load(path: &Path) -> Result<Self> {
        // 将文件读取到字符串
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read {}", path.display()))?;

        // 将字符串转为json对象
        let json: Value = content.parse()?;
        ensure!(
            json["priv_key"]["type"].as_str() == Some("tendermint/PrivKeyEd25519"),
            "Currently only support Ed25519"
        );

        // 获取私钥
        let bytes = STANDARD.decode(
            json["priv_key"]["value"]
                .as_str()
                .context("Missing priv_key.value")?,
        )?; // as_str返回option，不能?，.context将None转换位Err("Missing...")之后可以用?了
        ensure!(bytes.len() == 64, "Expected a 64-byte Ed25519 key");

        // 获取前32个字节
        let seed: [u8; 32] = bytes[..32].try_into()?;
        let priv_key = SigningKey::from(seed);

        // 从前32个字节的seed推导公钥
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

// 公钥是公开的，所以提供公钥并不能证明身份，需要证明手里真的有私钥
// 这里握手就是通过一方签名另一方验签，签名对象就是本次握手的相关数据
// 需要验证：
// - 对方持有正确的公钥（如果对方提供自己的公私钥，但不是配置中的那把公钥也不行）
// -  对方有这把正确公钥对应的私钥
pub fn probe_secret_connection(
    addr: SocketAddr,
    local: &NodeID,
    expected_remote_id: &str,
) -> Result<String> {
    let timeout = Duration::from_secs(3);

    // 通过addr建立一个连接
    let stream = TcpStream::connect_timeout(&addr, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    // 建立加密连接
    let connection = SecretConnection::new(stream, local.priv_key.clone(), Version::V0_34)?; // 向对方证明自己身份和验证对方身份，建立加密连接

    let remote_id = connection.remote_pubkey().peer_id().to_string();

    ensure!(remote_id == expected_remote_id, "Unexpected peer");
    Ok(remote_id)
}
