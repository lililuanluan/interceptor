use std::net::{Shutdown, TcpStream};

use anyhow::{Result, ensure};
use bollard::Docker;

pub struct ResourceManager {
    docker: Docker,
    container_ids: Vec<String>,
    sockets: Vec<TcpStream>, // 通常情况下，socket句柄在释放后就会关闭，但是我这里需要阻塞收包（read_exact，数据没有收够的时候就一直在那等，且timeout=None），所以必须在退出时打断读取
}

impl ResourceManager {
    pub fn new(docker: Docker) -> Self {
        Self {
            docker: docker,
            container_ids: Vec::new(),
            sockets: Vec::new(),
        }
    }

    pub fn register_container(&mut self, container_id: String) {
        self.container_ids.push(container_id);
    }

    pub fn register_socket(&mut self, socket: TcpStream) {
        self.sockets.push(socket);
    }

    pub async fn cleanup(&self) -> Result<()> {
        let mut errors = Vec::new();
        for socket in &self.sockets {
            // both = read + write
            if let Err(err) = socket.shutdown(Shutdown::Both) {
                // 如果是已经断开的连接，则不需要再次关闭
                if err.kind() != std::io::ErrorKind::NotConnected {
                    errors.push(format!("Shutdown socket {socket:?}: {err}"));
                }
            }
        }
        for container_id in &self.container_ids {
            if let Err(err) = self.docker.stop_container(&container_id, None).await {
                errors.push(format!("Stop {container_id}: {err}"));
            }
            if let Err(err) = self.docker.remove_container(&container_id, None).await {
                errors.push(format!("Remove {container_id}: {err}"));
            }
        }

        ensure!(errors.is_empty(), "{}", errors.join("\n"));
        Ok(())
    }
}
