use std::net::{Shutdown, TcpStream};

use anyhow::{Result, ensure};
use bollard::Docker;

pub struct ResourceManager {
    docker: Docker,
    container_ids: Vec<String>,
    // Sockets normally close when their handles are dropped, but read_exact can wait indefinitely
    // for enough data with timeout=None, so shutdown must explicitly interrupt these reads.
    sockets: Vec<TcpStream>,
}

impl ResourceManager {
    pub fn new(docker: Docker) -> Self {
        Self {
            docker,
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
                // An already disconnected socket does not need to be shut down again.
                if err.kind() != std::io::ErrorKind::NotConnected {
                    errors.push(format!("Shutdown socket {socket:?}: {err}"));
                }
            }
        }
        for container_id in &self.container_ids {
            if let Err(err) = self.docker.stop_container(container_id, None).await {
                errors.push(format!("Stop {container_id}: {err}"));
            }
            if let Err(err) = self.docker.remove_container(container_id, None).await {
                errors.push(format!("Remove {container_id}: {err}"));
            }
        }

        ensure!(errors.is_empty(), "{}", errors.join("\n"));
        Ok(())
    }
}
