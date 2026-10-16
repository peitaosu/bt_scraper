use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct UdpPool {
    slots: Arc<Semaphore>,
}

impl UdpPool {
    pub fn new(size: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(size.max(1))),
        }
    }

    pub async fn acquire(&self) -> anyhow::Result<UdpLease> {
        let permit = self.slots.clone().acquire_owned().await?;
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        Ok(UdpLease { socket, _permit: permit })
    }
}

pub struct UdpLease {
    pub socket: UdpSocket,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl std::ops::Deref for UdpLease {
    type Target = UdpSocket;

    fn deref(&self) -> &Self::Target {
        &self.socket
    }
}
