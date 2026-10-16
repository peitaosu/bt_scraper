use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct SharedHttp {
    inner: Arc<reqwest::Client>,
}

impl SharedHttp {
    pub fn new(timeout_secs: u64) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs.max(1)))
            .pool_max_idle_per_host(8)
            .build()
            .expect("build http client");
        Self {
            inner: Arc::new(client),
        }
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.inner
    }
}
