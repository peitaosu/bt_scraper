use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RoutingTableSnapshot {
    pub nodes: Vec<String>,
    pub saved_at: u64,
}

pub fn load_routing_table(path: impl AsRef<Path>) -> Vec<String> {
    let path = path.as_ref();
    if !path.exists() {
        return Vec::new();
    }
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(_) => return Vec::new(),
    };
    serde_json::from_str::<RoutingTableSnapshot>(&content)
        .map(|snapshot| snapshot.nodes)
        .unwrap_or_default()
}

pub fn save_routing_table(path: impl AsRef<Path>, nodes: &[String]) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let snapshot = RoutingTableSnapshot {
        nodes: nodes.to_vec(),
        saved_at: crate::unix_now(),
    };
    let payload = serde_json::to_string_pretty(&snapshot)?;
    std::fs::write(path, payload).context("write routing table")?;
    Ok(())
}
