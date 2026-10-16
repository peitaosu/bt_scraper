use bt_scraper::routing::{load_routing_table, save_routing_table};
use tempfile::TempDir;

#[test]
fn routing_table_roundtrip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("routing.json");
    let nodes = vec![
        "1.2.3.4:6881".to_string(),
        "5.6.7.8:6881".to_string(),
    ];
    save_routing_table(&path, &nodes).unwrap();
    let loaded = load_routing_table(&path);
    assert_eq!(loaded, nodes);
}

#[test]
fn routing_table_missing_file_returns_empty() {
    let loaded = load_routing_table("/tmp/bt-scraper-nonexistent-routing-table.json");
    assert!(loaded.is_empty());
}
