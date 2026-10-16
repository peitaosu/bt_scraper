use bt_scraper::enrich::{run_discovery_batcher, DiscoverySettings};
use bt_scraper::store::Store;
use bt_scraper::{DiscoveredHash, DiscoverySource, InfoHash, Peer, PeerSource};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn discovery_batcher_stores_bep51_without_enrich_queue() {
    let db = NamedTempFile::new().unwrap();
    let store = Store::open(db.path(), 16, 10).unwrap();
    let (disc_tx, disc_rx) = mpsc::channel(64);
    let (enrich_tx, mut enrich_rx) = mpsc::channel(64);

    let batcher = tokio::spawn(run_discovery_batcher(
        disc_rx,
        store.clone(),
        enrich_tx,
        DiscoverySettings {
            batch_size: 4,
            flush_ms: 25,
        },
        CancellationToken::new(),
    ));

    for i in 0u8..4 {
        disc_tx
            .send(DiscoveredHash {
                info_hash: InfoHash([i + 50; 20]),
                peer: None,
                source: DiscoverySource::Bep51,
            })
            .await
            .unwrap();
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if enrich_rx.try_recv().is_ok() {
                panic!("bep51 hash should not enqueue enrichment");
            }
            let (total, _, _) = store.count_by_status().await.unwrap();
            if total == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("batcher should persist discoveries");

    drop(disc_tx);
    batcher.await.unwrap();
}

#[tokio::test]
async fn discovery_batcher_enqueues_announce_peer() {
    let db = NamedTempFile::new().unwrap();
    let store = Store::open(db.path(), 16, 10).unwrap();
    let (disc_tx, disc_rx) = mpsc::channel(64);
    let (enrich_tx, mut enrich_rx) = mpsc::channel(64);

    let batcher = tokio::spawn(run_discovery_batcher(
        disc_rx,
        store,
        enrich_tx,
        DiscoverySettings {
            batch_size: 100,
            flush_ms: 30,
        },
        CancellationToken::new(),
    ));

    disc_tx
        .send(DiscoveredHash {
            info_hash: InfoHash([77u8; 20]),
            peer: Some(Peer::new(
                SocketAddr::from((Ipv4Addr::new(192, 168, 1, 1), 51413)),
                PeerSource::Announce,
            )),
            source: DiscoverySource::AnnouncePeer,
        })
        .await
        .unwrap();

    let hash = tokio::time::timeout(Duration::from_secs(2), enrich_rx.recv())
        .await
        .expect("announce should enqueue enrichment")
        .expect("hash should be present");
    assert_eq!(hash, InfoHash([77u8; 20]));

    drop(disc_tx);
    batcher.await.unwrap();
}
