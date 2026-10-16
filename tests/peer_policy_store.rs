use bt_scraper::enrich::{run_discovery_batcher, DiscoverySettings};
use bt_scraper::store::Store;
use bt_scraper::{DiscoveredHash, DiscoverySource, InfoHash, Peer, PeerSource};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn private_announce_peer_is_not_stored_as_quality() {
    let db = NamedTempFile::new().unwrap();
    let store = Store::open(db.path(), 16, 10).unwrap();
    let (disc_tx, disc_rx) = mpsc::channel(8);
    let (enrich_tx, _enrich_rx) = mpsc::channel(8);

    let batcher = tokio::spawn(run_discovery_batcher(
        disc_rx,
        store.clone(),
        enrich_tx,
        DiscoverySettings {
            batch_size: 1,
            flush_ms: 10,
        },
        CancellationToken::new(),
    ));

    let hash = InfoHash([88u8; 20]);
    disc_tx
        .send(DiscoveredHash {
            info_hash: hash,
            peer: Some(Peer::new(
                SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 51413)),
                PeerSource::Announce,
            )),
            source: DiscoverySource::AnnouncePeer,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !store.has_quality_peer(&hash).await.unwrap() {
                let peers = store.get_peers(&hash, 8).await.unwrap();
                if peers.is_empty() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("private peer should not be stored");

    assert!(!store.has_quality_peer(&hash).await.unwrap());
    drop(disc_tx);
    batcher.await.unwrap();
}
