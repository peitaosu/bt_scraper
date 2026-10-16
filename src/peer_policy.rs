use crate::{Peer, PeerSource};
use std::net::{IpAddr, SocketAddr};

pub fn is_routable_addr(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(v4) => {
            if v4.is_unspecified() || v4.is_loopback() || v4.is_link_local() || v4.is_broadcast() {
                return false;
            }
            if v4.is_private() {
                return false;
            }
            let octets = v4.octets();
            !(octets[0] == 0 || octets[0] >= 224)
        }
        IpAddr::V6(v6) => !v6.is_unspecified() && !v6.is_loopback() && !v6.is_multicast(),
    }
}

pub fn is_storable_peer(peer: &Peer) -> bool {
    if !is_routable_addr(peer.addr) {
        return false;
    }
    matches!(
        peer.source,
        PeerSource::Announce | PeerSource::Tracker | PeerSource::Dht
    )
}

pub fn is_quality_peer(peer: &Peer) -> bool {
    is_storable_peer(peer)
}

pub fn peer_source_rank(source: PeerSource) -> u8 {
    match source {
        PeerSource::Announce => 0,
        PeerSource::Dht => 1,
        PeerSource::Tracker => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn rejects_private_announce_peers() {
        let peer = Peer::new(
            SocketAddr::from((Ipv4Addr::new(192, 168, 1, 1), 51413)),
            PeerSource::Announce,
        );
        assert!(!is_storable_peer(&peer));
    }

    #[test]
    fn accepts_public_dht_peer() {
        let peer = Peer::new(
            SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 6881)),
            PeerSource::Dht,
        );
        assert!(is_quality_peer(&peer));
    }
}
