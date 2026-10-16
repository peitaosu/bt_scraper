use crate::{DiscoveredHash, DiscoverySource, InfoHash, Peer, PeerSource};
use mainline::RequestSpecific;
use std::net::{SocketAddr, SocketAddrV4};

pub fn discovery_from_request(
    request: &RequestSpecific,
    from: SocketAddrV4,
) -> Option<DiscoveredHash> {
    let kind = format!("{:?}", request.request_type);
    if kind.starts_with("GetPeers(") {
        let info_hash = parse_info_hash_field(&kind)?;
        return Some(DiscoveredHash {
            info_hash,
            peer: None,
            source: DiscoverySource::GetPeers,
        });
    }
    if let Some((info_hash, peer)) = parse_announce_peer(&kind, from) {
        return Some(DiscoveredHash {
            info_hash,
            peer: Some(peer),
            source: DiscoverySource::AnnouncePeer,
        });
    }
    None
}

fn parse_announce_peer(kind: &str, from: SocketAddrV4) -> Option<(InfoHash, Peer)> {
    if !kind.contains("AnnouncePeer(AnnouncePeerRequestArguments") {
        return None;
    }
    let info_hash = parse_info_hash_field(kind)?;
    let port = parse_u16_field(kind, "port:")?;
    let implied = kind.contains("implied_port: Some(true)");
    let addr = if implied {
        from
    } else {
        SocketAddrV4::new(*from.ip(), port)
    };
    Some((
        info_hash,
        Peer::new(SocketAddr::V4(addr), PeerSource::Announce),
    ))
}

fn parse_info_hash_field(debug: &str) -> Option<InfoHash> {
    let marker = "info_hash: ";
    let start = debug.find(marker)? + marker.len();
    parse_id_token(&debug[start..])
}

fn parse_id_token(text: &str) -> Option<InfoHash> {
    let id_start = text.find("Id(")? + 3;
    let rest = &text[id_start..];
    let end = rest.find(')')?;
    let inner = rest[..end].trim();
    if let Some(hash) = InfoHash::from_hex(inner) {
        return Some(hash);
    }
    if inner.starts_with('[') {
        return parse_id_byte_list(inner);
    }
    None
}

fn parse_id_byte_list(inner: &str) -> Option<InfoHash> {
    let inner = inner.strip_prefix('[')?.strip_suffix(']')?;
    let mut bytes = [0u8; 20];
    for (index, part) in inner.split(", ").enumerate() {
        if index >= 20 {
            return None;
        }
        bytes[index] = part.trim().parse().ok()?;
    }
    Some(InfoHash(bytes))
}

fn parse_u16_field(debug: &str, key: &str) -> Option<u16> {
    let needle = format!("{key} ");
    let start = debug.find(&needle)? + needle.len();
    let rest = &debug[start..];
    let end = rest.find(|c: char| c == ',' || c == '}')?;
    rest[..end].trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mainline::Id;

    #[test]
    fn parse_info_hash_from_mainline_debug_shape() {
        let mut bytes = [0u8; 20];
        bytes[0] = 7;
        bytes[19] = 9;
        let id = Id::from_bytes(bytes).unwrap();
        let fragment = format!("GetPeers(GetPeersRequestArguments {{ info_hash: {id:?} }})");
        let hash = parse_info_hash_field(&fragment).unwrap();
        assert_eq!(hash, InfoHash(bytes));
    }

    #[test]
    fn parse_announce_port_from_debug_shape() {
        let mut bytes = [1u8; 20];
        bytes[0] = 2;
        let id = Id::from_bytes(bytes).unwrap();
        let fragment = format!(
            "Put(PutRequest {{ token: [0], put_request_type: AnnouncePeer(AnnouncePeerRequestArguments {{ info_hash: {id:?}, port: 6881, implied_port: None }}) }})"
        );
        let (hash, peer) = parse_announce_peer(
            &fragment,
            SocketAddrV4::new(std::net::Ipv4Addr::new(1, 2, 3, 4), 9999),
        )
        .unwrap();
        assert_eq!(hash, InfoHash(bytes));
        assert_eq!(peer.addr.port(), 6881);
    }
}
