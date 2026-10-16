use anyhow::{Context, Result, bail};
use crate::{InfoHash, TorrentFile, TorrentMetadata};
use rand::Rng;
use serde_bencode::value::Value;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

const PROTOCOL: &[u8] = b"BitTorrent protocol";
const RESERVED: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01];
const PIECE_SIZE: usize = 16 * 1024;
const MAX_METADATA: usize = 10_000_000;
const EXT_MSG: u8 = 20;
const EXT_HANDSHAKE: u8 = 0;
const LOCAL_UT_METADATA: u8 = 1;

pub async fn fetch_metadata(
    info_hash: &InfoHash,
    peer: SocketAddr,
    peer_timeout: Duration,
) -> Result<TorrentMetadata> {
    timeout(peer_timeout, fetch_metadata_inner(info_hash, peer))
        .await
        .context("metadata timeout")?
}

struct PeerConn {
    stream: TcpStream,
    read_buf: Vec<u8>,
}

impl PeerConn {
    async fn connect(peer: SocketAddr, connect_timeout: Duration) -> Result<Self> {
        let stream = timeout(connect_timeout, TcpStream::connect(peer))
            .await
            .context("tcp connect timeout")?
            .context("tcp connect")?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            read_buf: Vec::new(),
        })
    }

    async fn write_all(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write_all(data).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }

    async fn fill(&mut self) -> Result<()> {
        let mut chunk = [0u8; 4096];
        let read = self.stream.read(&mut chunk).await?;
        if read == 0 {
            bail!("early eof");
        }
        self.read_buf.extend_from_slice(&chunk[..read]);
        Ok(())
    }

    async fn ensure(&mut self, need: usize) -> Result<()> {
        while self.read_buf.len() < need {
            self.fill().await?;
        }
        Ok(())
    }

    fn drain(&mut self, count: usize) -> Vec<u8> {
        self.read_buf.drain(..count).collect()
    }

    async fn read_length_message(&mut self) -> Result<Vec<u8>> {
        self.ensure(4).await?;
        let len = u32::from_be_bytes(self.read_buf[0..4].try_into()?) as usize;
        self.drain(4);
        if len == 0 {
            return Ok(Vec::new());
        }
        self.ensure(len).await?;
        Ok(self.drain(len))
    }

    async fn send_handshake(&mut self, info_hash: &InfoHash) -> Result<()> {
        let mut peer_id = [0u8; 20];
        peer_id[..8].copy_from_slice(b"-BT0001-");
        rand::thread_rng().fill(&mut peer_id[8..]);
        let mut packet = Vec::with_capacity(68);
        packet.push(PROTOCOL.len() as u8);
        packet.extend_from_slice(PROTOCOL);
        packet.extend_from_slice(&RESERVED);
        packet.extend_from_slice(&info_hash.0);
        packet.extend_from_slice(&peer_id);
        self.write_all(&packet).await?;
        self.flush().await?;
        Ok(())
    }

    async fn read_handshake(&mut self, info_hash: &InfoHash) -> Result<()> {
        self.ensure(1).await?;
        let pstrlen = self.read_buf[0] as usize;
        let total = 1 + pstrlen + 48;
        self.ensure(total).await?;
        let handshake = self.drain(total);
        let protocol = &handshake[1..1 + pstrlen];
        if protocol != PROTOCOL {
            bail!("invalid protocol");
        }
        let reserved_start = 1 + pstrlen;
        if handshake[reserved_start + 5] & 0x10 == 0 {
            bail!("peer does not support extensions");
        }
        let hash_start = reserved_start + 8;
        if handshake[hash_start..hash_start + 20] != info_hash.0 {
            bail!("infohash mismatch in handshake");
        }
        Ok(())
    }

    async fn send_ext_handshake(&mut self) -> Result<()> {
        let payload = serde_bencode::to_bytes(&Value::Dict(HashMap::from([(
            b"m".to_vec(),
            Value::Dict(HashMap::from([(
                b"ut_metadata".to_vec(),
                Value::Int(1),
            )])),
        )])))?;
        self.send_ext_message(EXT_HANDSHAKE, &payload).await
    }

    async fn send_ext_message(&mut self, ext_id: u8, payload: &[u8]) -> Result<()> {
        let mut body = Vec::with_capacity(2 + payload.len());
        body.push(EXT_MSG);
        body.push(ext_id);
        body.extend_from_slice(payload);
        let len = (body.len() as u32).to_be_bytes();
        self.write_all(&len).await?;
        self.write_all(&body).await?;
        self.flush().await?;
        Ok(())
    }

    async fn send_interested(&mut self) -> Result<()> {
        let len = 1u32.to_be_bytes();
        self.write_all(&len).await?;
        self.write_all(&[2u8]).await?;
        self.flush().await?;
        Ok(())
    }

    async fn read_peer_ext_handshake(&mut self, wait: Duration) -> Result<(usize, u8)> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if tokio::time::Instant::now() >= deadline {
                bail!("extension handshake timeout");
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let msg = timeout(remaining, self.read_length_message())
                .await
                .context("extension handshake timeout")??;
            if msg.len() < 2 || msg[0] != EXT_MSG || msg[1] != EXT_HANDSHAKE {
                continue;
            }
            let dict = serde_bencode::from_bytes::<Value>(&msg[2..]).context("ext handshake")?;
            let metadata_size = dict_int(&dict, "metadata_size")? as usize;
            let ut_id = dict_dict_int(&dict, "m", "ut_metadata")? as u8;
            return Ok((metadata_size, ut_id));
        }
    }
}

async fn fetch_metadata_inner(info_hash: &InfoHash, peer: SocketAddr) -> Result<TorrentMetadata> {
    let connect_timeout = Duration::from_secs(8);
    let mut conn = PeerConn::connect(peer, connect_timeout).await?;

    conn.send_handshake(info_hash).await.context("send handshake")?;
    conn.read_handshake(info_hash)
        .await
        .context("read handshake")?;
    conn.send_ext_handshake()
        .await
        .context("send extension handshake")?;

    let (metadata_size, peer_ut_metadata_id) = conn
        .read_peer_ext_handshake(Duration::from_secs(15))
        .await
        .context("read extension handshake")?;
    if metadata_size == 0 || metadata_size > MAX_METADATA {
        bail!("invalid metadata size");
    }

    conn.send_interested().await?;

    let num_pieces = metadata_size.div_ceil(PIECE_SIZE);
    let mut buffer = vec![0u8; metadata_size];
    let mut received = vec![false; num_pieces];

    let piece_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    for piece in 0..num_pieces {
        let payload = serde_bencode::to_bytes(&Value::Dict(HashMap::from([
            (b"msg_type".to_vec(), Value::Int(0)),
            (b"piece".to_vec(), Value::Int(piece as i64)),
        ])))?;
        conn.send_ext_message(peer_ut_metadata_id, &payload).await?;
        while !received[piece] {
            if tokio::time::Instant::now() >= piece_deadline {
                bail!("incomplete metadata pieces");
            }
            let remaining = piece_deadline.saturating_duration_since(tokio::time::Instant::now());
            let msg = timeout(remaining, conn.read_length_message())
                .await
                .context("metadata piece timeout")??;
            if msg.len() < 2 || msg[0] != EXT_MSG || msg[1] != LOCAL_UT_METADATA {
                continue;
            }
            let (dict, data) = split_bencode_prefix(&msg[2..])?;
            let msg_type = dict_int(&dict, "msg_type")?;
            if msg_type == 2 {
                bail!("metadata piece rejected");
            }
            if msg_type != 1 {
                continue;
            }
            let piece_index = dict_int(&dict, "piece")? as usize;
            if piece_index >= num_pieces {
                continue;
            }
            let offset = piece_index * PIECE_SIZE;
            let end = (offset + data.len()).min(metadata_size);
            buffer[offset..end].copy_from_slice(&data[..end - offset]);
            received[piece_index] = true;
        }
    }

    verify_info_hash(&buffer, info_hash)?;
    parse_metadata(&buffer)
}

fn split_bencode_prefix(data: &[u8]) -> Result<(Value, Vec<u8>)> {
    for end in 1..=data.len() {
        if let Ok(dict) = serde_bencode::from_bytes::<Value>(&data[..end]) {
            return Ok((dict, data[end..].to_vec()));
        }
    }
    bail!("bencode prefix not found");
}

fn dict_int(dict: &Value, key: &str) -> Result<i64> {
    let Value::Dict(entries) = dict else {
        bail!("expected dict");
    };
    for (k, v) in entries {
        if k.as_slice() == key.as_bytes() {
            let Value::Int(n) = v else {
                bail!("expected int");
            };
            return Ok(*n);
        }
    }
    bail!("key not found");
}

fn dict_dict_int(dict: &Value, outer: &str, inner: &str) -> Result<i64> {
    let Value::Dict(entries) = dict else {
        bail!("expected dict");
    };
    for (k, v) in entries {
        if k.as_slice() == outer.as_bytes() {
            return dict_int(v, inner);
        }
    }
    bail!("outer key not found");
}

fn verify_info_hash(metadata: &[u8], expected: &InfoHash) -> Result<()> {
    let info_bytes = info_dict_bytes(metadata)?;
    let hash = Sha1::digest(&info_bytes);
    if hash.as_slice() != expected.0 {
        bail!("infohash mismatch");
    }
    Ok(())
}

fn info_dict_bytes(metadata: &[u8]) -> Result<Vec<u8>> {
    let root = serde_bencode::from_bytes::<Value>(metadata).context("decode metadata")?;
    let Value::Dict(entries) = root else {
        bail!("metadata root not dict");
    };
    if entries.keys().any(|k| k.as_slice() == b"info") {
        let info = entries
            .get(b"info".as_slice())
            .context("info key missing")?
            .clone();
        return serde_bencode::to_bytes(&info).context("encode info");
    }
    Ok(metadata.to_vec())
}

fn parse_metadata(metadata: &[u8]) -> Result<TorrentMetadata> {
    let info_bytes = info_dict_bytes(metadata)?;
    let info = serde_bencode::from_bytes::<Value>(&info_bytes).context("decode info")?;
    let Value::Dict(entries) = info else {
        bail!("info not dict");
    };

    let mut name = String::new();
    let mut files = Vec::new();
    let mut total_length = 0u64;

    for (k, v) in &entries {
        if k.as_slice() == b"name" {
            if let Value::Bytes(bytes) = v {
                name = String::from_utf8_lossy(bytes).into_owned();
            }
        }
        if k.as_slice() == b"length" {
            if let Value::Int(n) = v {
                total_length = *n as u64;
                files.push(TorrentFile {
                    path: vec![name.clone()],
                    length: *n as u64,
                });
            }
        }
        if k.as_slice() == b"files" {
            if let Value::List(items) = v {
                for item in items {
                    if let Value::Dict(file_entries) = item {
                        let mut path_parts = Vec::new();
                        let mut length = 0u64;
                        for (fk, fv) in file_entries {
                            if fk.as_slice() == b"path" {
                                if let Value::List(parts) = fv {
                                    for part in parts {
                                        if let Value::Bytes(p) = part {
                                            path_parts
                                                .push(String::from_utf8_lossy(p).into_owned());
                                        }
                                    }
                                }
                            }
                            if fk.as_slice() == b"length" {
                                if let Value::Int(n) = fv {
                                    length = *n as u64;
                                }
                            }
                        }
                        total_length += length;
                        files.push(TorrentFile { path: path_parts, length });
                    }
                }
            }
        }
    }

    if name.is_empty() && !files.is_empty() {
        name = files[0].path.join("/");
    }

    Ok(TorrentMetadata {
        name,
        files,
        total_length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn sample_torrent() -> (InfoHash, Vec<u8>) {
        let info = Value::Dict(HashMap::from([
            (b"name".to_vec(), Value::Bytes(b"wire-mock".to_vec())),
            (b"length".to_vec(), Value::Int(12)),
        ]));
        let metadata_bytes = serde_bencode::to_bytes(&info).unwrap();
        let hash = Sha1::digest(&metadata_bytes);
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&hash);
        (InfoHash(arr), metadata_bytes)
    }

    #[tokio::test]
    async fn fetch_metadata_from_mock_peer() {
        let (info_hash, metadata_bytes) = sample_torrent();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metadata_bytes_task = metadata_bytes.clone();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 68];
            stream.read_exact(&mut buf).await.unwrap();
            let mut response = vec![PROTOCOL.len() as u8];
            response.extend_from_slice(PROTOCOL);
            response.extend_from_slice(&RESERVED);
            response.extend_from_slice(&info_hash.0);
            response.extend_from_slice(&[0u8; 20]);
            stream.write_all(&response).await.unwrap();
            let ext_payload = serde_bencode::to_bytes(&Value::Dict(HashMap::from([
                (
                    b"m".to_vec(),
                    Value::Dict(HashMap::from([(b"ut_metadata".to_vec(), Value::Int(1))])),
                ),
                (
                    b"metadata_size".to_vec(),
                    Value::Int(metadata_bytes_task.len() as i64),
                ),
            ])))
            .unwrap();
            let mut ext_msg = vec![0u8; 4];
            ext_msg.extend_from_slice(&(2 + ext_payload.len() as u32).to_be_bytes());
            ext_msg.push(EXT_MSG);
            ext_msg.push(EXT_HANDSHAKE);
            ext_msg.extend_from_slice(&ext_payload);
            stream.write_all(&ext_msg).await.unwrap();
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut client_ext = vec![0u8; len];
            stream.read_exact(&mut client_ext).await.unwrap();
            let piece_payload = serde_bencode::to_bytes(&Value::Dict(HashMap::from([
                (b"msg_type".to_vec(), Value::Int(1)),
                (b"piece".to_vec(), Value::Int(0)),
            ])))
            .unwrap();
            let mut piece_msg = vec![0u8; 4];
            let body_len = 2 + piece_payload.len() + metadata_bytes_task.len();
            piece_msg.extend_from_slice(&(body_len as u32).to_be_bytes());
            piece_msg.push(EXT_MSG);
            piece_msg.push(1);
            piece_msg.extend_from_slice(&piece_payload);
            piece_msg.extend_from_slice(&metadata_bytes_task);
            stream.write_all(&piece_msg).await.unwrap();
        });
        let metadata = fetch_metadata(&info_hash, addr, Duration::from_secs(5))
            .await
            .expect("metadata fetch");
        assert_eq!(metadata.name, "wire-mock");
        assert_eq!(metadata.total_length, 12);
    }
}
