use anyhow::{Context, Result, bail};
use crate::InfoHash;
use mainline::Id;
use rand::Rng;
use serde_bencode::value::Value;
use std::collections::HashMap;

pub struct SampleInfohashesResponse {
    pub samples: Vec<InfoHash>,
    pub interval: u64,
    pub num: u64,
}

pub fn build_sample_query(node_id: [u8; 20], target: Id, transaction_id: [u8; 2]) -> Result<Vec<u8>> {
    let message = Value::Dict(HashMap::from([
        (b"t".to_vec(), Value::Bytes(transaction_id.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        (b"q".to_vec(), Value::Bytes(b"sample_infohashes".to_vec())),
        (
            b"a".to_vec(),
            Value::Dict(HashMap::from([
                (b"id".to_vec(), Value::Bytes(node_id.to_vec())),
                (
                    b"target".to_vec(),
                    Value::Bytes(target.as_bytes().to_vec()),
                ),
            ])),
        ),
    ]));
    serde_bencode::to_bytes(&message).context("encode sample_infohashes query")
}

pub fn decode_sample_response(data: &[u8]) -> Result<SampleInfohashesResponse> {
    let value = serde_bencode::from_bytes::<Value>(data).context("decode krpc response")?;
    let Value::Dict(entries) = value else {
        bail!("response not dict");
    };
    let Some(Value::Dict(r_entries)) = entries.get(b"r".as_slice()) else {
        bail!("response missing r");
    };
    let has_samples = r_entries.contains_key(b"samples".as_slice());
    let has_interval = r_entries.contains_key(b"interval".as_slice());
    if !has_samples || !has_interval {
        bail!("response missing samples or interval");
    }
    let interval = r_entries
        .get(b"interval".as_slice())
        .and_then(|v| match v {
            Value::Int(n) => Some(*n as u64),
            _ => None,
        })
        .unwrap_or(0);
    let num = r_entries
        .get(b"num".as_slice())
        .and_then(|v| match v {
            Value::Int(n) => Some(*n as u64),
            _ => None,
        })
        .unwrap_or(0);
    let mut samples = Vec::new();
    if let Some(Value::Bytes(bytes)) = r_entries.get(b"samples".as_slice()) {
        if !bytes.len().is_multiple_of(20) {
            bail!("invalid samples length");
        }
        for chunk in bytes.chunks_exact(20) {
            let mut arr = [0u8; 20];
            arr.copy_from_slice(chunk);
            samples.push(InfoHash(arr));
        }
    }
    Ok(SampleInfohashesResponse {
        samples,
        interval,
        num,
    })
}

pub fn encode_sample_response(
    responder_id: [u8; 20],
    interval: u64,
    num: u64,
    samples: &[InfoHash],
    nodes: &[u8],
) -> Result<Vec<u8>> {
    let mut samples_buf = Vec::with_capacity(samples.len() * 20);
    for hash in samples {
        samples_buf.extend_from_slice(&hash.0);
    }
    let mut r = HashMap::from([
        (b"id".to_vec(), Value::Bytes(responder_id.to_vec())),
        (b"interval".to_vec(), Value::Int(interval as i64)),
        (b"num".to_vec(), Value::Int(num as i64)),
        (b"samples".to_vec(), Value::Bytes(samples_buf)),
    ]);
    if !nodes.is_empty() {
        r.insert(b"nodes".to_vec(), Value::Bytes(nodes.to_vec()));
    }
    let message = Value::Dict(HashMap::from([
        (b"t".to_vec(), Value::Bytes(random_transaction_id().to_vec())),
        (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
        (b"r".to_vec(), Value::Dict(r)),
    ]));
    serde_bencode::to_bytes(&message).context("encode sample_infohashes response")
}

pub fn random_transaction_id() -> [u8; 2] {
    rand::thread_rng().gen::<u16>().to_be_bytes()
}

pub fn response_matches_transaction(data: &[u8], transaction_id: &[u8]) -> bool {
    let value = match serde_bencode::from_bytes::<Value>(data) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let Value::Dict(entries) = value else {
        return false;
    };
    entries.iter().any(|(key, value)| {
        key.as_slice() == b"t"
            && matches!(value, Value::Bytes(bytes) if bytes.as_slice() == transaction_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn decodes_sample_response_with_num() {
        let mut samples = [0u8; 40];
        samples[20..40].fill(1);
        let response = Value::Dict(HashMap::from([
            (b"r".to_vec(), Value::Dict(HashMap::from([
                (b"id".to_vec(), Value::Bytes(vec![0u8; 20])),
                (b"interval".to_vec(), Value::Int(300)),
                (b"num".to_vec(), Value::Int(42)),
                (b"samples".to_vec(), Value::Bytes(samples.to_vec())),
            ]))),
            (b"t".to_vec(), Value::Bytes(vec![1, 2])),
            (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
        ]));
        let encoded = serde_bencode::to_bytes(&response).unwrap();
        let parsed = decode_sample_response(&encoded).unwrap();
        assert_eq!(parsed.interval, 300);
        assert_eq!(parsed.num, 42);
        assert_eq!(parsed.samples.len(), 2);
    }

    #[test]
    fn accepts_empty_samples() {
        let response = Value::Dict(HashMap::from([
            (b"r".to_vec(), Value::Dict(HashMap::from([
                (b"id".to_vec(), Value::Bytes(vec![0u8; 20])),
                (b"interval".to_vec(), Value::Int(60)),
                (b"num".to_vec(), Value::Int(0)),
                (b"samples".to_vec(), Value::Bytes(vec![])),
            ]))),
            (b"t".to_vec(), Value::Bytes(vec![1, 2])),
            (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
        ]));
        let encoded = serde_bencode::to_bytes(&response).unwrap();
        let parsed = decode_sample_response(&encoded).unwrap();
        assert!(parsed.samples.is_empty());
    }

    #[test]
    fn encode_response_includes_samples_key() {
        let hash = InfoHash([9u8; 20]);
        let payload = encode_sample_response([1u8; 20], 60, 1, &[hash], &[]).unwrap();
        let parsed = decode_sample_response(&payload).unwrap();
        assert_eq!(parsed.samples.len(), 1);
        assert_eq!(parsed.num, 1);
    }
}
