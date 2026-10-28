//! Wire protocol v2 between Rust and `ml/embedding_server.py`.
//!
//! v1 was newline-delimited JSON: every `f32` was printed as decimal text,
//! parsed back, and the whole request was materialised as nested JSON arrays.
//! v2 is length-prefixed binary frames carrying raw little-endian arrays, so a
//! forward pass costs one `memcpy`-shaped encode instead of a text round trip.
//! `f32` → bytes → `f32` is lossless, so v2 is bit-identical to v1 (the
//! existing incremental-vs-full-recompute exactness suites run unchanged over
//! it, which is the proof).
//!
//! ```text
//! frame   := u32 LE len | u8 type | payload          (len counts type + payload)
//! FORWARD := u32 LE header_len | header JSON | features f32[n*d]
//!            | edge_index u32[2*e] (row 0 = src, row 1 = dst) | targets u32[t]
//!            | (attribution only) attr_targets u32[at] | edge_index_before u32[2*eb]
//! FORWARD_OK := u32 LE header_len | header JSON | embeddings f32[t*dim]
//! ```
//!
//! The worker also puts its stdout framing on a private file descriptor, so a
//! stray `print` from a library cannot corrupt the stream.

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};

use crate::embedding::model_bridge::{AttributionRequest, RawAttributionEntry};
use crate::error::{CareGraphError, Result};

pub const PROTOCOL_VERSION: u32 = 2;
/// Refuse absurd frames instead of allocating whatever a corrupt length says.
pub const MAX_FRAME: usize = 512 * 1024 * 1024;

pub mod ty {
    pub const HANDSHAKE: u8 = 0;
    pub const FORWARD: u8 = 1;
    pub const PING: u8 = 2;
    pub const FORWARD_OK: u8 = 0x81;
    pub const PONG: u8 = 0x82;
    pub const ERROR: u8 = 0x8F;
}

pub fn write_frame<W: Write>(w: &mut W, frame_type: u8, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len() + 1)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&[frame_type])?;
    w.write_all(payload)?;
    w.flush()
}

pub fn read_frame<R: Read>(r: &mut R) -> io::Result<(u8, Vec<u8>)> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad frame length {len}"),
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    let t = body[0];
    body.drain(..1);
    Ok((t, body))
}

#[derive(Serialize)]
struct ForwardHeader {
    n: usize,
    d: usize,
    e: usize,
    t: usize,
    attr: Option<AttrHeader>,
}

#[derive(Serialize)]
struct AttrHeader {
    targets: usize,
    edges_before: usize,
    steps: u32,
    top_k: usize,
}

fn put_u32s(out: &mut Vec<u8>, it: impl Iterator<Item = u32>) {
    for v in it {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn bad(msg: impl Into<String>) -> CareGraphError {
    CareGraphError::Io(io::Error::new(io::ErrorKind::InvalidInput, msg.into()))
}

fn check_edges(rows: &[Vec<u32>], what: &str) -> Result<usize> {
    if rows.len() != 2 || rows[0].len() != rows[1].len() {
        return Err(bad(format!("{what} must be two equal-length rows")));
    }
    Ok(rows[0].len())
}

/// Encode a FORWARD request payload.
pub fn encode_forward(
    features: &[Vec<f32>],
    edge_index: &[Vec<u32>],
    targets: &[usize],
    attribution: Option<&AttributionRequest<'_>>,
) -> Result<Vec<u8>> {
    let n = features.len();
    let d = features.first().map_or(0, Vec::len);
    if features.iter().any(|r| r.len() != d) {
        return Err(bad("node_features rows must all have the same width"));
    }
    let e = check_edges(edge_index, "edge_index")?;
    let attr = match attribution {
        Some(a) => Some(AttrHeader {
            targets: a.targets.len(),
            edges_before: check_edges(a.edge_index_before, "edge_index_before")?,
            steps: a.steps,
            top_k: a.top_k,
        }),
        None => None,
    };
    let header = serde_json::to_vec(&ForwardHeader {
        n,
        d,
        e,
        t: targets.len(),
        attr,
    })?;

    let mut out = Vec::with_capacity(
        4 + header.len()
            + 4 * (n * d + 2 * e + targets.len())
            + attribution.map_or(0, |a| {
                4 * (a.targets.len() + 2 * a.edge_index_before.first().map_or(0, Vec::len))
            }),
    );
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&header);
    for row in features {
        for v in row {
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
    put_u32s(&mut out, edge_index[0].iter().copied());
    put_u32s(&mut out, edge_index[1].iter().copied());
    put_u32s(&mut out, targets.iter().map(|&t| t as u32));
    if let Some(a) = attribution {
        put_u32s(&mut out, a.targets.iter().map(|&t| t as u32));
        put_u32s(&mut out, a.edge_index_before[0].iter().copied());
        put_u32s(&mut out, a.edge_index_before[1].iter().copied());
    }
    Ok(out)
}

/// Embedding rows plus, for an attribution request, the per-target attribution.
pub type DecodedForward = (Vec<Vec<f32>>, Option<Vec<RawAttributionEntry>>);

#[derive(Deserialize)]
struct OkHeader {
    t: usize,
    dim: usize,
    #[serde(default)]
    attribution: Option<Vec<RawAttributionEntry>>,
}

/// Decode a FORWARD_OK payload into embedding rows (+ optional attribution).
pub fn decode_forward_ok(payload: &[u8]) -> Result<DecodedForward> {
    if payload.len() < 4 {
        return Err(bad("FORWARD_OK payload shorter than its header length"));
    }
    let hl = u32::from_le_bytes(payload[..4].try_into().expect("4 bytes")) as usize;
    let header_end = 4usize
        .checked_add(hl)
        .filter(|&e| e <= payload.len())
        .ok_or_else(|| bad("FORWARD_OK header length exceeds payload"))?;
    let header: OkHeader = serde_json::from_slice(&payload[4..header_end])?;
    let body = &payload[header_end..];
    let want = header
        .t
        .checked_mul(header.dim)
        .and_then(|c| c.checked_mul(4))
        .ok_or_else(|| bad("FORWARD_OK dimensions overflow"))?;
    if body.len() != want {
        return Err(bad(format!(
            "FORWARD_OK body is {} bytes, header says {}x{} f32 = {want}",
            body.len(),
            header.t,
            header.dim
        )));
    }
    let mut rows = Vec::with_capacity(header.t);
    for r in 0..header.t {
        let mut row = Vec::with_capacity(header.dim);
        for c in 0..header.dim {
            let off = (r * header.dim + c) * 4;
            row.push(f32::from_le_bytes(
                body[off..off + 4].try_into().expect("4 bytes"),
            ));
        }
        rows.push(row);
    }
    Ok((rows, header.attribution))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, ty::PING, b"abc").unwrap();
        let (t, p) = read_frame(&mut buf.as_slice()).unwrap();
        assert_eq!((t, p.as_slice()), (ty::PING, &b"abc"[..]));
    }

    #[test]
    fn an_oversized_or_zero_length_frame_is_refused_not_allocated() {
        let huge = (MAX_FRAME as u32 + 1).to_le_bytes();
        assert!(read_frame(&mut &huge[..]).is_err());
        assert!(read_frame(&mut &0u32.to_le_bytes()[..]).is_err());
    }

    #[test]
    fn a_truncated_frame_is_an_error() {
        let mut buf = Vec::new();
        write_frame(&mut buf, ty::PING, b"abcdef").unwrap();
        buf.truncate(buf.len() - 2);
        assert!(read_frame(&mut buf.as_slice()).is_err());
    }

    #[test]
    fn encode_forward_lays_out_the_documented_sections() {
        let feats = vec![vec![1.0f32, 2.0], vec![3.0, 4.0]];
        let edges = vec![vec![0u32, 1], vec![1u32, 0]];
        let p = encode_forward(&feats, &edges, &[1], None).unwrap();
        let hl = u32::from_le_bytes(p[..4].try_into().unwrap()) as usize;
        let header: serde_json::Value = serde_json::from_slice(&p[4..4 + hl]).unwrap();
        assert_eq!(header["n"], 2);
        assert_eq!(header["d"], 2);
        assert_eq!(header["e"], 2);
        assert_eq!(header["t"], 1);
        // 4 f32 features + 4 u32 edge entries + 1 u32 target = 9 words
        assert_eq!(p.len() - 4 - hl, 9 * 4);
        let first = f32::from_le_bytes(p[4 + hl..4 + hl + 4].try_into().unwrap());
        assert_eq!(first, 1.0);
    }

    #[test]
    fn ragged_inputs_are_rejected_before_they_reach_the_worker() {
        let ragged = vec![vec![1.0f32], vec![1.0, 2.0]];
        let edges = vec![vec![0u32], vec![0u32]];
        assert!(encode_forward(&ragged, &edges, &[0], None).is_err());
        let feats = vec![vec![1.0f32]];
        let bad_edges = vec![vec![0u32, 1], vec![0u32]];
        assert!(encode_forward(&feats, &bad_edges, &[0], None).is_err());
    }

    #[test]
    fn decode_forward_ok_is_bit_exact_for_f32() {
        let vals = [0.1f32, -3.4028235e38, f32::MIN_POSITIVE, 1.0e-45];
        let header = br#"{"t":2,"dim":2}"#;
        let mut payload = (header.len() as u32).to_le_bytes().to_vec();
        payload.extend_from_slice(header);
        for v in vals {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        let (rows, attr) = decode_forward_ok(&payload).unwrap();
        assert!(attr.is_none());
        assert_eq!(rows[0][0].to_bits(), vals[0].to_bits());
        assert_eq!(rows[0][1].to_bits(), vals[1].to_bits());
        assert_eq!(rows[1][0].to_bits(), vals[2].to_bits());
        assert_eq!(rows[1][1].to_bits(), vals[3].to_bits());
    }

    #[test]
    fn a_body_whose_size_disagrees_with_the_header_is_rejected() {
        let header = br#"{"t":2,"dim":2}"#;
        let mut payload = (header.len() as u32).to_le_bytes().to_vec();
        payload.extend_from_slice(header);
        payload.extend_from_slice(&[0u8; 12]);
        assert!(decode_forward_ok(&payload).is_err());
    }
}
