//! Transport-level limits and per-request input limits.
//!
//! Two different things live here. [`ServerLimits`] configures the gRPC
//! transport (timeouts, message sizes, connection and stream caps, drain
//! behaviour). [`InputLimits`] bounds what a single request may carry into the
//! storage layer: `properties_json` size and nesting, and how far in the future
//! a client timestamp may be (a far-future timestamp under the `strict` policy
//! would otherwise poison the high-water mark and lock every other writer out).

// `tonic::Status` is tonic's required error type for request validation; see the
// same allowance and reasoning at the top of `src/api/mod.rs`.
#![allow(clippy::result_large_err)]

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use tonic::Status;

use crate::config::Source;

#[derive(Debug, Clone)]
pub struct ServerLimits {
    /// Whole-request deadline applied by the transport.
    pub request_timeout: Duration,
    /// Largest request message the server will decode.
    pub max_decoding_bytes: usize,
    /// Largest response message the server will encode.
    pub max_encoding_bytes: usize,
    pub concurrency_limit_per_connection: usize,
    pub max_concurrent_streams: u32,
    pub http2_keepalive_interval: Duration,
    pub http2_keepalive_timeout: Duration,
    /// After SIGTERM: how long to stay up but report not-ready so the load
    /// balancer stops sending traffic before the listener closes.
    pub prestop_delay: Duration,
    /// Upper bound on draining in-flight requests before the process exits.
    pub drain_deadline: Duration,
    /// Per-principal token bucket, requests per second. 0 disables limiting.
    pub rate_limit_rps: u32,
    pub rate_limit_burst: u32,
}

impl Default for ServerLimits {
    fn default() -> Self {
        ServerLimits {
            // Explain-enabled mutations legitimately take tens of seconds
            // (docs/benchmark_report.md section 2.6), so the ceiling is generous;
            // the bulkheads, not this, are what bound ordinary load.
            request_timeout: Duration::from_secs(120),
            max_decoding_bytes: 1 << 20,
            max_encoding_bytes: 16 << 20,
            concurrency_limit_per_connection: 256,
            max_concurrent_streams: 512,
            http2_keepalive_interval: Duration::from_secs(30),
            http2_keepalive_timeout: Duration::from_secs(10),
            prestop_delay: Duration::ZERO,
            drain_deadline: Duration::from_secs(30),
            rate_limit_rps: 0,
            rate_limit_burst: 0,
        }
    }
}

fn num(src: &dyn Source, name: &str, default: u64) -> Result<u64> {
    match src.get(name) {
        Some(v) => v
            .parse::<u64>()
            .with_context(|| format!("{name} must be a non-negative integer, got `{v}`")),
        None => Ok(default),
    }
}

fn secs(src: &dyn Source, name: &str, default: Duration) -> Result<Duration> {
    Ok(Duration::from_secs(num(src, name, default.as_secs())?))
}

impl ServerLimits {
    pub fn load(src: &dyn Source) -> Result<Self> {
        let d = ServerLimits::default();
        let limits = ServerLimits {
            request_timeout: secs(src, "CAREGRAPH_REQUEST_TIMEOUT_SECS", d.request_timeout)?,
            max_decoding_bytes: num(
                src,
                "CAREGRAPH_MAX_REQUEST_BYTES",
                d.max_decoding_bytes as u64,
            )? as usize,
            max_encoding_bytes: num(
                src,
                "CAREGRAPH_MAX_RESPONSE_BYTES",
                d.max_encoding_bytes as u64,
            )? as usize,
            concurrency_limit_per_connection: num(
                src,
                "CAREGRAPH_CONN_CONCURRENCY",
                d.concurrency_limit_per_connection as u64,
            )? as usize,
            max_concurrent_streams: num(
                src,
                "CAREGRAPH_MAX_CONCURRENT_STREAMS",
                d.max_concurrent_streams as u64,
            )? as u32,
            http2_keepalive_interval: secs(
                src,
                "CAREGRAPH_KEEPALIVE_INTERVAL_SECS",
                d.http2_keepalive_interval,
            )?,
            http2_keepalive_timeout: secs(
                src,
                "CAREGRAPH_KEEPALIVE_TIMEOUT_SECS",
                d.http2_keepalive_timeout,
            )?,
            prestop_delay: secs(src, "CAREGRAPH_PRESTOP_DELAY_SECS", d.prestop_delay)?,
            drain_deadline: secs(src, "CAREGRAPH_DRAIN_DEADLINE_SECS", d.drain_deadline)?,
            rate_limit_rps: num(src, "CAREGRAPH_RATE_LIMIT_RPS", 0)? as u32,
            rate_limit_burst: num(src, "CAREGRAPH_RATE_LIMIT_BURST", 0)? as u32,
        };
        limits.validate()?;
        Ok(limits)
    }

    pub fn validate(&self) -> Result<()> {
        if self.request_timeout.is_zero() {
            return Err(anyhow!("CAREGRAPH_REQUEST_TIMEOUT_SECS must be > 0"));
        }
        if self.max_decoding_bytes < 1024 {
            return Err(anyhow!("CAREGRAPH_MAX_REQUEST_BYTES must be at least 1024"));
        }
        if self.concurrency_limit_per_connection == 0 || self.max_concurrent_streams == 0 {
            return Err(anyhow!("connection concurrency limits must be > 0"));
        }
        if self.drain_deadline.is_zero() {
            return Err(anyhow!("CAREGRAPH_DRAIN_DEADLINE_SECS must be > 0"));
        }
        if self.rate_limit_rps > 0 && self.rate_limit_burst == 0 {
            return Err(anyhow!(
                "CAREGRAPH_RATE_LIMIT_BURST must be > 0 when CAREGRAPH_RATE_LIMIT_RPS is set"
            ));
        }
        Ok(())
    }
}

/// Per-request input bounds, checked before any storage or model work.
#[derive(Debug, Clone)]
pub struct InputLimits {
    pub max_properties_bytes: usize,
    pub max_properties_depth: usize,
    /// A client timestamp may be at most this far ahead of the server clock.
    pub max_future_skew_us: u64,
    /// Whether `explain = true` is honoured. Attribution costs tens of seconds
    /// per request, so production leaves it off unless explicitly enabled.
    pub explain_enabled: bool,
}

impl Default for InputLimits {
    fn default() -> Self {
        InputLimits {
            max_properties_bytes: 64 * 1024,
            max_properties_depth: 32,
            max_future_skew_us: 5 * 60 * 1_000_000,
            explain_enabled: true,
        }
    }
}

impl InputLimits {
    pub fn load(src: &dyn Source, explain_enabled: bool) -> Result<Self> {
        let d = InputLimits::default();
        Ok(InputLimits {
            max_properties_bytes: num(
                src,
                "CAREGRAPH_MAX_PROPERTIES_BYTES",
                d.max_properties_bytes as u64,
            )? as usize,
            max_properties_depth: num(
                src,
                "CAREGRAPH_MAX_PROPERTIES_DEPTH",
                d.max_properties_depth as u64,
            )? as usize,
            max_future_skew_us: num(src, "CAREGRAPH_MAX_FUTURE_SKEW_SECS", 300)?
                .saturating_mul(1_000_000),
            explain_enabled,
        })
    }

    /// Parses `properties_json`, refusing oversize or deeply nested documents
    /// before the parser allocates anything for them. Empty means `{}`; the
    /// value must be a JSON object.
    pub fn parse_properties(&self, raw: &str) -> Result<Value, Status> {
        if raw.is_empty() {
            return Ok(Value::Object(Default::default()));
        }
        if raw.len() > self.max_properties_bytes {
            return Err(Status::invalid_argument(format!(
                "properties_json is {} bytes; the limit is {}",
                raw.len(),
                self.max_properties_bytes
            )));
        }
        if json_depth(raw.as_bytes()) > self.max_properties_depth {
            return Err(Status::invalid_argument(format!(
                "properties_json nests deeper than {} levels",
                self.max_properties_depth
            )));
        }
        let value: Value = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("properties_json: {e}")))?;
        if !value.is_object() {
            return Err(Status::invalid_argument(
                "properties_json must be a JSON object",
            ));
        }
        Ok(value)
    }

    /// `0` means "server assigns"; anything else must not be further in the
    /// future than the skew allowance.
    pub fn check_timestamp(&self, ts_us: u64, now_us: u64) -> Result<(), Status> {
        if ts_us != 0 && ts_us > now_us.saturating_add(self.max_future_skew_us) {
            return Err(Status::invalid_argument(format!(
                "timestamp_us is more than {} seconds ahead of the server clock",
                self.max_future_skew_us / 1_000_000
            )));
        }
        Ok(())
    }
}

/// Maximum bracket nesting in a JSON text, ignoring brackets inside strings.
/// A scan, not a parse, so a hostile document costs one pass and no allocation.
pub fn json_depth(bytes: &[u8]) -> usize {
    let (mut depth, mut max) = (0usize, 0usize);
    let (mut in_string, mut escaped) = (false, false);
    for &b in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                max = max.max(depth);
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_ignores_brackets_inside_strings() {
        assert_eq!(json_depth(br#"{"a":"[[[[{{{"}"#), 1);
        assert_eq!(json_depth(br#"{"a":[{"b":[1]}]}"#), 4);
        assert_eq!(json_depth(br#"{"a":"quote \" [ still string"}"#), 1);
    }

    #[test]
    fn properties_limits_are_enforced() {
        let l = InputLimits {
            max_properties_bytes: 64,
            max_properties_depth: 3,
            ..InputLimits::default()
        };
        assert!(l.parse_properties("").unwrap().is_object());
        assert!(l.parse_properties(r#"{"a":1}"#).is_ok());
        let big = format!(r#"{{"a":"{}"}}"#, "x".repeat(100));
        assert_eq!(
            l.parse_properties(&big).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let deep = format!("{}1{}", r#"{"a":"#.repeat(5), "}".repeat(5));
        assert_eq!(
            l.parse_properties(&deep).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert!(l.parse_properties("[1,2]").is_err(), "must be an object");
        assert!(l.parse_properties("{not json").is_err());
    }

    #[test]
    fn far_future_timestamps_are_refused_and_zero_is_allowed() {
        let l = InputLimits::default();
        let now = 1_000_000_000_000u64;
        assert!(l.check_timestamp(0, now).is_ok());
        assert!(l.check_timestamp(now - 1, now).is_ok());
        assert!(l.check_timestamp(now + 299_000_000, now).is_ok());
        assert!(l.check_timestamp(now + 301_000_000, now).is_err());
        assert!(l.check_timestamp(u64::MAX, now).is_err());
    }
}
