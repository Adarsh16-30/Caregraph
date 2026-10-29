//! Keeps a pod's `caregraph.io/role` label equal to the role its server is in.
//!
//! The chart's `caregraph-write` Service selects `caregraph.io/role=leader`. The
//! server decides its own role (standalone, leader, follower, demoted, fenced) and
//! persists it, so after a promotion or a pod restart the only thing that can be
//! right is the server's answer. A sidecar running `caregraph-admin kube-label-role`
//! asks the local server for its role (`GET /role` on the metrics port) and patches the
//! pod's label through the Kubernetes API, so the Service follows a failover without a
//! person relabelling pods, and a recreated pod is relabelled within one interval.
//!
//! This needs one `PATCH`, so it speaks HTTP/1.1 over rustls directly rather than
//! pulling in a Kubernetes client crate. The pod's service account needs `patch` on
//! its own pod (the chart grants exactly that, by name).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// The label the write Service selects on.
pub const ROLE_LABEL: &str = "caregraph.io/role";

/// Where the API server and the pod's credentials are, normally read from the
/// environment and the service-account mount.
#[derive(Debug, Clone)]
pub struct ApiAccess {
    /// `host:port` of the API server.
    pub addr: String,
    /// Name in the API server's certificate.
    pub server_name: String,
    pub ca_pem: Vec<u8>,
    pub token: String,
}

const SERVICE_ACCOUNT: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

impl ApiAccess {
    /// Reads the in-cluster configuration: `KUBERNETES_SERVICE_HOST` / `_PORT` and the
    /// mounted token and CA.
    pub fn in_cluster() -> Result<Self> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST")
            .context("KUBERNETES_SERVICE_HOST is not set: this only runs inside a pod")?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
        let dir = PathBuf::from(SERVICE_ACCOUNT);
        let token = std::fs::read_to_string(dir.join("token"))
            .context("reading the service-account token")?
            .trim()
            .to_string();
        let ca_pem = std::fs::read(dir.join("ca.crt")).context("reading the cluster CA")?;
        let addr = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        Ok(ApiAccess {
            addr,
            server_name: "kubernetes.default.svc".to_string(),
            ca_pem,
            token,
        })
    }
}

fn tls_connector(ca_pem: &[u8]) -> Result<TlsConnector> {
    let mut roots = RootCertStore::empty();
    for ca in rustls_pemfile::certs(&mut &*ca_pem) {
        roots.add(ca.context("parsing the cluster CA")?)?;
    }
    if roots.is_empty() {
        bail!("the cluster CA file holds no certificate");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

/// The request body for setting one label, as a JSON merge patch.
pub fn label_patch(value: &str) -> String {
    serde_json::json!({"metadata": {"labels": {ROLE_LABEL: value}}}).to_string()
}

/// Sets `caregraph.io/role=<value>` on `pod` in `namespace`.
pub async fn patch_role(api: &ApiAccess, namespace: &str, pod: &str, value: &str) -> Result<()> {
    let plain = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    };
    if !(plain(value) && plain(namespace) && plain(pod)) {
        bail!("refusing to send a namespace, pod name or label value with unexpected characters");
    }
    let body = label_patch(value);
    let request = format!(
        "PATCH /api/v1/namespaces/{namespace}/pods/{pod} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Authorization: Bearer {token}\r\n\
         Content-Type: application/merge-patch+json\r\n\
         Accept: application/json\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\r\n{body}",
        host = api.server_name,
        token = api.token,
        len = body.len(),
    );
    let tcp = TcpStream::connect(&api.addr)
        .await
        .with_context(|| format!("connecting to the API server at {}", api.addr))?;
    let name = ServerName::try_from(api.server_name.clone())
        .map_err(|_| anyhow!("{} is not a valid server name", api.server_name))?;
    let mut tls = tls_connector(&api.ca_pem)?.connect(name, tcp).await?;
    tls.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    // The server closes the connection after the response (Connection: close); a
    // close without a TLS close_notify is still a complete response.
    let _ = tls.read_to_end(&mut response).await;
    let head = String::from_utf8_lossy(&response);
    let status_line = head.lines().next().unwrap_or_default();
    let code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| anyhow!("the API server sent no HTTP status"))?;
    if !(200..300).contains(&code) {
        // The body of a Status object explains why (forbidden, not found, ...).
        let reason = head.split("\r\n\r\n").nth(1).unwrap_or("").trim();
        bail!("the API server answered {code} to the label patch: {reason}");
    }
    Ok(())
}

/// `GET http://<addr><path>` over plain TCP; returns the trimmed body of a 200.
pub async fn http_get(addr: &str, path: &str) -> Result<String> {
    let mut tcp = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to {addr}"))?;
    tcp.write_all(
        format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await?;
    let mut response = Vec::new();
    tcp.read_to_end(&mut response).await?;
    let text = String::from_utf8_lossy(&response);
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .unwrap_or("");
    if status != "200" {
        bail!(
            "GET {path} answered `{}`",
            head.lines().next().unwrap_or("")
        );
    }
    Ok(body.trim().to_string())
}

/// Polls the local server's role and keeps the pod label equal to it, until the
/// process is stopped. A role that cannot be read leaves the label as it is: a server
/// that is restarting must not be relabelled "unknown", and a label that is stale for
/// a few seconds is no worse than before.
pub async fn run(
    api: ApiAccess,
    namespace: String,
    pod: String,
    status_addr: String,
    interval: Duration,
) -> Result<()> {
    let mut applied: Option<(String, std::time::Instant)> = None;
    loop {
        match http_get(&status_addr, "/role").await {
            Ok(role) => {
                // Re-assert periodically so a label removed by hand comes back.
                let stale = applied
                    .as_ref()
                    .map_or(true, |(r, at)| *r != role || at.elapsed() > interval * 12);
                if stale {
                    match patch_role(&api, &namespace, &pod, &role).await {
                        Ok(()) => {
                            tracing::info!(%pod, %role, "pod role label set");
                            applied = Some((role, std::time::Instant::now()));
                        }
                        Err(e) => tracing::warn!(error = %e, "setting the pod role label failed"),
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "could not read the server's role"),
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_patch_sets_exactly_the_role_label() {
        let v: serde_json::Value = serde_json::from_str(&label_patch("leader")).unwrap();
        assert_eq!(v["metadata"]["labels"][ROLE_LABEL], "leader");
        assert_eq!(v["metadata"]["labels"].as_object().unwrap().len(), 1);
        assert_eq!(v["metadata"].as_object().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn an_odd_label_value_is_refused_before_anything_is_sent() {
        let api = ApiAccess {
            addr: "127.0.0.1:1".into(),
            server_name: "kubernetes.default.svc".into(),
            ca_pem: Vec::new(),
            token: "t".into(),
        };
        let e = patch_role(&api, "ns", "pod", "leader\"}")
            .await
            .unwrap_err();
        assert!(e.to_string().contains("unexpected characters"), "{e}");
        assert!(patch_role(&api, "ns/../x", "pod", "leader").await.is_err());
    }
}
