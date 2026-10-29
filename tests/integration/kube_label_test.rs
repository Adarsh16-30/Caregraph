//! The pod-label sidecar against a stand-in API server: a real TLS listener that
//! records the request it receives, and the real `/role` endpoint of the metrics
//! server.

use std::sync::{Arc, Mutex};

use caregraph::kube_label::{http_get, patch_role, ApiAccess, ROLE_LABEL};
use caregraph::server::Health;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

struct Pki {
    ca_pem: String,
    leaf_der: Vec<u8>,
    leaf_key: Vec<u8>,
}

fn pki(server_name: &str) -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["Fake Cluster CA".to_string()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let leaf_key = KeyPair::generate().unwrap();
    let leaf_params = CertificateParams::new(vec![server_name.to_string()]).unwrap();
    let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
    Pki {
        ca_pem: ca.pem(),
        leaf_der: leaf.der().to_vec(),
        leaf_key: leaf_key.serialize_der(),
    }
}

/// Serves one TLS connection, records the raw request, answers with `status_line`
/// and `body`. Returns the listening address and a slot for the request.
async fn fake_api(
    pki: &Pki,
    status_line: &'static str,
    body: &'static str,
) -> (String, Arc<Mutex<String>>) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(pki.leaf_der.clone())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pki.leaf_key.clone())),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen = Arc::new(Mutex::new(String::new()));
    let slot = seen.clone();
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let Ok(mut tls) = acceptor.accept(tcp).await else {
            return;
        };
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = tls.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&buf).to_string();
            if let Some((head, rest)) = text.split_once("\r\n\r\n") {
                let want: usize = head
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse().unwrap_or(0))
                    })
                    .unwrap_or(0);
                if rest.len() >= want {
                    break;
                }
            }
        }
        *slot.lock().unwrap() = String::from_utf8_lossy(&buf).to_string();
        let response = format!(
            "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = tls.write_all(response.as_bytes()).await;
        let _ = tls.shutdown().await;
    });
    (addr, seen)
}

fn access(addr: String, ca_pem: String) -> ApiAccess {
    ApiAccess {
        addr,
        server_name: "kubernetes.default.svc".to_string(),
        ca_pem: ca_pem.into_bytes(),
        token: "sa-token-123".to_string(),
    }
}

#[tokio::test]
async fn the_role_label_is_patched_on_the_pods_own_object_with_the_service_account_token() {
    let pki = pki("kubernetes.default.svc");
    let (addr, seen) = fake_api(&pki, "HTTP/1.1 200 OK", "{}").await;

    patch_role(
        &access(addr, pki.ca_pem.clone()),
        "care",
        "caregraph-writer-1",
        "leader",
    )
    .await
    .unwrap();

    let req = seen.lock().unwrap().clone();
    assert!(
        req.starts_with("PATCH /api/v1/namespaces/care/pods/caregraph-writer-1 HTTP/1.1\r\n"),
        "{req}"
    );
    assert!(
        req.contains("Authorization: Bearer sa-token-123\r\n"),
        "{req}"
    );
    assert!(
        req.contains("Content-Type: application/merge-patch+json\r\n"),
        "{req}"
    );
    let body: serde_json::Value =
        serde_json::from_str(req.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(body["metadata"]["labels"][ROLE_LABEL], "leader");
}

#[tokio::test]
async fn a_refusal_from_the_api_server_is_an_error_that_says_why() {
    let pki = pki("kubernetes.default.svc");
    let (addr, _) = fake_api(
        &pki,
        "HTTP/1.1 403 Forbidden",
        r#"{"message":"pods \"caregraph-writer-1\" is forbidden"}"#,
    )
    .await;
    let e = patch_role(
        &access(addr, pki.ca_pem.clone()),
        "care",
        "caregraph-writer-1",
        "follower",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(e.contains("403"), "{e}");
    assert!(e.contains("forbidden"), "{e}");
}

#[tokio::test]
async fn an_api_server_that_the_pod_does_not_trust_never_receives_the_token() {
    let real = pki("kubernetes.default.svc");
    let other = pki("kubernetes.default.svc");
    let (addr, seen) = fake_api(&real, "HTTP/1.1 200 OK", "{}").await;
    // The pod trusts a different CA than the one that signed the server.
    let result = patch_role(&access(addr, other.ca_pem), "care", "pod-0", "leader").await;
    assert!(result.is_err(), "the handshake must fail");
    assert!(
        seen.lock().unwrap().is_empty(),
        "no request, and so no bearer token, may reach an untrusted server"
    );
}

#[tokio::test]
async fn the_metrics_server_reports_the_role_the_label_sidecar_reads() {
    let health = Health::new(Arc::new(Vec::new), Arc::new(|| true));
    let app = caregraph::metrics_server::router(prometheus::Registry::new(), health.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, app).await });

    assert_eq!(http_get(&addr, "/role").await.unwrap(), "standalone");
    health.set_role_probe(Arc::new(|| "leader"));
    assert_eq!(http_get(&addr, "/role").await.unwrap(), "leader");
    assert!(http_get(&addr, "/no-such-path").await.is_err());
}
