//! Phase 7 — mutual TLS on the gRPC listener, proven against a real TLS
//! handshake over a real ephemeral TCP port, not a config value that's
//! merely present.
//!
//! Three real network round-trips: a client presenting a certificate signed
//! by the server's trusted CA can call an RPC end to end; a client
//! presenting no certificate at all is rejected during the TLS handshake
//! itself (never reaches the RPC layer); a client presenting a certificate
//! from a *different* CA (untrusted) is rejected the same way. Certificates
//! are generated fresh in-process via `rcgen` for each test run — nothing
//! committed, nothing that can go stale.

use std::net::SocketAddr;
use std::time::Duration;

use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::care_graph_service_server::CareGraphServiceServer;
use caregraph::api::proto::SnapshotRequest;
use caregraph::api::{AuthInterceptor, CareGraphApi};
use caregraph::storage::RocksKv;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use tempfile::TempDir;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, Server, ServerTlsConfig};
use tonic::Request;

const API_KEY: &str = "test-only-shared-secret";

/// One CA plus a cert/key pair signed by it, all PEM-encoded — everything
/// `tonic::transport`'s `Identity`/`Certificate` types need.
struct Ca {
    cert_pem: String,
    key_pair: KeyPair,
    cert: rcgen::Certificate,
}

fn make_ca(name: &str) -> Ca {
    let key_pair = KeyPair::generate().expect("generate CA key");
    let mut params = CertificateParams::new(vec![name.to_string()]).expect("CA params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let cert = params.self_signed(&key_pair).expect("self-sign CA");
    Ca {
        cert_pem: cert.pem(),
        key_pair,
        cert,
    }
}

/// A leaf certificate (server or client) signed by `ca`.
fn make_leaf(ca: &Ca, common_name: &str) -> (String, String) {
    let key_pair = KeyPair::generate().expect("generate leaf key");
    let params = CertificateParams::new(vec![common_name.to_string()]).expect("leaf params");
    let cert = params
        .signed_by(&key_pair, &ca.cert, &ca.key_pair)
        .expect("sign leaf cert");
    (cert.pem(), key_pair.serialize_pem())
}

/// Binds a real ephemeral TCP port and serves `CareGraphApi` behind mTLS —
/// `client_ca` is the CA whose signed client certificates the server trusts.
async fn spawn_mtls_server(
    server_ca: &Ca,
    client_ca: &Ca,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let dir = TempDir::new().expect("temp dir");
    let store = RocksKv::open(dir.path().join("caregraph")).expect("open rocksdb");
    std::mem::forget(dir); // kept alive for the server task's lifetime

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr: SocketAddr = listener.local_addr().expect("local addr");
    drop(listener);

    let registry = prometheus::Registry::new();
    let api = CareGraphApi::new(store, None, None, &registry).expect("build service");
    let interceptor = AuthInterceptor::new(API_KEY);
    let service = CareGraphServiceServer::with_interceptor(api, interceptor);

    let (server_cert_pem, server_key_pem) = make_leaf(server_ca, "localhost");
    let tls_config = ServerTlsConfig::new()
        .identity(Identity::from_pem(server_cert_pem, server_key_pem))
        .client_ca_root(Certificate::from_pem(client_ca.cert_pem.clone()));

    let handle = tokio::spawn(async move {
        Server::builder()
            .tls_config(tls_config)
            .expect("configure server mTLS")
            .add_service(service)
            .serve(addr)
            .await
            .expect("server");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, handle)
}

async fn connect_with(
    addr: SocketAddr,
    trusted_ca_pem: &str,
    client_identity: Option<(String, String)>,
) -> Result<Channel, tonic::transport::Error> {
    let mut tls = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(trusted_ca_pem));
    if let Some((cert_pem, key_pem)) = client_identity {
        tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
    }

    Channel::from_shared(format!("https://{addr}"))
        .expect("valid uri")
        .tls_config(tls)
        .expect("valid client tls config")
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
}

fn authed<T>(msg: T) -> Request<T> {
    let mut req = Request::new(msg);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {API_KEY}").parse().unwrap(),
    );
    req
}

#[tokio::test]
async fn a_client_with_a_trusted_certificate_completes_a_real_rpc() {
    let server_ca = make_ca("CareGraph Test Server CA");
    let client_ca = make_ca("CareGraph Test Client CA");
    let (addr, _handle) = spawn_mtls_server(&server_ca, &client_ca).await;

    let client_identity = make_leaf(&client_ca, "test-client");
    let channel = connect_with(addr, &server_ca.cert_pem, Some(client_identity))
        .await
        .expect("a client with a certificate signed by the trusted CA must connect");

    let mut client = CareGraphServiceClient::new(channel);
    let response = client
        .snapshot(authed(SnapshotRequest {
            subject: 1,
            as_of_us: 0,
            edge_types: vec![],
        }))
        .await
        .expect("RPC over a real mTLS connection must succeed");
    // Not asserting on subject existence here (that's Rule 2's own test's
    // job) — reaching a real response at all through the TLS handshake is
    // what this suite is proving.
    let _ = response.into_inner();
}

/// `Channel::connect()` is lazy in tonic/hyper: it builds a channel object
/// without necessarily performing the TLS handshake, which only happens on
/// the first real request. So `.connect()` succeeding proves nothing here —
/// the handshake (and rejection) is only observable by actually issuing an
/// RPC and seeing it fail. Found by running the negative-case tests below
/// and watching them fail even though the rejection this project cares
/// about was, in fact, happening — just one call later than assumed.
async fn connect_then_attempt_rpc(
    addr: SocketAddr,
    trusted_ca_pem: &str,
    client_identity: Option<(String, String)>,
) -> bool {
    let channel = match connect_with(addr, trusted_ca_pem, client_identity).await {
        Ok(channel) => channel,
        Err(_) => return false, // rejected at connect() time — also acceptable
    };
    let mut client = CareGraphServiceClient::new(channel);
    client
        .snapshot(authed(SnapshotRequest {
            subject: 1,
            as_of_us: 0,
            edge_types: vec![],
        }))
        .await
        .is_ok()
}

#[tokio::test]
async fn a_client_with_no_certificate_is_rejected_at_the_tls_handshake() {
    let server_ca = make_ca("CareGraph Test Server CA");
    let client_ca = make_ca("CareGraph Test Client CA");
    let (addr, _handle) = spawn_mtls_server(&server_ca, &client_ca).await;

    let rpc_succeeded = connect_then_attempt_rpc(addr, &server_ca.cert_pem, None).await;
    assert!(
        !rpc_succeeded,
        "mTLS violation: a client presenting no certificate completed a real RPC \
         against a server that requires one"
    );
}

#[tokio::test]
async fn a_client_with_a_certificate_from_an_untrusted_ca_is_rejected() {
    let server_ca = make_ca("CareGraph Test Server CA");
    let client_ca = make_ca("CareGraph Test Client CA");
    let (addr, _handle) = spawn_mtls_server(&server_ca, &client_ca).await;

    // Signed by a CA the server was never told to trust as a client_ca_root.
    let rogue_ca = make_ca("Rogue CA (not trusted by the server)");
    let rogue_identity = make_leaf(&rogue_ca, "rogue-client");

    let rpc_succeeded =
        connect_then_attempt_rpc(addr, &server_ca.cert_pem, Some(rogue_identity)).await;
    assert!(
        !rpc_succeeded,
        "mTLS violation: a client certificate signed by an untrusted CA completed a \
         real RPC"
    );
}
