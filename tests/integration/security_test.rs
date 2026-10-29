//! Identity and access control end to end: real TLS handshakes against the
//! hot-reloading acceptor, the caller's name taken from the client
//! certificate, roles decided by the policy file, and hashed tokens bound to
//! that same name. Certificates are generated in-process for every test.
//!
//! No embedding model is deployed, so a mutation that passes authorization
//! reaches the model check and fails `FAILED_PRECONDITION`; that code is how
//! these tests tell "allowed" from "denied" for writes.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use caregraph::api::proto::care_graph_service_client::CareGraphServiceClient;
use caregraph::api::proto::care_graph_service_server::CareGraphServiceServer;
use caregraph::api::proto::{
    AddEdgeRequest, EdgeType as ProtoEdgeType, ModelKind as ProtoModelKind, SnapshotRequest,
};
use caregraph::api::{CareGraphApi, MutualTlsInterceptor, ServiceOptions};
use caregraph::security::tokens::hash_token;
use caregraph::security::{tls, PolicyHandle, TlsPaths, TlsState, TokenHandle};
use caregraph::storage::RocksKv;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
use tempfile::TempDir;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, Server};
use tonic::{Code, Request};

struct Ca {
    cert_pem: String,
    key_pair: KeyPair,
    cert: rcgen::Certificate,
}

fn make_ca() -> Ca {
    let key_pair = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["Security Test CA".to_string()]).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let cert = params.self_signed(&key_pair).unwrap();
    Ca {
        cert_pem: cert.pem(),
        key_pair,
        cert,
    }
}

/// A leaf signed by `ca`, carrying exactly the given subject-alternative-names.
fn leaf(ca: &Ca, sans: Vec<SanType>) -> (String, String) {
    let key_pair = KeyPair::generate().unwrap();
    let mut params = CertificateParams::default();
    params.subject_alt_names = sans;
    let cert = params.signed_by(&key_pair, &ca.cert, &ca.key_pair).unwrap();
    (cert.pem(), key_pair.serialize_pem())
}

fn server_leaf(ca: &Ca) -> (String, String) {
    leaf(ca, vec![SanType::DnsName("localhost".try_into().unwrap())])
}

fn workload(ca: &Ca, name: &str) -> (String, String) {
    leaf(
        ca,
        vec![SanType::URI(
            format!("spiffe://caregraph/{name}").try_into().unwrap(),
        )],
    )
}

struct Fixture {
    dir: TempDir,
    ca: Ca,
    addr: SocketAddr,
    tls: Arc<TlsState>,
    policy: Arc<PolicyHandle>,
    _server: tokio::task::JoinHandle<()>,
}

fn write(path: &PathBuf, text: &str) {
    std::fs::write(path, text).unwrap();
}

async fn start(policy_text: &str, tokens: Option<&str>) -> Fixture {
    let dir = TempDir::new().unwrap();
    let ca = make_ca();
    let (cert, key) = server_leaf(&ca);
    let paths = TlsPaths {
        cert: dir.path().join("server.pem"),
        key: dir.path().join("server.key"),
        client_ca: dir.path().join("ca.pem"),
        crl: None,
    };
    write(&paths.cert, &cert);
    write(&paths.key, &key);
    write(&paths.client_ca, &ca.cert_pem);
    let tls = TlsState::load(paths).expect("load TLS state");

    let policy_path = dir.path().join("rbac.toml");
    write(&policy_path, policy_text);
    let policy = PolicyHandle::from_file(&policy_path).unwrap();

    let token_handle = tokens.map(|t| {
        let p = dir.path().join("tokens.toml");
        write(&p, t);
        TokenHandle::from_file(&p).unwrap()
    });

    let store = RocksKv::open(dir.path().join("db")).unwrap();
    let registry = prometheus::Registry::new();
    let api = CareGraphApi::with_options(
        store,
        None,
        None,
        &registry,
        ServiceOptions {
            policy: Some(policy.clone()),
            ..ServiceOptions::default()
        },
    )
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tls::incoming(listener, tls.clone(), Duration::from_secs(5));
    let service = InterceptedService::new(
        CareGraphServiceServer::new(api),
        MutualTlsInterceptor::new(token_handle),
    );
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    Fixture {
        dir,
        ca,
        addr,
        tls,
        policy,
        _server: server,
    }
}

async fn client(
    f: &Fixture,
    identity: Option<(String, String)>,
) -> CareGraphServiceClient<Channel> {
    let mut cfg = ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(f.ca.cert_pem.clone()));
    if let Some((c, k)) = identity {
        cfg = cfg.identity(Identity::from_pem(c, k));
    }
    let channel = Channel::from_shared(format!("https://{}", f.addr))
        .unwrap()
        .tls_config(cfg)
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("channel");
    CareGraphServiceClient::new(channel)
}

fn snapshot() -> SnapshotRequest {
    SnapshotRequest {
        min_sequence: 0,
        subject: 1,
        as_of_us: u64::MAX,
        edge_types: vec![],
    }
}

fn add_edge() -> AddEdgeRequest {
    AddEdgeRequest {
        src: 1,
        dst: 2,
        edge_type: ProtoEdgeType::DiagnosedWith as i32,
        timestamp_us: 0,
        properties_json: String::new(),
        model: ProtoModelKind::Graphsage as i32,
        explain: false,
        request_id: String::new(),
    }
}

fn with_token<T>(msg: T, token: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

const POLICY: &str = r#"
[principals]
"spiffe://caregraph/ingest"  = ["writer"]
"spiffe://caregraph/analyst" = ["reader"]
"#;

#[tokio::test]
async fn each_role_can_call_exactly_what_it_is_allowed_to() {
    let f = start(POLICY, None).await;

    let mut analyst = client(&f, Some(workload(&f.ca, "analyst"))).await;
    assert!(analyst.snapshot(Request::new(snapshot())).await.is_ok());
    let denied = analyst
        .add_edge(Request::new(add_edge()))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let mut ingest = client(&f, Some(workload(&f.ca, "ingest"))).await;
    assert!(ingest.snapshot(Request::new(snapshot())).await.is_ok());
    let reached_model = ingest.add_edge(Request::new(add_edge())).await.unwrap_err();
    assert_eq!(
        reached_model.code(),
        Code::FailedPrecondition,
        "a writer passes authorization and reaches the (absent) model"
    );

    // Valid certificate, but a name the policy has never heard of.
    let mut stranger = client(&f, Some(workload(&f.ca, "stranger"))).await;
    let err = stranger
        .snapshot(Request::new(snapshot()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(
        !err.message().contains("reader") && !err.message().contains("writer"),
        "a denial does not describe the role model: {}",
        err.message()
    );
}

#[tokio::test]
async fn a_certificate_with_no_usable_identity_is_unauthenticated() {
    let f = start(POLICY, None).await;
    let nameless = leaf(&f.ca, vec![]);
    let mut c = client(&f, Some(nameless)).await;
    let err = c.snapshot(Request::new(snapshot())).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn editing_the_policy_file_changes_access_without_a_restart() {
    let f = start(POLICY, None).await;
    let mut analyst = client(&f, Some(workload(&f.ca, "analyst"))).await;
    assert!(analyst.add_edge(Request::new(add_edge())).await.is_err());
    assert_eq!(
        analyst
            .add_edge(Request::new(add_edge()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    write(
        &f.dir.path().join("rbac.toml"),
        "[principals]\n\"spiffe://caregraph/analyst\" = [\"writer\"]\n",
    );
    assert!(f.policy.reload_if_changed().unwrap());
    assert_eq!(
        analyst
            .add_edge(Request::new(add_edge()))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition,
        "promoted to writer, now reaches the model check"
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    write(&f.dir.path().join("rbac.toml"), "[principals]\n");
    assert!(f.policy.reload_if_changed().unwrap());
    assert_eq!(
        analyst
            .snapshot(Request::new(snapshot()))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "removed from the policy, access is gone"
    );
}

fn token_file(entries: &[(&str, &str, &str, u64)]) -> String {
    entries
        .iter()
        .map(|(id, token, principal, exp)| {
            let h: String = hash_token(token).iter().map(|b| format!("{b:02x}")).collect();
            format!(
                "[[token]]\nid=\"{id}\"\nsha256=\"{h}\"\nprincipal=\"{principal}\"\nnot_after_unix={exp}\n"
            )
        })
        .collect()
}

#[tokio::test]
async fn a_token_must_be_valid_unexpired_and_belong_to_the_certificates_principal() {
    let far = 4_000_000_000u64;
    let tokens = token_file(&[
        (
            "analyst-1",
            "analyst-secret",
            "spiffe://caregraph/analyst",
            far,
        ),
        (
            "ingest-1",
            "ingest-secret",
            "spiffe://caregraph/ingest",
            far,
        ),
        (
            "analyst-old",
            "expired-secret",
            "spiffe://caregraph/analyst",
            1,
        ),
    ]);
    let f = start(POLICY, Some(&tokens)).await;
    let mut analyst = client(&f, Some(workload(&f.ca, "analyst"))).await;

    assert!(analyst
        .snapshot(with_token(snapshot(), "analyst-secret"))
        .await
        .is_ok());

    for (label, req) in [
        ("no token", Request::new(snapshot())),
        ("wrong token", with_token(snapshot(), "guess")),
        ("expired token", with_token(snapshot(), "expired-secret")),
        (
            "another principal's valid token",
            with_token(snapshot(), "ingest-secret"),
        ),
    ] {
        let err = analyst.snapshot(req).await.unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated, "{label}");
    }

    // The mismatch and the plain bad token are indistinguishable to the caller.
    let a = analyst
        .snapshot(with_token(snapshot(), "ingest-secret"))
        .await
        .unwrap_err();
    let b = analyst
        .snapshot(with_token(snapshot(), "guess"))
        .await
        .unwrap_err();
    assert_eq!(a.message(), b.message());
}

#[tokio::test]
async fn a_client_without_a_certificate_never_reaches_the_service() {
    let f = start(POLICY, None).await;
    let mut c = client(&f, None).await;
    assert!(c.snapshot(Request::new(snapshot())).await.is_err());
}

/// The serial number of the certificate the server presents, read with a raw
/// TLS client so the test sees exactly what a new connection is given.
async fn presented_server_serial(f: &Fixture) -> Vec<u8> {
    use rustls::pki_types::ServerName;
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls_pemfile::certs(&mut f.ca.cert_pem.as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let (cert_pem, key_pem) = workload(&f.ca, "analyst");
    let chain: Vec<_> = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .map(|c| c.unwrap())
        .collect();
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .unwrap()
        .unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)
        .unwrap();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(f.addr).await.unwrap();
    let stream = connector
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    let der = stream.get_ref().1.peer_certificates().unwrap()[0].clone();
    use x509_parser::prelude::{FromDer, X509Certificate};
    let (_, cert) = X509Certificate::from_der(der.as_ref()).unwrap();
    cert.tbs_certificate.raw_serial().to_vec()
}

#[tokio::test]
async fn a_renewed_server_certificate_is_served_without_a_restart() {
    let f = start(POLICY, None).await;
    let before = presented_server_serial(&f).await;
    assert_eq!(f.tls.reload_count(), 0);

    // cert-manager style renewal: new certificate and key written in place.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let (cert, key) = server_leaf(&f.ca);
    write(&f.dir.path().join("server.pem"), &cert);
    write(&f.dir.path().join("server.key"), &key);
    assert!(f.tls.reload_if_changed().unwrap());
    assert_eq!(f.tls.reload_count(), 1);

    let after = presented_server_serial(&f).await;
    assert_ne!(before, after, "new connections get the renewed certificate");

    // A corrupted file does not take the server down or change what it serves.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    write(&f.dir.path().join("server.pem"), "garbage");
    assert!(f.tls.reload_if_changed().is_err());
    assert_eq!(f.tls.reload_failure_count(), 1);
    assert_eq!(presented_server_serial(&f).await, after);
    assert!(f.tls.cert_not_after_unix() > 0);
}
