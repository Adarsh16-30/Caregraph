//! Mutual TLS with certificates that reload without a restart.
//!
//! tonic's `ServerTlsConfig` is fixed when the server starts, which forces a
//! restart (and a drain) every time a short-lived certificate is renewed. This
//! module terminates TLS itself: a TCP accept loop hands each connection to a
//! `tokio_rustls` acceptor built from the *current* [`ServerConfig`], which a
//! background task rebuilds whenever the certificate, key, client CA or CRL
//! files change. New connections get the new certificate; established ones
//! keep theirs until they close.
//!
//! TLS 1.3 only; clients must present a certificate that chains to the
//! configured CA (and is not on the configured CRL, if any). The verified
//! peer certificate is exposed to request handlers as [`PeerInfo`].

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use rustls::pki_types::CertificateDer;
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tonic::transport::server::Connected;

use crate::security::identity::principal_from_cert;

/// Where the TLS material lives.
#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: PathBuf,
    /// Optional certificate revocation list(s) in PEM.
    pub crl: Option<PathBuf>,
}

/// The authenticated peer of one connection, attached to every request that
/// arrives on it.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub remote_addr: Option<SocketAddr>,
    /// Name from the verified client certificate; `None` if it had no usable
    /// subject-alternative-name.
    pub principal: Option<String>,
}

/// A TLS stream that tells tonic who is on the other end.
pub struct TlsIo {
    inner: TlsStream<TcpStream>,
    info: PeerInfo,
}

impl Connected for TlsIo {
    type ConnectInfo = PeerInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for TlsIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Shared, reloadable TLS configuration.
pub struct TlsState {
    paths: TlsPaths,
    current: RwLock<Arc<ServerConfig>>,
    fingerprint: RwLock<Vec<Option<SystemTime>>>,
    /// Unix seconds at which the serving certificate expires (for alerting).
    cert_not_after: AtomicI64,
    reloads: AtomicU64,
    reload_failures: AtomicU64,
    metrics: std::sync::OnceLock<TlsMetrics>,
}

struct TlsMetrics {
    expiry: prometheus::IntGauge,
    reloads: prometheus::IntGauge,
    failures: prometheus::IntGauge,
}

impl TlsState {
    pub fn load(paths: TlsPaths) -> Result<Arc<Self>> {
        let (config, not_after) = build_config(&paths)?;
        let fingerprint = fingerprint(&paths);
        Ok(Arc::new(TlsState {
            paths,
            current: RwLock::new(Arc::new(config)),
            fingerprint: RwLock::new(fingerprint),
            cert_not_after: AtomicI64::new(not_after),
            reloads: AtomicU64::new(0),
            reload_failures: AtomicU64::new(0),
            metrics: std::sync::OnceLock::new(),
        }))
    }

    /// Exposes expiry and reload counts as Prometheus gauges, refreshed on
    /// every watch tick. Alert on `tls_cert_expiry_seconds` approaching zero.
    pub fn register_metrics(&self, registry: &prometheus::Registry) -> Result<()> {
        let g = |name: &str, help: &str| -> Result<prometheus::IntGauge> {
            let g = prometheus::IntGauge::new(name, help)?;
            registry.register(Box::new(g.clone()))?;
            Ok(g)
        };
        let m = TlsMetrics {
            expiry: g(
                "tls_cert_expiry_seconds",
                "Seconds until the serving TLS certificate expires",
            )?,
            reloads: g("tls_reloads", "Successful TLS material reloads")?,
            failures: g("tls_reload_failures", "Failed TLS material reloads")?,
        };
        let _ = self.metrics.set(m);
        self.refresh_metrics();
        Ok(())
    }

    fn refresh_metrics(&self) {
        if let Some(m) = self.metrics.get() {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            m.expiry.set(self.cert_not_after_unix() - now);
            m.reloads.set(self.reload_count() as i64);
            m.failures.set(self.reload_failure_count() as i64);
        }
    }

    pub fn config(&self) -> Arc<ServerConfig> {
        self.current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn cert_not_after_unix(&self) -> i64 {
        self.cert_not_after.load(Ordering::Relaxed)
    }

    pub fn reload_count(&self) -> u64 {
        self.reloads.load(Ordering::Relaxed)
    }

    pub fn reload_failure_count(&self) -> u64 {
        self.reload_failures.load(Ordering::Relaxed)
    }

    /// Rebuilds the configuration if any input file changed. On failure the
    /// previous configuration stays in force.
    pub fn reload_if_changed(&self) -> Result<bool> {
        let now = fingerprint(&self.paths);
        if *self.fingerprint.read().unwrap_or_else(|e| e.into_inner()) == now {
            return Ok(false);
        }
        match build_config(&self.paths) {
            Ok((config, not_after)) => {
                *self.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(config);
                *self.fingerprint.write().unwrap_or_else(|e| e.into_inner()) = now;
                self.cert_not_after.store(not_after, Ordering::Relaxed);
                self.reloads.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(e) => {
                // Remember the fingerprint so a broken file is reported once,
                // not on every poll.
                *self.fingerprint.write().unwrap_or_else(|e| e.into_inner()) = now;
                self.reload_failures.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    pub async fn watch(self: Arc<Self>, every: Duration) {
        loop {
            tokio::time::sleep(every).await;
            let result = self.reload_if_changed();
            self.refresh_metrics();
            match result {
                Ok(true) => tracing::info!("TLS certificate material reloaded"),
                Ok(false) => {}
                Err(e) => tracing::error!(
                    error = %e,
                    "TLS reload failed; continuing with the previous certificate"
                ),
            }
        }
    }
}

fn fingerprint(paths: &TlsPaths) -> Vec<Option<SystemTime>> {
    let mtime = |p: &Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
    vec![
        mtime(&paths.cert),
        mtime(&paths.key),
        mtime(&paths.client_ca),
        paths.crl.as_deref().and_then(mtime),
    ]
}

fn build_config(paths: &TlsPaths) -> Result<(ServerConfig, i64)> {
    let cert_pem = std::fs::read(&paths.cert)
        .with_context(|| format!("reading TLS cert at {}", paths.cert.display()))?;
    let key_pem = std::fs::read(&paths.key)
        .with_context(|| format!("reading TLS key at {}", paths.key.display()))?;
    let ca_pem = std::fs::read(&paths.client_ca)
        .with_context(|| format!("reading client CA at {}", paths.client_ca.display()))?;

    let chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<std::result::Result<_, _>>()
        .context("parsing TLS certificate chain")?;
    if chain.is_empty() {
        bail!("{} contains no certificate", paths.cert.display());
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .context("parsing TLS private key")?
        .ok_or_else(|| anyhow!("{} contains no private key", paths.key.display()))?;

    let mut roots = RootCertStore::empty();
    for ca in rustls_pemfile::certs(&mut ca_pem.as_slice()) {
        roots
            .add(ca.context("parsing client CA certificate")?)
            .context("adding client CA to the trust store")?;
    }
    if roots.is_empty() {
        bail!("{} contains no CA certificate", paths.client_ca.display());
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone());
    if let Some(crl_path) = &paths.crl {
        let crl_pem = std::fs::read(crl_path)
            .with_context(|| format!("reading CRL at {}", crl_path.display()))?;
        let crls: Vec<_> = rustls_pemfile::crls(&mut crl_pem.as_slice())
            .collect::<std::result::Result<_, _>>()
            .context("parsing CRL")?;
        verifier = verifier.with_crls(crls);
    }
    let verifier = verifier.build().context("building the client verifier")?;

    let not_after = leaf_not_after(&chain[0]);

    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("selecting TLS 1.3")?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)
        .context("installing the server certificate and key")?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok((config, not_after))
}

/// Expiry of a DER certificate as Unix seconds, or 0 if it cannot be parsed.
fn leaf_not_after(der: &CertificateDer<'_>) -> i64 {
    use x509_parser::prelude::{FromDer, X509Certificate};
    X509Certificate::from_der(der.as_ref())
        .map(|(_, c)| c.validity().not_after.timestamp())
        .unwrap_or(0)
}

/// Accepts TCP connections on `listener`, completes mutual TLS on each in its
/// own task (a slow handshake never blocks accepting), and yields the
/// established streams. Failed handshakes are logged and dropped.
pub fn incoming(
    listener: tokio::net::TcpListener,
    state: Arc<TlsState>,
    handshake_timeout: Duration,
) -> impl tokio_stream::Stream<Item = std::io::Result<TlsIo>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<TlsIo>>(64);
    tokio::spawn(async move {
        loop {
            let (tcp, remote) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    // Transient (e.g. out of file descriptors): back off, don't spin.
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let _ = tcp.set_nodelay(true);
            let acceptor = TlsAcceptor::from(state.config());
            let tx = tx.clone();
            tokio::spawn(async move {
                let handshake = tokio::time::timeout(handshake_timeout, acceptor.accept(tcp));
                match handshake.await {
                    Ok(Ok(stream)) => {
                        let principal = stream
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|c| c.first())
                            .and_then(|c| principal_from_cert(c.as_ref()));
                        let io = TlsIo {
                            inner: stream,
                            info: PeerInfo {
                                remote_addr: Some(remote),
                                principal,
                            },
                        };
                        let _ = tx.send(Ok(io)).await;
                    }
                    Ok(Err(e)) => tracing::debug!(%remote, error = %e, "TLS handshake failed"),
                    Err(_) => tracing::debug!(%remote, "TLS handshake timed out"),
                }
            });
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}
