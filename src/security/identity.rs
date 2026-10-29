//! Who is calling: the principal named by a verified client certificate.
//!
//! The certificate has already been validated against the trusted CA by the
//! TLS layer by the time this runs; this only decides which name in it to
//! treat as the caller's identity. A URI subject-alternative-name is preferred
//! (SPIFFE style, `spiffe://<trust-domain>/<workload>`), then a DNS name. The
//! subject common name is not used: it is deprecated for identity and easy to
//! get wrong.

use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

/// The principal for a DER-encoded client certificate, or `None` if it carries
/// no usable subject-alternative-name.
pub fn principal_from_cert(der: &[u8]) -> Option<String> {
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    let san = cert.subject_alternative_name().ok().flatten()?;
    let names = &san.value.general_names;

    let uri = names.iter().find_map(|n| match n {
        GeneralName::URI(u) if u.starts_with("spiffe://") => Some((*u).to_string()),
        _ => None,
    });
    uri.or_else(|| {
        names.iter().find_map(|n| match n {
            GeneralName::DNSName(d) => Some((*d).to_string()),
            _ => None,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, KeyPair, SanType};

    fn cert_with(sans: Vec<SanType>) -> Vec<u8> {
        let mut params = CertificateParams::default();
        params.subject_alt_names = sans;
        let key = KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().der().to_vec()
    }

    #[test]
    fn a_spiffe_uri_wins_over_a_dns_name() {
        let der = cert_with(vec![
            SanType::DnsName("ingest.example".try_into().unwrap()),
            SanType::URI("spiffe://caregraph/ingest".try_into().unwrap()),
        ]);
        assert_eq!(
            principal_from_cert(&der).as_deref(),
            Some("spiffe://caregraph/ingest")
        );
    }

    #[test]
    fn a_dns_name_is_used_when_there_is_no_spiffe_uri() {
        let der = cert_with(vec![SanType::DnsName(
            "analyst.example".try_into().unwrap(),
        )]);
        assert_eq!(
            principal_from_cert(&der).as_deref(),
            Some("analyst.example")
        );
    }

    #[test]
    fn a_certificate_without_names_or_garbage_has_no_principal() {
        assert_eq!(principal_from_cert(&cert_with(vec![])), None);
        assert_eq!(principal_from_cert(b"not a certificate"), None);
    }
}
