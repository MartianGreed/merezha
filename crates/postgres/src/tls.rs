//! rustls connector for `tls = "require"`: platform trust store plus an
//! optional PEM CA bundle, full chain and host name verification.
use merezha_domain::Failure;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::{io::Read, path::Path, sync::Arc};
use tokio_postgres_rustls::MakeRustlsConnect;

/// Upper bound for the CA bundle file read at startup.
const MAX_CA_FILE_BYTES: u64 = 1_048_576;

pub(crate) fn connector(ca_file: Option<&Path>) -> Result<MakeRustlsConnect, Failure> {
    let failure = || Failure::permanent("tls_config");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let extra = match ca_file {
        Some(path) => read_roots(path)?,
        None => Vec::new(),
    };
    let verifier =
        rustls_platform_verifier::Verifier::new_with_extra_roots(extra, provider.clone())
            .map_err(|_| failure())?;
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| failure())?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}

fn read_roots(path: &Path) -> Result<Vec<CertificateDer<'static>>, Failure> {
    let failure = || Failure::permanent("tls_ca_file");
    let file = std::fs::File::open(path).map_err(|_| failure())?;
    if !file.metadata().map_err(|_| failure())?.is_file() {
        return Err(failure());
    }
    let mut pem = Vec::new();
    file.take(MAX_CA_FILE_BYTES + 1)
        .read_to_end(&mut pem)
        .map_err(|_| failure())?;
    if pem.len() as u64 > MAX_CA_FILE_BYTES {
        return Err(failure());
    }
    let roots = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| failure())?;
    if roots.is_empty() {
        return Err(failure());
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ca_file_must_contain_certificates() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a certificate").unwrap();
        assert_eq!(connector(Some(&empty)).err().unwrap().code, "tls_ca_file");
        assert_eq!(
            connector(Some(&dir.path().join("missing.pem")))
                .err()
                .unwrap()
                .code,
            "tls_ca_file"
        );
        assert_eq!(
            connector(Some(dir.path())).err().unwrap().code,
            "tls_ca_file"
        );
        assert!(connector(None).is_ok());
        let ca = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/test-ca.pem");
        assert!(connector(Some(&ca)).is_ok());
    }
}
