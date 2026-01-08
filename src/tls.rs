//! TLS/SSL configuration and certificate loading
//!
//! Provides rustls-based TLS configuration for secure client connections.

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

/// TLS configuration result
pub type TlsResult<T> = Result<T, TlsError>;

/// TLS configuration errors
#[derive(Debug)]
pub enum TlsError {
    Io(std::io::Error),
    Rustls(rustls::Error),
    NoCertificates,
    NoPrivateKey,
    InvalidConfig(String),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Io(e) => write!(f, "IO error: {}", e),
            TlsError::Rustls(e) => write!(f, "TLS error: {}", e),
            TlsError::NoCertificates => write!(f, "No certificates found in file"),
            TlsError::NoPrivateKey => write!(f, "No private key found in file"),
            TlsError::InvalidConfig(s) => write!(f, "Invalid TLS config: {}", s),
        }
    }
}

impl std::error::Error for TlsError {}

impl From<std::io::Error> for TlsError {
    fn from(e: std::io::Error) -> Self {
        TlsError::Io(e)
    }
}

impl From<rustls::Error> for TlsError {
    fn from(e: rustls::Error) -> Self {
        TlsError::Rustls(e)
    }
}

/// Load TLS configuration from certificate and key files
pub fn load_tls_config(
    cert_path: &str,
    key_path: &str,
    _key_password: Option<&str>,
    _ca_cert_path: Option<&str>,
) -> TlsResult<Arc<ServerConfig>> {
    // Load certificate chain
    let certs = load_certs(Path::new(cert_path))?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates);
    }

    // Load private key
    let key = load_private_key(Path::new(key_path))?;

    // Build server config
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(Arc::new(config))
}

/// Load certificates from a PEM file
fn load_certs(path: &Path) -> TlsResult<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .filter_map(|r| r.ok())
        .collect();

    Ok(certs)
}

/// Load private key from a PEM file
fn load_private_key(path: &Path) -> TlsResult<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Try to find any type of private key
    loop {
        match rustls_pemfile::read_one(&mut reader)? {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Some(rustls_pemfile::Item::Sec1Key(key)) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            Some(_) => continue, // Skip other items
            None => break,
        }
    }

    Err(TlsError::NoPrivateKey)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_tls_error_display() {
        use super::TlsError;
        let err = TlsError::NoCertificates;
        assert!(err.to_string().contains("No certificates"));
    }
}
