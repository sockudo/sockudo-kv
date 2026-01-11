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
    ca_path: Option<&str>,
    auth_type: &str,
    session_cache_size: usize,
    _session_cache_timeout: u64, // rustls handles session lifetime via tickets, cache size is main control
    _prefer_server_ciphers: bool, // rustls ignores this safely
) -> TlsResult<Arc<ServerConfig>> {
    // Load certificate chain
    let certs = load_certs(Path::new(cert_path))?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates);
    }

    // Load private key
    let key = load_private_key(Path::new(key_path))?;

    // Load CA root store if configured or needed for client auth
    let root_store = if let Some(ca_path) = ca_path {
        let certs = load_certs(Path::new(ca_path))?;
        let mut root_store = rustls::RootCertStore::empty();
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
        }
        Some(root_store)
    } else {
        None
    };

    // Build server config
    let builder = ServerConfig::builder();

    let builder = match auth_type {
        "yes" | "required" => {
            if let Some(root_store) = root_store {
                let verifier = rustls::server::WebPkiClientVerifier::builder(root_store.into())
                    .build()
                    .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
                builder.with_client_cert_verifier(verifier)
            } else {
                return Err(TlsError::InvalidConfig(
                    "Client auth required but no CA specified".into(),
                ));
            }
        }
        "optional" => {
            if let Some(root_store) = root_store {
                let verifier = rustls::server::WebPkiClientVerifier::builder(root_store.into())
                    .allow_unauthenticated()
                    .build()
                    .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
                builder.with_client_cert_verifier(verifier)
            } else {
                // If optional but no CA, effectively no auth? Or error? Redis allows no CA?
                builder.with_no_client_auth()
            }
        }
        _ => builder.with_no_client_auth(),
    };

    let mut config = builder.with_single_cert(certs, key)?;

    // Set session cache
    if session_cache_size > 0 {
        config.session_storage = rustls::server::ServerSessionMemoryCache::new(session_cache_size);
    } else {
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    }

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

/// Load TLS client configuration
pub fn load_client_tls_config(
    ca_path: Option<&str>,
    cert_path: Option<&str>,
    key_path: Option<&str>,
) -> TlsResult<Arc<rustls::ClientConfig>> {
    let mut root_store = rustls::RootCertStore::empty();

    // Load CA certs if provided
    if let Some(ca_path) = ca_path {
        let certs = load_certs(Path::new(ca_path))?;
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
        }
    } else {
        // If no CA provided, we might want to load system roots?
        // For strict Redis compat, usually you provide CA.
        // But let's load native certs strictly? No, let's keep it empty if not provided -> fail validation usually.
        // Or we can use webpki-roots if needed.
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(root_store);

    let config = if let (Some(cert_path), Some(key_path)) = (cert_path, key_path) {
        let certs = load_certs(Path::new(cert_path))?;
        let key = load_private_key(Path::new(key_path))?;
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| TlsError::Rustls(e))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(Arc::new(config))
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
