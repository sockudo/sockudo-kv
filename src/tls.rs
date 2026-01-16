//! TLS/SSL configuration and certificate loading
//!
//! Provides rustls-based TLS configuration for secure client connections.

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{CipherSuite, ServerConfig, SupportedCipherSuite};
use std::fs::{self, File};
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

/// TLS server configuration options
#[derive(Default)]
pub struct TlsServerOptions<'a> {
    pub cert_path: &'a str,
    pub key_path: &'a str,
    pub key_password: Option<&'a str>,
    pub ca_cert_file: Option<&'a str>,
    pub ca_cert_dir: Option<&'a str>,
    pub auth_clients: &'a str, // "yes", "no", "optional"
    pub session_cache_size: usize,
    pub session_cache_timeout: u64,    // Not directly used by rustls
    pub prefer_server_ciphers: bool,   // Not directly used by rustls
    pub protocols: Option<&'a str>,    // e.g. "TLSv1.2 TLSv1.3"
    pub ciphers: Option<&'a str>,      // TLS 1.2 ciphers
    pub ciphersuites: Option<&'a str>, // TLS 1.3 ciphersuites
}

/// Load TLS server configuration from options
pub fn load_tls_config_with_options(opts: &TlsServerOptions) -> TlsResult<Arc<ServerConfig>> {
    // Load certificate chain
    let certs = load_certs(Path::new(opts.cert_path))?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates);
    }

    // Load private key (with optional password - note: rustls_pemfile doesn't support encrypted keys directly)
    let key = load_private_key(Path::new(opts.key_path), opts.key_password)?;

    // Load CA root store from file and/or directory
    let root_store = load_ca_roots(opts.ca_cert_file, opts.ca_cert_dir)?;

    // Build cipher suite list based on protocols and ciphers config
    let cipher_suites = build_cipher_suites(opts.protocols, opts.ciphers, opts.ciphersuites);

    // Build server config with custom cipher suites if specified
    let builder = if let Some(_suites) = cipher_suites {
        let versions = build_protocol_versions(opts.protocols);
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
            .with_protocol_versions(&versions)
            .map_err(|e| TlsError::InvalidConfig(e.to_string()))?
    } else {
        ServerConfig::builder()
    };

    // Configure client authentication
    let builder = match opts.auth_clients {
        "yes" | "required" => {
            if let Some(root_store) = root_store {
                let verifier = WebPkiClientVerifier::builder(root_store.into())
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
                let verifier = WebPkiClientVerifier::builder(root_store.into())
                    .allow_unauthenticated()
                    .build()
                    .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
                builder.with_client_cert_verifier(verifier)
            } else {
                builder.with_no_client_auth()
            }
        }
        _ => builder.with_no_client_auth(),
    };

    let mut config = builder.with_single_cert(certs, key)?;

    // Set session cache
    if opts.session_cache_size > 0 {
        config.session_storage =
            rustls::server::ServerSessionMemoryCache::new(opts.session_cache_size);
    } else {
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    }

    Ok(Arc::new(config))
}

/// Legacy function signature for backwards compatibility
pub fn load_tls_config(
    cert_path: &str,
    key_path: &str,
    key_password: Option<&str>,
    ca_path: Option<&str>,
    auth_type: &str,
    session_cache_size: usize,
    session_cache_timeout: u64,
    prefer_server_ciphers: bool,
) -> TlsResult<Arc<ServerConfig>> {
    load_tls_config_with_options(&TlsServerOptions {
        cert_path,
        key_path,
        key_password,
        ca_cert_file: ca_path,
        ca_cert_dir: None,
        auth_clients: auth_type,
        session_cache_size,
        session_cache_timeout,
        prefer_server_ciphers,
        protocols: None,
        ciphers: None,
        ciphersuites: None,
    })
}

/// Load CA certificates from file and/or directory
fn load_ca_roots(
    ca_file: Option<&str>,
    ca_dir: Option<&str>,
) -> TlsResult<Option<rustls::RootCertStore>> {
    let mut root_store = rustls::RootCertStore::empty();
    let mut loaded_any = false;

    // Load from file
    if let Some(path) = ca_file {
        let certs = load_certs(Path::new(path))?;
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
            loaded_any = true;
        }
    }

    // Load from directory
    if let Some(dir_path) = ca_dir {
        let dir = Path::new(dir_path);
        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                // Only load .pem, .crt, .cer files
                if let Some(ext) = path.extension() {
                    let ext = ext.to_string_lossy().to_lowercase();
                    if ext == "pem" || ext == "crt" || ext == "cer" {
                        if let Ok(certs) = load_certs(&path) {
                            for cert in certs {
                                let _ = root_store.add(cert); // Ignore individual errors
                                loaded_any = true;
                            }
                        }
                    }
                }
            }
        }
    }

    if loaded_any {
        Ok(Some(root_store))
    } else {
        Ok(None)
    }
}

/// Build protocol versions from config string
fn build_protocol_versions(
    protocols: Option<&str>,
) -> Vec<&'static rustls::SupportedProtocolVersion> {
    let protocols = match protocols {
        Some(p) if !p.is_empty() => p,
        _ => return vec![&rustls::version::TLS13, &rustls::version::TLS12],
    };

    let protocols_lower = protocols.to_lowercase();
    let mut versions = Vec::new();

    if protocols_lower.contains("tlsv1.3") || protocols_lower.contains("tls1.3") {
        versions.push(&rustls::version::TLS13);
    }
    if protocols_lower.contains("tlsv1.2") || protocols_lower.contains("tls1.2") {
        versions.push(&rustls::version::TLS12);
    }

    // TLS 1.0 and 1.1 are not supported by rustls (good!)
    if protocols_lower.contains("tlsv1.1") || protocols_lower.contains("tlsv1.0") {
        eprintln!("Warning: TLS 1.0 and 1.1 are not supported (deprecated), using TLS 1.2+");
    }

    if versions.is_empty() {
        // Default to TLS 1.2 and 1.3
        vec![&rustls::version::TLS13, &rustls::version::TLS12]
    } else {
        versions
    }
}

/// Build cipher suites from config strings
fn build_cipher_suites(
    _protocols: Option<&str>,
    ciphers: Option<&str>,
    ciphersuites: Option<&str>,
) -> Option<Vec<SupportedCipherSuite>> {
    // If no custom cipher config, use rustls defaults
    if ciphers.is_none() && ciphersuites.is_none() {
        return None;
    }

    let mut suites = Vec::new();

    // TLS 1.3 ciphersuites
    if let Some(cs) = ciphersuites {
        for name in cs.split_whitespace() {
            if let Some(suite) = cipher_suite_from_name(name) {
                suites.push(suite);
            }
        }
    } else {
        // Add default TLS 1.3 suites
        suites.extend(
            rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
                .iter()
                .filter(|s| {
                    matches!(
                        s.suite(),
                        CipherSuite::TLS13_AES_256_GCM_SHA384
                            | CipherSuite::TLS13_AES_128_GCM_SHA256
                            | CipherSuite::TLS13_CHACHA20_POLY1305_SHA256
                    )
                }),
        );
    }

    // TLS 1.2 ciphers
    if let Some(c) = ciphers {
        for name in c.split(':') {
            if let Some(suite) = cipher_suite_from_name(name) {
                if !suites.iter().any(|s| s.suite() == suite.suite()) {
                    suites.push(suite);
                }
            }
        }
    } else {
        // Add default TLS 1.2 suites
        suites.extend(
            rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
                .iter()
                .filter(|s| {
                    matches!(
                        s.suite(),
                        CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
                            | CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
                            | CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
                            | CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
                            | CipherSuite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
                            | CipherSuite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
                    )
                }),
        );
    }

    if suites.is_empty() {
        None
    } else {
        Some(suites)
    }
}

/// Map cipher suite name to rustls SupportedCipherSuite
fn cipher_suite_from_name(name: &str) -> Option<SupportedCipherSuite> {
    // Normalize: uppercase, replace hyphens with underscores
    let name_upper = name.to_uppercase().replace('-', "_");

    // TLS 1.3 suites
    if name_upper.contains("TLS_AES_256_GCM_SHA384") || name_upper.contains("TLS13_AES_256_GCM") {
        return rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
            .iter()
            .find(|s| s.suite() == CipherSuite::TLS13_AES_256_GCM_SHA384)
            .copied();
    }
    if name_upper.contains("TLS_AES_128_GCM_SHA256") || name_upper.contains("TLS13_AES_128_GCM") {
        return rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
            .iter()
            .find(|s| s.suite() == CipherSuite::TLS13_AES_128_GCM_SHA256)
            .copied();
    }
    if name_upper.contains("TLS_CHACHA20_POLY1305_SHA256") || name_upper.contains("CHACHA20") {
        return rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
            .iter()
            .find(|s| s.suite() == CipherSuite::TLS13_CHACHA20_POLY1305_SHA256)
            .copied();
    }

    // TLS 1.2 suites - handle both IANA (TLS_ECDHE_RSA_...) and OpenSSL (ECDHE_RSA_AES256_...) formats
    // ECDHE-ECDSA with AES-256-GCM
    if (name_upper.contains("ECDHE")
        && name_upper.contains("ECDSA")
        && name_upper.contains("AES")
        && name_upper.contains("256")
        && name_upper.contains("GCM"))
        || name_upper.contains("TLS_ECDHE_ECDSA_WITH_AES_256_GCM")
    {
        return rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
            .iter()
            .find(|s| s.suite() == CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384)
            .copied();
    }
    // ECDHE-RSA with AES-256-GCM
    if (name_upper.contains("ECDHE")
        && name_upper.contains("RSA")
        && name_upper.contains("AES")
        && name_upper.contains("256")
        && name_upper.contains("GCM"))
        || name_upper.contains("TLS_ECDHE_RSA_WITH_AES_256_GCM")
    {
        return rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
            .iter()
            .find(|s| s.suite() == CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
            .copied();
    }
    // ECDHE-ECDSA with AES-128-GCM
    if (name_upper.contains("ECDHE")
        && name_upper.contains("ECDSA")
        && name_upper.contains("AES")
        && name_upper.contains("128")
        && name_upper.contains("GCM"))
        || name_upper.contains("TLS_ECDHE_ECDSA_WITH_AES_128_GCM")
    {
        return rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
            .iter()
            .find(|s| s.suite() == CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256)
            .copied();
    }
    // ECDHE-RSA with AES-128-GCM
    if (name_upper.contains("ECDHE")
        && name_upper.contains("RSA")
        && name_upper.contains("AES")
        && name_upper.contains("128")
        && name_upper.contains("GCM"))
        || name_upper.contains("TLS_ECDHE_RSA_WITH_AES_128_GCM")
    {
        return rustls::crypto::aws_lc_rs::ALL_CIPHER_SUITES
            .iter()
            .find(|s| s.suite() == CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256)
            .copied();
    }

    None
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
/// Supports encrypted PKCS#8 keys when password is provided
fn load_private_key(path: &Path, password: Option<&str>) -> TlsResult<PrivateKeyDer<'static>> {
    let pem_content = std::fs::read_to_string(path)?;

    // If password is provided and key appears encrypted, try to decrypt
    if let Some(pwd) = password {
        if pem_content.contains("ENCRYPTED PRIVATE KEY") {
            use pkcs8::EncryptedPrivateKeyInfo;
            use pkcs8::der::Decode;

            // Parse PEM to get DER
            let pem = pem_content
                .lines()
                .filter(|line| !line.starts_with("-----"))
                .collect::<String>();

            if let Ok(der_bytes) = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                &pem.replace(['\n', '\r', ' '], ""),
            ) {
                if let Ok(encrypted_key) = EncryptedPrivateKeyInfo::from_der(&der_bytes) {
                    match encrypted_key.decrypt(pwd.as_bytes()) {
                        Ok(decrypted) => {
                            return Ok(PrivateKeyDer::Pkcs8(
                                rustls::pki_types::PrivatePkcs8KeyDer::from(
                                    decrypted.as_bytes().to_vec(),
                                ),
                            ));
                        }
                        Err(e) => {
                            return Err(TlsError::InvalidConfig(format!(
                                "Failed to decrypt private key: {}",
                                e
                            )));
                        }
                    }
                }
            }
        }
    }

    // Fall back to unencrypted PEM parsing
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
            Some(_) => continue,
            None => break,
        }
    }

    Err(TlsError::NoPrivateKey)
}

/// TLS client configuration options
#[derive(Default)]
pub struct TlsClientOptions<'a> {
    pub ca_cert_file: Option<&'a str>,
    pub ca_cert_dir: Option<&'a str>,
    pub client_cert_file: Option<&'a str>,
    pub client_key_file: Option<&'a str>,
    pub client_key_password: Option<&'a str>,
    pub protocols: Option<&'a str>,
    pub ciphers: Option<&'a str>,
    pub ciphersuites: Option<&'a str>,
}

/// Load TLS client configuration with all options
pub fn load_client_tls_config_with_options(
    opts: &TlsClientOptions,
) -> TlsResult<Arc<rustls::ClientConfig>> {
    // Load CA roots
    let root_store = load_ca_roots(opts.ca_cert_file, opts.ca_cert_dir)?
        .unwrap_or_else(rustls::RootCertStore::empty);

    // Build client config
    let versions = build_protocol_versions(opts.protocols);
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&versions)
    .map_err(|e| TlsError::InvalidConfig(e.to_string()))?
    .with_root_certificates(root_store);

    let config =
        if let (Some(cert_path), Some(key_path)) = (opts.client_cert_file, opts.client_key_file) {
            let certs = load_certs(Path::new(cert_path))?;
            let key = load_private_key(Path::new(key_path), opts.client_key_password)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(TlsError::Rustls)?
        } else {
            builder.with_no_client_auth()
        };

    Ok(Arc::new(config))
}

/// Legacy function for backwards compatibility
pub fn load_client_tls_config(
    ca_path: Option<&str>,
    cert_path: Option<&str>,
    key_path: Option<&str>,
) -> TlsResult<Arc<rustls::ClientConfig>> {
    load_client_tls_config_with_options(&TlsClientOptions {
        ca_cert_file: ca_path,
        ca_cert_dir: None,
        client_cert_file: cert_path,
        client_key_file: key_path,
        client_key_password: None,
        protocols: None,
        ciphers: None,
        ciphersuites: None,
    })
}

/// Extract the Common Name (CN) from a client certificate
/// Used for tls-auth-clients-user feature
pub fn extract_cn_from_cert(cert_der: &[u8]) -> Option<String> {
    // Parse the certificate using x509-parser
    use x509_parser::prelude::*;

    match X509Certificate::from_der(cert_der) {
        Ok((_, cert)) => {
            // Get the subject and find CN
            for rdn in cert.subject().iter() {
                for attr in rdn.iter() {
                    if attr.attr_type() == &oid_registry::OID_X509_COMMON_NAME {
                        if let Ok(cn) = attr.as_str() {
                            return Some(cn.to_string());
                        }
                    }
                }
            }
            None
        }
        Err(_) => None,
    }
}

/// Check if tls_auth_clients_user is configured and extract username from client cert
/// Returns the username to authenticate as, or None if not applicable
pub fn get_username_from_client_cert(
    auth_user_config: Option<&str>,
    client_cert: Option<&CertificateDer<'_>>,
) -> Option<String> {
    let auth_config = auth_user_config?;
    if auth_config.eq_ignore_ascii_case("off") || auth_config.is_empty() {
        return None;
    }

    let cert = client_cert?;

    if auth_config.eq_ignore_ascii_case("CN") {
        extract_cn_from_cert(cert.as_ref())
    } else {
        // Future: support other fields like SAN, etc.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_error_display() {
        let err = TlsError::NoCertificates;
        assert!(err.to_string().contains("No certificates"));
    }

    #[test]
    fn test_protocol_versions() {
        let versions = build_protocol_versions(Some("TLSv1.2 TLSv1.3"));
        assert_eq!(versions.len(), 2);

        let versions = build_protocol_versions(Some("TLSv1.3"));
        assert_eq!(versions.len(), 1);

        let versions = build_protocol_versions(None);
        assert_eq!(versions.len(), 2); // Default TLS 1.2 + 1.3
    }

    #[test]
    fn test_cipher_suite_lookup() {
        assert!(cipher_suite_from_name("TLS_AES_256_GCM_SHA384").is_some());
        assert!(cipher_suite_from_name("TLS_CHACHA20_POLY1305_SHA256").is_some());
        assert!(cipher_suite_from_name("ECDHE-RSA-AES256-GCM-SHA384").is_some());
        assert!(cipher_suite_from_name("INVALID_CIPHER").is_none());
    }
}
