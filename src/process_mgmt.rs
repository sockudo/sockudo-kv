//! Process management module
//!
//! Implements systemd notification and process title customization

/// Notify systemd that the service is ready
#[cfg(target_os = "linux")]
pub fn notify_systemd_ready() -> bool {
    // Check NOTIFY_SOCKET environment variable
    if let Ok(socket_path) = std::env::var("NOTIFY_SOCKET") {
        use std::os::unix::net::UnixDatagram;

        if let Ok(socket) = UnixDatagram::unbound() {
            let path = if socket_path.starts_with('@') {
                // Abstract socket
                format!("\0{}", &socket_path[1..])
            } else {
                socket_path
            };

            let _ = socket.send_to(b"READY=1", path);
            return true;
        }
    }
    false
}

#[cfg(not(target_os = "linux"))]
pub fn notify_systemd_ready() -> bool {
    false // systemd not available on non-Linux
}

/// Notify systemd of service stopping
#[cfg(target_os = "linux")]
pub fn notify_systemd_stopping() -> bool {
    if let Ok(socket_path) = std::env::var("NOTIFY_SOCKET") {
        use std::os::unix::net::UnixDatagram;

        if let Ok(socket) = UnixDatagram::unbound() {
            let path = if socket_path.starts_with('@') {
                format!("\0{}", &socket_path[1..])
            } else {
                socket_path
            };

            let _ = socket.send_to(b"STOPPING=1", path);
            return true;
        }
    }
    false
}

#[cfg(not(target_os = "linux"))]
pub fn notify_systemd_stopping() -> bool {
    false
}

/// Set process title (for ps listing)
/// Uses prctl on Linux, does nothing on other platforms
#[cfg(target_os = "linux")]
pub fn set_process_title(title: &str) {
    use std::io::Write;

    // Try to write to /proc/self/comm (limited to 15 chars)
    if let Ok(mut file) = std::fs::File::create("/proc/self/comm") {
        let title_bytes = if title.len() > 15 {
            &title.as_bytes()[..15]
        } else {
            title.as_bytes()
        };
        let _ = file.write_all(title_bytes);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set_process_title(_title: &str) {
    // Not supported on this platform
}

/// Apply process title template with variable substitution
/// Supports: {title}, {port}, {config-file}
pub fn format_process_title(template: &str, port: u16, config_file: &str) -> String {
    template
        .replace("{title}", "sockudo-kv")
        .replace("{port}", &port.to_string())
        .replace("{config-file}", config_file)
        .replace("{listen-addr}", &format!("*:{}", port))
        .replace("{server-mode}", "standalone")
}

/// Handle supervised mode based on config
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisedMode {
    No,
    Upstart,
    Systemd,
    Auto,
}

impl SupervisedMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "upstart" => Self::Upstart,
            "systemd" => Self::Systemd,
            "auto" => Self::Auto,
            _ => Self::No,
        }
    }

    /// Detect and apply supervised mode
    pub fn apply(&self) {
        match self {
            SupervisedMode::Systemd => {
                let _ = notify_systemd_ready();
            }
            SupervisedMode::Auto => {
                // Auto-detect systemd
                if std::env::var("NOTIFY_SOCKET").is_ok() {
                    let _ = notify_systemd_ready();
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_process_title() {
        let title = format_process_title("{title} {port}", 6379, "redis.conf");
        assert_eq!(title, "sockudo-kv 6379");
    }

    #[test]
    fn test_supervised_mode_parse() {
        assert_eq!(SupervisedMode::from_str("systemd"), SupervisedMode::Systemd);
        assert_eq!(SupervisedMode::from_str("auto"), SupervisedMode::Auto);
        assert_eq!(SupervisedMode::from_str("no"), SupervisedMode::No);
    }
}
