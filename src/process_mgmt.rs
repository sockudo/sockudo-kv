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
            let path = if let Some(abstract_name) = socket_path.strip_prefix('@') {
                // Abstract socket
                format!("\0{}", abstract_name)
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
            let path = if let Some(abstract_name) = socket_path.strip_prefix('@') {
                format!("\0{}", abstract_name)
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

/// Daemonize the process (fork to background)
///
/// This performs the standard Unix daemon dance:
/// 1. Fork and exit parent
/// 2. Create new session (setsid)
/// 3. Fork again and exit parent
/// 4. Change directory to /
/// 5. Redirect stdin/stdout/stderr to /dev/null
#[cfg(unix)]
pub fn daemonize() -> Result<(), String> {
    use std::fs::File;
    use std::os::unix::io::AsRawFd;

    // First fork
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err("First fork failed".to_string());
    }
    if pid > 0 {
        // Parent exits
        std::process::exit(0);
    }

    // Create new session
    if unsafe { libc::setsid() } < 0 {
        return Err("setsid failed".to_string());
    }

    // Second fork to prevent acquiring a controlling terminal
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err("Second fork failed".to_string());
    }
    if pid > 0 {
        // First child exits
        std::process::exit(0);
    }

    // Change working directory to /
    let _ = std::env::set_current_dir("/");

    // Set file mode creation mask
    unsafe { libc::umask(0) };

    // Redirect stdin, stdout, stderr to /dev/null
    if let Ok(dev_null) = File::open("/dev/null") {
        let null_fd = dev_null.as_raw_fd();
        unsafe {
            libc::dup2(null_fd, 0); // stdin
            libc::dup2(null_fd, 1); // stdout
            libc::dup2(null_fd, 2); // stderr
        }
    }

    Ok(())
}

#[cfg(not(unix))]
pub fn daemonize() -> Result<(), String> {
    Err("Daemonizing is not supported on this platform. Use a service manager instead.".to_string())
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

impl std::str::FromStr for SupervisedMode {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "upstart" => Self::Upstart,
            "systemd" => Self::Systemd,
            "auto" => Self::Auto,
            _ => Self::No,
        })
    }
}

impl SupervisedMode {
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
        assert_eq!(
            "systemd".parse::<SupervisedMode>().unwrap(),
            SupervisedMode::Systemd
        );
        assert_eq!(
            "auto".parse::<SupervisedMode>().unwrap(),
            SupervisedMode::Auto
        );
        assert_eq!("no".parse::<SupervisedMode>().unwrap(), SupervisedMode::No);
    }
}
