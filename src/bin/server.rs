//! Sockudo-KV Server Binary
//!
//! Main Redis-compatible server binary.
//! For now, this is just a symbolic link to the main sockudo-kv binary

// Just re-export everything from the main binary
fn main() {
    // The actual implementation is in src/main.rs
    // This is a placeholder to indicate we should symlink or copy the main binary
    eprintln!("Please use './sockudo-kv' or './sockudo-kv-sentinel sentinel.conf' instead");
    eprintln!("This binary will be properly implemented soon.");
    std::process::exit(1);
}
