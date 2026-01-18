//! Lua scripting engine for Redis-compatible EVAL and FUNCTION commands
//!
//! This module provides high-performance Lua script execution with:
//! - Script caching via SHA1 digest
//! - `redis.call()` and `redis.pcall()` for executing Redis commands
//! - KEYS and ARGV argument passing
//! - Function library support for FUNCTION commands

use bytes::Bytes;
use dashmap::DashMap;
use mlua::{Function, Lua, Result as LuaResult, Value, Variadic};
use sha1::{Digest, Sha1};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::protocol::RespValue;
use crate::storage::Store;

/// A compiled Lua script ready for execution
#[derive(Clone)]
pub struct CompiledScript {
    /// SHA1 digest of the script source
    pub sha1: String,
    /// Original script source code
    pub source: String,
}

/// A Lua function library (Redis 7.0+ FUNCTION)
#[derive(Clone)]
pub struct LuaLibrary {
    pub name: String,
    pub code: String,
    pub functions: Vec<String>,
}

/// The Lua scripting engine
pub struct LuaEngine {
    /// Script cache: SHA1 -> CompiledScript
    scripts: DashMap<String, CompiledScript>,
    /// Function libraries: library name -> LuaLibrary
    libraries: DashMap<String, LuaLibrary>,
    /// Kill flag for script interruption
    kill_flag: AtomicBool,
}

impl LuaEngine {
    /// Create a new Lua engine
    pub fn new() -> Self {
        Self {
            scripts: DashMap::new(),
            libraries: DashMap::new(),
            kill_flag: AtomicBool::new(false),
        }
    }

    /// Request to kill any running script
    pub fn request_kill(&self) {
        self.kill_flag.store(true, Ordering::SeqCst);
    }

    /// Clear kill flag
    pub fn clear_kill(&self) {
        self.kill_flag.store(false, Ordering::SeqCst);
    }

    /// Check if kill was requested
    pub fn should_kill(&self) -> bool {
        self.kill_flag.load(Ordering::SeqCst)
    }

    /// Get number of loaded libraries
    pub fn library_count(&self) -> usize {
        self.libraries.len()
    }

    /// Compute SHA1 hash of script
    #[inline]
    pub fn sha1_hex(script: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(script.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Load a script into the cache, returning its SHA1
    pub fn script_load(&self, script: String) -> String {
        let sha1 = Self::sha1_hex(&script);
        self.scripts.insert(
            sha1.clone(),
            CompiledScript {
                sha1: sha1.clone(),
                source: script,
            },
        );
        sha1
    }

    /// Check if scripts exist by SHA1
    pub fn script_exists(&self, sha1s: &[&str]) -> Vec<bool> {
        sha1s
            .iter()
            .map(|sha| self.scripts.contains_key(*sha))
            .collect()
    }

    /// Flush all cached scripts
    pub fn script_flush(&self) {
        self.scripts.clear();
    }

    /// Get script by SHA1
    pub fn get_script(&self, sha1: &str) -> Option<CompiledScript> {
        self.scripts.get(sha1).map(|s| s.clone())
    }

    /// Execute a script with KEYS and ARGV
    pub fn execute(
        &self,
        store: &Store,
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    ) -> Result<RespValue, String> {
        // Clear any previous kill request
        self.clear_kill();

        // Create a new Lua state for this execution
        let lua = Lua::new();

        // Set up KEYS and ARGV globals
        self.setup_keys_argv(&lua, &keys, &args)?;

        // Set up the redis table with call/pcall using scope for non-static store
        self.setup_redis_api(&lua, store)?;

        // Execute the script
        let result = lua
            .load(script)
            .eval::<Value>()
            .map_err(|e| format!("ERR Error running script: {}", e))?;

        // Convert result to RespValue
        lua_value_to_resp(&lua, result)
    }

    /// Execute a script by SHA1
    pub fn execute_sha(
        &self,
        store: &Store,
        sha1: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    ) -> Result<RespValue, String> {
        let script = self
            .get_script(sha1)
            .ok_or_else(|| "NOSCRIPT No matching script. Please use EVAL.".to_string())?;

        self.execute(store, &script.source, keys, args)
    }

    /// Set up the redis.call() and redis.pcall() functions
    fn setup_redis_api(&self, lua: &Lua, store: &Store) -> Result<(), String> {
        let globals = lua.globals();

        // Create redis table
        let redis_table = lua
            .create_table()
            .map_err(|e| format!("Failed to create redis table: {}", e))?;

        // Use raw pointer to store for callbacks (safe within script execution scope)
        let store_ptr = store as *const Store;

        // redis.call() - execute command, raise error on failure
        let call_fn = lua
            .create_function(move |lua, args: Variadic<Value>| {
                // SAFETY: This is safe because the Lua state and callbacks are only used
                // within the scope of execute(), which has a valid &Store reference
                let store = unsafe { &*store_ptr };
                execute_redis_command(lua, store, args, false)
            })
            .map_err(|e| format!("Failed to create redis.call: {}", e))?;

        redis_table
            .set("call", call_fn)
            .map_err(|e| format!("Failed to set redis.call: {}", e))?;

        // redis.pcall() - execute command, return error as table on failure
        let pcall_fn = lua
            .create_function(move |lua, args: Variadic<Value>| {
                let store = unsafe { &*store_ptr };
                execute_redis_command(lua, store, args, true)
            })
            .map_err(|e| format!("Failed to create redis.pcall: {}", e))?;

        redis_table
            .set("pcall", pcall_fn)
            .map_err(|e| format!("Failed to set redis.pcall: {}", e))?;

        // redis.error_reply() helper
        let error_reply_fn = lua
            .create_function(|lua, msg: String| {
                let table = lua.create_table()?;
                table.set("err", msg)?;
                Ok(Value::Table(table))
            })
            .map_err(|e| format!("Failed to create redis.error_reply: {}", e))?;

        redis_table
            .set("error_reply", error_reply_fn)
            .map_err(|e| format!("Failed to set redis.error_reply: {}", e))?;

        // redis.status_reply() helper
        let status_reply_fn = lua
            .create_function(|lua, msg: String| {
                let table = lua.create_table()?;
                table.set("ok", msg)?;
                Ok(Value::Table(table))
            })
            .map_err(|e| format!("Failed to create redis.status_reply: {}", e))?;

        redis_table
            .set("status_reply", status_reply_fn)
            .map_err(|e| format!("Failed to set redis.status_reply: {}", e))?;

        // redis.log(level, message) - proper logging with levels
        // LOG_DEBUG=0, LOG_VERBOSE=1, LOG_NOTICE=2, LOG_WARNING=3
        let log_fn = lua
            .create_function(|_, args: Variadic<Value>| {
                if args.len() >= 2 {
                    let level = match &args[0] {
                        Value::Integer(i) => match *i {
                            0 => "DEBUG",
                            1 => "VERBOSE",
                            2 => "NOTICE",
                            3 => "WARNING",
                            _ => "LOG",
                        },
                        _ => "LOG",
                    };
                    let message = match &args[1] {
                        Value::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                        Value::Integer(i) => i.to_string(),
                        Value::Number(n) => n.to_string(),
                        _ => String::new(),
                    };
                    eprintln!("[REDIS:{}] {}", level, message);
                }
                Ok(())
            })
            .map_err(|e| format!("Failed to create redis.log: {}", e))?;

        redis_table
            .set("log", log_fn)
            .map_err(|e| format!("Failed to set redis.log: {}", e))?;

        // Log level constants
        let log_constants = [
            ("LOG_DEBUG", 0),
            ("LOG_VERBOSE", 1),
            ("LOG_NOTICE", 2),
            ("LOG_WARNING", 3),
        ];
        for (name, value) in log_constants {
            redis_table
                .set(name, value)
                .map_err(|e| format!("Failed to set redis.{}: {}", name, e))?;
        }

        // Set redis global
        globals
            .set("redis", redis_table)
            .map_err(|e| format!("Failed to set redis global: {}", e))?;

        // Add locale-aware string comparison to the string library
        // This provides string.locale_compare(a, b) for locale-aware sorting
        let string_table: mlua::Table = globals
            .get("string")
            .map_err(|e| format!("Failed to get string table: {}", e))?;

        let locale_compare_fn = lua
            .create_function(|_, (a, b): (String, String)| {
                // Use Unicode-aware comparison (case-insensitive first, then case-sensitive)
                // This provides reasonable locale-like behavior without libc dependency
                let a_lower = a.to_lowercase();
                let b_lower = b.to_lowercase();
                match a_lower.cmp(&b_lower) {
                    std::cmp::Ordering::Equal => Ok(a.cmp(&b) as i32),
                    ord => Ok(ord as i32),
                }
            })
            .map_err(|e| format!("Failed to create string.locale_compare: {}", e))?;

        string_table
            .set("locale_compare", locale_compare_fn)
            .map_err(|e| format!("Failed to set string.locale_compare: {}", e))?;

        Ok(())
    }

    /// Set up KEYS and ARGV tables
    fn setup_keys_argv(&self, lua: &Lua, keys: &[Bytes], args: &[Bytes]) -> Result<(), String> {
        let globals = lua.globals();

        // Create KEYS table (1-indexed)
        let keys_table = lua
            .create_table()
            .map_err(|e| format!("Failed to create KEYS table: {}", e))?;

        for (i, key) in keys.iter().enumerate() {
            let key_str = String::from_utf8_lossy(key).to_string();
            keys_table
                .set(i + 1, key_str)
                .map_err(|e| format!("Failed to set KEYS[{}]: {}", i + 1, e))?;
        }

        globals
            .set("KEYS", keys_table)
            .map_err(|e| format!("Failed to set KEYS: {}", e))?;

        // Create ARGV table (1-indexed)
        let argv_table = lua
            .create_table()
            .map_err(|e| format!("Failed to create ARGV table: {}", e))?;

        for (i, arg) in args.iter().enumerate() {
            let arg_str = String::from_utf8_lossy(arg).to_string();
            argv_table
                .set(i + 1, arg_str)
                .map_err(|e| format!("Failed to set ARGV[{}]: {}", i + 1, e))?;
        }

        globals
            .set("ARGV", argv_table)
            .map_err(|e| format!("Failed to set ARGV: {}", e))?;

        Ok(())
    }

    // ==================== FUNCTION Commands ====================

    /// Load a function library
    pub fn function_load(&self, code: String, replace: bool) -> Result<String, String> {
        // Parse library name from Shebang-style header
        // Expected format: #!lua name=mylib
        let name = parse_library_name(&code)?;

        if !replace && self.libraries.contains_key(&name) {
            return Err(format!("ERR Library '{}' already exists", name));
        }

        // Parse function names from the code
        let functions = parse_function_names(&code);

        self.libraries.insert(
            name.clone(),
            LuaLibrary {
                name: name.clone(),
                code,
                functions,
            },
        );

        Ok(name)
    }

    /// Delete a function library
    pub fn function_delete(&self, name: &str) -> Result<(), String> {
        self.libraries
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| format!("ERR Library '{}' not found", name))
    }

    /// List all function libraries
    pub fn function_list(&self, pattern: Option<&str>, with_code: bool) -> Vec<LibraryInfo> {
        self.libraries
            .iter()
            .filter(|entry| {
                pattern.is_none_or(|p| entry.key().contains(p) || glob_match(p, entry.key()))
            })
            .map(|entry| LibraryInfo {
                name: entry.name.clone(),
                functions: entry.functions.clone(),
                code: if with_code {
                    Some(entry.code.clone())
                } else {
                    None
                },
            })
            .collect()
    }

    /// Flush all function libraries
    pub fn function_flush(&self) {
        self.libraries.clear();
    }

    /// Execute a function by name
    pub fn fcall(
        &self,
        store: &Store,
        function_name: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    ) -> Result<RespValue, String> {
        // Find the library containing this function
        let library = self
            .libraries
            .iter()
            .find(|entry| entry.functions.contains(&function_name.to_string()))
            .map(|entry| entry.value().clone())
            .ok_or_else(|| format!("ERR Function '{}' not found", function_name))?;

        // Create Lua state and load the library
        let lua = Lua::new();
        self.setup_redis_api(&lua, store)?;
        self.setup_keys_argv(&lua, &keys, &args)?;

        // Load the library code
        lua.load(&library.code)
            .exec()
            .map_err(|e| format!("ERR Error loading library: {}", e))?;

        // Get and call the function
        let globals = lua.globals();
        let func: Function = globals
            .get(function_name)
            .map_err(|e| format!("ERR Function '{}' not callable: {}", function_name, e))?;

        let result = func
            .call::<Value>(())
            .map_err(|e| format!("ERR Error calling function: {}", e))?;

        lua_value_to_resp(&lua, result)
    }
}

impl Default for LuaEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Library info for FUNCTION LIST
pub struct LibraryInfo {
    pub name: String,
    pub functions: Vec<String>,
    pub code: Option<String>,
}

// ==================== Helper Functions ====================

/// Execute a Redis command from Lua
fn execute_redis_command(
    lua: &Lua,
    store: &Store,
    args: Variadic<Value>,
    protected: bool,
) -> LuaResult<Value> {
    if args.is_empty() {
        return Err(mlua::Error::external(
            "Please specify at least one argument for redis.call()",
        ));
    }

    // Convert Lua values to command arguments
    let mut cmd_args: Vec<Bytes> = Vec::with_capacity(args.len());
    for arg in args.iter() {
        let s = lua_to_string(arg)?;
        cmd_args.push(Bytes::from(s));
    }

    // Get command name
    let cmd_name = cmd_args.remove(0);

    // Execute the command using the dispatcher
    let cmd = crate::protocol::Command {
        name: cmd_name,
        args: cmd_args,
    };

    let result = crate::commands::Dispatcher::execute_basic(store, cmd);

    // Convert result back to Lua
    resp_to_lua_value(lua, &result, protected)
}

/// Convert Lua value to string for command arguments
fn lua_to_string(value: &Value) -> LuaResult<String> {
    match value {
        Value::String(s) => Ok(s.to_str()?.to_string()),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Boolean(b) => Ok(if *b { "1" } else { "0" }.to_string()),
        Value::Nil => Ok("".to_string()),
        _ => Err(mlua::Error::external(
            "Unsupported value type in command arguments",
        )),
    }
}

/// Convert Lua value to RespValue
fn lua_value_to_resp(_lua: &Lua, value: Value) -> Result<RespValue, String> {
    match value {
        Value::Nil => Ok(RespValue::Null),
        Value::Boolean(b) => Ok(RespValue::integer(if b { 1 } else { 0 })),
        Value::Integer(i) => Ok(RespValue::integer(i)),
        Value::Number(n) => {
            // Return number as bulk string like Redis does
            Ok(RespValue::bulk(Bytes::from(n.to_string())))
        }
        Value::String(s) => {
            let s = s.to_str().map_err(|e| format!("Invalid UTF-8: {}", e))?;
            Ok(RespValue::bulk(Bytes::from(s.to_string())))
        }
        Value::Table(table) => {
            // Check for special ok/err fields
            if let Ok(err) = table.get::<String>("err") {
                return Ok(RespValue::error(&err));
            }
            if let Ok(ok) = table.get::<String>("ok") {
                return Ok(RespValue::SimpleString(Bytes::from(ok)));
            }

            // Convert array-like table to array
            let len = table.len().map_err(|e| format!("Table error: {}", e))?;
            let mut arr = Vec::with_capacity(len as usize);

            for i in 1..=len {
                let val: Value = table
                    .get(i)
                    .map_err(|e| format!("Failed to get table[{}]: {}", i, e))?;
                arr.push(lua_value_to_resp(_lua, val)?);
            }

            Ok(RespValue::array(arr))
        }
        _ => Ok(RespValue::Null),
    }
}

/// Convert RespValue to Lua value
fn resp_to_lua_value(lua: &Lua, resp: &RespValue, protected: bool) -> LuaResult<Value> {
    match resp {
        RespValue::Null | RespValue::NullArray => Ok(Value::Nil),
        RespValue::Integer(i) => Ok(Value::Integer(*i)),
        RespValue::SimpleString(s) | RespValue::BulkString(s) => {
            let lua_str = lua.create_string(s.as_ref())?;
            Ok(Value::String(lua_str))
        }
        RespValue::Error(e) => {
            if protected {
                // Return error as table with err field
                let table = lua.create_table()?;
                table.set("err", e.as_ref())?;
                Ok(Value::Table(table))
            } else {
                Err(mlua::Error::external(
                    String::from_utf8_lossy(e.as_ref()).to_string(),
                ))
            }
        }
        RespValue::Array(arr) => {
            let table = lua.create_table()?;
            for (i, val) in arr.iter().enumerate() {
                let lua_val = resp_to_lua_value(lua, val, protected)?;
                table.set(i + 1, lua_val)?;
            }
            Ok(Value::Table(table))
        }
        RespValue::Map(pairs) => {
            // Convert map to Lua table with string keys
            let table = lua.create_table()?;
            for (key, val) in pairs.iter() {
                let lua_key = resp_to_lua_value(lua, key, protected)?;
                let lua_val = resp_to_lua_value(lua, val, protected)?;
                table.set(lua_key, lua_val)?;
            }
            Ok(Value::Table(table))
        }
    }
}

/// Parse library name from function code (#!lua name=mylib)
fn parse_library_name(code: &str) -> Result<String, String> {
    for line in code.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#!lua")
            && let Some(name_part) = trimmed.split("name=").nth(1)
        {
            let name = name_part.split_whitespace().next().unwrap_or("");
            if !name.is_empty() {
                return Ok(name.to_string());
            }
        }
    }
    Err("ERR Missing library name in function code".to_string())
}

/// Parse function names from Lua code
fn parse_function_names(code: &str) -> Vec<String> {
    let mut functions = Vec::new();
    for line in code.lines() {
        let trimmed = line.trim();
        // Match: function myfunction(
        if trimmed.starts_with("function ")
            && let Some(rest) = trimmed.strip_prefix("function ")
            && let Some(name) = rest.split('(').next()
        {
            let name = name.trim();
            if !name.is_empty() && !name.contains(':') {
                functions.push(name.to_string());
            }
        }
    }
    functions
}

/// Simple glob matching for library name patterns
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(rest) = pattern.strip_prefix('*') {
        if let Some(inner) = rest.strip_suffix('*') {
            return text.contains(inner);
        }
        return text.ends_with(rest);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return text.starts_with(prefix);
    }
    pattern == text
}
