//! Scripting command handlers
//!
//! Implements EVAL, EVALSHA, SCRIPT, FUNCTION, and FCALL commands

use bytes::Bytes;
use std::sync::{Arc, LazyLock};

use crate::error::{Error, Result};
use crate::lua_engine::LuaEngine;
use crate::protocol::RespValue;
use crate::storage::Store;

/// Global Lua engine instance (thread-safe)
static LUA_ENGINE: LazyLock<Arc<LuaEngine>> = LazyLock::new(|| Arc::new(LuaEngine::new()));

/// Get a reference to the global Lua engine
pub fn lua_engine() -> &'static Arc<LuaEngine> {
    &LUA_ENGINE
}

/// Execute a scripting command
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    let engine = lua_engine();

    match cmd.to_ascii_uppercase().as_slice() {
        b"EVAL" => cmd_eval(store, engine, args, false),
        b"EVAL_RO" => cmd_eval(store, engine, args, true),
        b"EVALSHA" => cmd_evalsha(store, engine, args, false),
        b"EVALSHA_RO" => cmd_evalsha(store, engine, args, true),
        b"SCRIPT" => cmd_script(engine, args),
        b"FUNCTION" => cmd_function(store, engine, args),
        b"FCALL" => cmd_fcall(store, engine, args, false),
        b"FCALL_RO" => cmd_fcall(store, engine, args, true),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// Parse numkeys and split args into keys and argv
fn parse_keys_argv(args: &[Bytes], start_idx: usize) -> Result<(Vec<Bytes>, Vec<Bytes>)> {
    if args.len() <= start_idx {
        return Err(Error::WrongArity("EVAL"));
    }

    let numkeys: usize = std::str::from_utf8(&args[start_idx])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    let key_start = start_idx + 1;
    let arg_start = key_start + numkeys;

    if args.len() < arg_start {
        return Err(Error::Syntax);
    }

    let keys = args[key_start..arg_start].to_vec();
    let argv = args[arg_start..].to_vec();

    Ok((keys, argv))
}

/// EVAL script numkeys [key [key ...]] [arg [arg ...]]
fn cmd_eval(
    store: &Store,
    lua_engine: &Arc<LuaEngine>,
    args: &[Bytes],
    _read_only: bool,
) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("EVAL"));
    }

    let script = std::str::from_utf8(&args[0])
        .map_err(|_| Error::Syntax)?
        .to_string();

    let (keys, argv) = parse_keys_argv(args, 1)?;

    // Cache the script
    lua_engine.script_load(script.clone());

    // Execute
    match lua_engine.execute(store, &script, keys, argv) {
        Ok(resp) => Ok(resp),
        Err(e) => Ok(RespValue::error(&e)),
    }
}

/// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
fn cmd_evalsha(
    store: &Store,
    lua_engine: &Arc<LuaEngine>,
    args: &[Bytes],
    _read_only: bool,
) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("EVALSHA"));
    }

    let sha1 = std::str::from_utf8(&args[0])
        .map_err(|_| Error::Syntax)?
        .to_lowercase();

    let (keys, argv) = parse_keys_argv(args, 1)?;

    // Execute by SHA1
    match lua_engine.execute_sha(store, &sha1, keys, argv) {
        Ok(resp) => Ok(resp),
        Err(e) => Ok(RespValue::error(&e)),
    }
}

/// SCRIPT LOAD|EXISTS|FLUSH|KILL|DEBUG
fn cmd_script(lua_engine: &Arc<LuaEngine>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SCRIPT"));
    }

    let subcmd = &args[0];

    if subcmd.eq_ignore_ascii_case(b"LOAD") {
        // SCRIPT LOAD script
        if args.len() < 2 {
            return Err(Error::WrongArity("SCRIPT LOAD"));
        }
        let script = std::str::from_utf8(&args[1])
            .map_err(|_| Error::Syntax)?
            .to_string();

        let sha1 = lua_engine.script_load(script);
        Ok(RespValue::bulk(Bytes::from(sha1)))
    } else if subcmd.eq_ignore_ascii_case(b"EXISTS") {
        // SCRIPT EXISTS sha1 [sha1 ...]
        if args.len() < 2 {
            return Err(Error::WrongArity("SCRIPT EXISTS"));
        }
        let sha1s: Vec<&str> = args[1..]
            .iter()
            .filter_map(|b| std::str::from_utf8(b).ok())
            .collect();

        let exists = lua_engine.script_exists(&sha1s);
        let result: Vec<RespValue> = exists
            .into_iter()
            .map(|e| RespValue::integer(if e { 1 } else { 0 }))
            .collect();

        Ok(RespValue::array(result))
    } else if subcmd.eq_ignore_ascii_case(b"FLUSH") {
        // SCRIPT FLUSH [ASYNC|SYNC]
        lua_engine.script_flush();
        Ok(RespValue::ok())
    } else if subcmd.eq_ignore_ascii_case(b"KILL") {
        // SCRIPT KILL - Request to kill running script
        lua_engine.request_kill();
        Ok(RespValue::ok())
    } else if subcmd.eq_ignore_ascii_case(b"DEBUG") {
        // SCRIPT DEBUG YES|SYNC|NO - No-op
        Ok(RespValue::ok())
    } else {
        Err(Error::Syntax)
    }
}

/// FUNCTION LOAD|DELETE|LIST|FLUSH|KILL|DUMP|RESTORE|STATS
fn cmd_function(_store: &Store, lua_engine: &Arc<LuaEngine>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FUNCTION"));
    }

    let subcmd = &args[0];

    if subcmd.eq_ignore_ascii_case(b"LOAD") {
        // FUNCTION LOAD [REPLACE] function-code
        if args.len() < 2 {
            return Err(Error::WrongArity("FUNCTION LOAD"));
        }

        let mut replace = false;
        let code_idx;

        if args.len() >= 3 && args[1].eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            code_idx = 2;
        } else {
            code_idx = 1;
        }

        if args.len() <= code_idx {
            return Err(Error::WrongArity("FUNCTION LOAD"));
        }

        let code = std::str::from_utf8(&args[code_idx])
            .map_err(|_| Error::Syntax)?
            .to_string();

        match lua_engine.function_load(code, replace) {
            Ok(name) => Ok(RespValue::bulk(Bytes::from(name))),
            Err(e) => Ok(RespValue::error(&e)),
        }
    } else if subcmd.eq_ignore_ascii_case(b"DELETE") {
        // FUNCTION DELETE library-name
        if args.len() < 2 {
            return Err(Error::WrongArity("FUNCTION DELETE"));
        }

        let name = std::str::from_utf8(&args[1]).map_err(|_| Error::Syntax)?;

        match lua_engine.function_delete(name) {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(&e)),
        }
    } else if subcmd.eq_ignore_ascii_case(b"LIST") {
        // FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
        let mut pattern = None;
        let mut with_code = false;

        let mut i = 1;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"LIBRARYNAME") {
                i += 1;
                if i < args.len() {
                    pattern = std::str::from_utf8(&args[i]).ok();
                }
            } else if args[i].eq_ignore_ascii_case(b"WITHCODE") {
                with_code = true;
            }
            i += 1;
        }

        let libraries = lua_engine.function_list(pattern, with_code);
        let result: Vec<RespValue> = libraries
            .into_iter()
            .map(|lib| {
                let mut fields = vec![
                    RespValue::bulk(Bytes::from_static(b"library_name")),
                    RespValue::bulk(Bytes::from(lib.name)),
                    RespValue::bulk(Bytes::from_static(b"engine")),
                    RespValue::bulk(Bytes::from_static(b"LUA")),
                    RespValue::bulk(Bytes::from_static(b"functions")),
                ];

                let funcs: Vec<RespValue> = lib
                    .functions
                    .into_iter()
                    .map(|f| {
                        RespValue::array(vec![
                            RespValue::bulk(Bytes::from_static(b"name")),
                            RespValue::bulk(Bytes::from(f)),
                        ])
                    })
                    .collect();
                fields.push(RespValue::array(funcs));

                if let Some(code) = lib.code {
                    fields.push(RespValue::bulk(Bytes::from_static(b"library_code")));
                    fields.push(RespValue::bulk(Bytes::from(code)));
                }

                RespValue::array(fields)
            })
            .collect();

        Ok(RespValue::array(result))
    } else if subcmd.eq_ignore_ascii_case(b"FLUSH") {
        // FUNCTION FLUSH [ASYNC|SYNC]
        lua_engine.function_flush();
        Ok(RespValue::ok())
    } else if subcmd.eq_ignore_ascii_case(b"KILL") {
        // FUNCTION KILL - Kill running function
        lua_engine.request_kill();
        Ok(RespValue::ok())
    } else if subcmd.eq_ignore_ascii_case(b"DUMP") {
        // FUNCTION DUMP - Return serialized libraries (simplified)
        Ok(RespValue::bulk(Bytes::new()))
    } else if subcmd.eq_ignore_ascii_case(b"RESTORE") {
        // FUNCTION RESTORE - Restore from dump
        Ok(RespValue::ok())
    } else if subcmd.eq_ignore_ascii_case(b"STATS") {
        // FUNCTION STATS - Return execution stats
        Ok(RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"running_script")),
            RespValue::integer(0),
            RespValue::bulk(Bytes::from_static(b"engines")),
            RespValue::array(vec![
                RespValue::bulk(Bytes::from_static(b"LUA")),
                RespValue::array(vec![
                    RespValue::bulk(Bytes::from_static(b"libraries_count")),
                    RespValue::integer(lua_engine.library_count() as i64),
                ]),
            ]),
        ]))
    } else {
        Err(Error::Syntax)
    }
}

/// FCALL function numkeys [key [key ...]] [arg [arg ...]]
fn cmd_fcall(
    store: &Store,
    lua_engine: &Arc<LuaEngine>,
    args: &[Bytes],
    _read_only: bool,
) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FCALL"));
    }

    let function_name = std::str::from_utf8(&args[0]).map_err(|_| Error::Syntax)?;

    let (keys, argv) = parse_keys_argv(args, 1)?;

    match lua_engine.fcall(store, function_name, keys, argv) {
        Ok(resp) => Ok(resp),
        Err(e) => Ok(RespValue::error(&e)),
    }
}
