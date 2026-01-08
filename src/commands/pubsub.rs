//! Pub/Sub command handlers
//!
//! Commands: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE,
//! PUBLISH, PUBSUB (CHANNELS, NUMSUB, NUMPAT)

use bytes::Bytes;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::pubsub::PubSub;

/// Execute PUBSUB subcommands (CHANNELS, NUMSUB, NUMPAT)
pub fn execute(pubsub: &Arc<PubSub>, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"PUBSUB" => cmd_pubsub(pubsub, args),
        b"PUBLISH" => cmd_publish(pubsub, args),
        b"SPUBLISH" => cmd_spublish(pubsub, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// Execute subscription commands (returns messages to send)
/// These need special handling as they change connection mode
pub fn execute_subscribe(
    pubsub: &Arc<PubSub>,
    sub_id: u64,
    cmd: &[u8],
    args: &[Bytes],
) -> Result<Vec<RespValue>> {
    match cmd {
        b"SUBSCRIBE" => cmd_subscribe(pubsub, sub_id, args),
        b"UNSUBSCRIBE" => cmd_unsubscribe(pubsub, sub_id, args),
        b"PSUBSCRIBE" => cmd_psubscribe(pubsub, sub_id, args),
        b"PUNSUBSCRIBE" => cmd_punsubscribe(pubsub, sub_id, args),
        // Sharded pub/sub (cluster-aware channels, scoped to hash slot)
        b"SSUBSCRIBE" => cmd_ssubscribe(pubsub, sub_id, args),
        b"SUNSUBSCRIBE" => cmd_sunsubscribe(pubsub, sub_id, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// PUBLISH channel message
fn cmd_publish(pubsub: &Arc<PubSub>, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("PUBLISH"));
    }

    let channel = &args[0];
    let message = args[1].clone();
    let count = pubsub.publish(channel, message);
    Ok(RespValue::integer(count as i64))
}

/// SPUBLISH shardchannel message - Sharded publish (cluster-aware)
/// Uses dedicated spublish method to deliver smessage type
fn cmd_spublish(pubsub: &Arc<PubSub>, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("SPUBLISH"));
    }

    let channel = &args[0];
    let message = args[1].clone();
    // Uses spublish for proper smessage delivery
    let count = pubsub.spublish(channel, message);
    Ok(RespValue::integer(count as i64))
}

/// PUBSUB subcommand [arguments]
fn cmd_pubsub(pubsub: &Arc<PubSub>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("PUBSUB"));
    }

    let subcmd = &args[0];
    let subcmd_upper: Vec<u8> = subcmd.iter().map(|b| b.to_ascii_uppercase()).collect();

    match subcmd_upper.as_slice() {
        b"CHANNELS" | b"SHARDCHANNELS" => {
            let pattern = args.get(1).map(|b| b.as_ref());
            let channels = pubsub.channels(pattern);
            Ok(RespValue::array(
                channels.into_iter().map(RespValue::bulk).collect(),
            ))
        }
        b"NUMSUB" | b"SHARDNUMSUB" => {
            let channels = &args[1..];
            let results = pubsub.numsub(channels);
            Ok(RespValue::array(
                results
                    .into_iter()
                    .flat_map(|(ch, count)| {
                        vec![RespValue::bulk(ch), RespValue::integer(count as i64)]
                    })
                    .collect(),
            ))
        }
        b"NUMPAT" => Ok(RespValue::integer(pubsub.numpat() as i64)),
        _ => Err(Error::Syntax),
    }
}

/// SUBSCRIBE channel [channel ...]
fn cmd_subscribe(pubsub: &Arc<PubSub>, sub_id: u64, args: &[Bytes]) -> Result<Vec<RespValue>> {
    if args.is_empty() {
        return Err(Error::WrongArity("SUBSCRIBE"));
    }

    let counts = pubsub.subscribe(sub_id, args);
    let responses: Vec<RespValue> = args
        .iter()
        .zip(counts)
        .map(|(channel, count)| {
            RespValue::array(vec![
                RespValue::bulk_string("subscribe"),
                RespValue::bulk(channel.clone()),
                RespValue::integer(count as i64),
            ])
        })
        .collect();

    Ok(responses)
}

/// UNSUBSCRIBE [channel ...]
fn cmd_unsubscribe(pubsub: &Arc<PubSub>, sub_id: u64, args: &[Bytes]) -> Result<Vec<RespValue>> {
    let results = pubsub.unsubscribe(sub_id, args);

    if results.is_empty() {
        // No subscriptions to unsubscribe
        return Ok(vec![RespValue::array(vec![
            RespValue::bulk_string("unsubscribe"),
            RespValue::null(),
            RespValue::integer(0),
        ])]);
    }

    let responses: Vec<RespValue> = results
        .into_iter()
        .map(|(channel, count)| {
            RespValue::array(vec![
                RespValue::bulk_string("unsubscribe"),
                RespValue::bulk(channel),
                RespValue::integer(count as i64),
            ])
        })
        .collect();

    Ok(responses)
}

/// PSUBSCRIBE pattern [pattern ...]
fn cmd_psubscribe(pubsub: &Arc<PubSub>, sub_id: u64, args: &[Bytes]) -> Result<Vec<RespValue>> {
    if args.is_empty() {
        return Err(Error::WrongArity("PSUBSCRIBE"));
    }

    let counts = pubsub.psubscribe(sub_id, args);
    let responses: Vec<RespValue> = args
        .iter()
        .zip(counts)
        .map(|(pattern, count)| {
            RespValue::array(vec![
                RespValue::bulk_string("psubscribe"),
                RespValue::bulk(pattern.clone()),
                RespValue::integer(count as i64),
            ])
        })
        .collect();

    Ok(responses)
}

/// PUNSUBSCRIBE [pattern ...]
fn cmd_punsubscribe(pubsub: &Arc<PubSub>, sub_id: u64, args: &[Bytes]) -> Result<Vec<RespValue>> {
    let results = pubsub.punsubscribe(sub_id, args);

    if results.is_empty() {
        return Ok(vec![RespValue::array(vec![
            RespValue::bulk_string("punsubscribe"),
            RespValue::null(),
            RespValue::integer(0),
        ])]);
    }

    let responses: Vec<RespValue> = results
        .into_iter()
        .map(|(pattern, count)| {
            RespValue::array(vec![
                RespValue::bulk_string("punsubscribe"),
                RespValue::bulk(pattern),
                RespValue::integer(count as i64),
            ])
        })
        .collect();

    Ok(responses)
}

/// SSUBSCRIBE shardchannel [shardchannel ...] - Sharded subscribe (cluster-aware)
fn cmd_ssubscribe(pubsub: &Arc<PubSub>, sub_id: u64, args: &[Bytes]) -> Result<Vec<RespValue>> {
    if args.is_empty() {
        return Err(Error::WrongArity("SSUBSCRIBE"));
    }

    // Uses ssubscribe for proper sharded channel tracking
    let counts = pubsub.ssubscribe(sub_id, args);
    let responses: Vec<RespValue> = args
        .iter()
        .zip(counts)
        .map(|(channel, count)| {
            RespValue::array(vec![
                RespValue::bulk_string("ssubscribe"),
                RespValue::bulk(channel.clone()),
                RespValue::integer(count as i64),
            ])
        })
        .collect();

    Ok(responses)
}

/// SUNSUBSCRIBE [shardchannel ...] - Sharded unsubscribe (cluster-aware)
fn cmd_sunsubscribe(pubsub: &Arc<PubSub>, sub_id: u64, args: &[Bytes]) -> Result<Vec<RespValue>> {
    // Uses sunsubscribe for proper sharded channel untracking
    let results = pubsub.sunsubscribe(sub_id, args);

    if results.is_empty() {
        return Ok(vec![RespValue::array(vec![
            RespValue::bulk_string("sunsubscribe"),
            RespValue::null(),
            RespValue::integer(0),
        ])]);
    }

    let responses: Vec<RespValue> = results
        .into_iter()
        .map(|(channel, count)| {
            RespValue::array(vec![
                RespValue::bulk_string("sunsubscribe"),
                RespValue::bulk(channel),
                RespValue::integer(count as i64),
            ])
        })
        .collect();

    Ok(responses)
}

/// Check if a command is a subscription command
#[inline]
pub fn is_subscribe_command(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"SSUBSCRIBE"
            | b"SUNSUBSCRIBE"
    )
}

/// Check if a command is allowed in pubsub mode
#[inline]
pub fn is_allowed_in_pubsub_mode(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"SSUBSCRIBE"
            | b"SUNSUBSCRIBE"
            | b"PING"
            | b"QUIT"
    )
}
