//! Building a replica's database from the leader's, over the network.
//!
//! [`fetch`] pulls a checkpoint with the `FetchCheckpoint` stream into
//! `<dest>.partial`, checks that every file arrived whole and the stream ended with
//! its footer, and only then renames it to `<dest>`. A transfer that dies halfway
//! leaves nothing at `<dest>`, so a pod that crashes mid-bootstrap starts the
//! bootstrap again instead of starting a server on half a database.
//!
//! [`decide_start`] is the rule for what a new pod does when it starts with an empty
//! volume: join the existing leader, become the first leader, or wait. It is a pure
//! function because a wrong answer here creates a second leader.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use tonic::Code;

use crate::replication::follower::{bearer, connect, FollowerOptions};
use crate::replication::proto::checkpoint_chunk::Payload;
use crate::replication::proto::replication_service_client::ReplicationServiceClient;
use crate::replication::proto::FetchCheckpointRequest;

/// What a completed fetch transferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    /// The leader's newest sequence number in the copy; the replica follows from
    /// `sequence + 1`.
    pub sequence: u64,
    pub epoch: u64,
    pub files: u64,
    pub bytes: u64,
}

/// A name the leader may send: a plain file name, never a path.
pub fn valid_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// `dest` is usable when it does not exist or is an empty directory. A database
/// is never overwritten.
fn check_dest(dest: &Path) -> Result<()> {
    match std::fs::read_dir(dest) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                bail!(
                    "{} already holds data; a replica is only built into an empty directory",
                    dest.display()
                );
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("reading {}", dest.display())),
    }
}

fn partial_dir(dest: &Path) -> PathBuf {
    let mut name = dest
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".partial");
    dest.with_file_name(name)
}

/// One attempt: pulls the leader's checkpoint into `dest`.
pub async fn fetch(opts: &FollowerOptions, follower_id: &str, dest: &Path) -> Result<Fetched> {
    check_dest(dest)?;
    let partial = partial_dir(dest);
    let _ = std::fs::remove_dir_all(&partial);
    std::fs::create_dir_all(&partial).with_context(|| format!("creating {}", partial.display()))?;
    match receive(opts, follower_id, &partial).await {
        Ok(fetched) => {
            // An empty `dest` directory is replaced by the finished copy.
            if dest.exists() {
                std::fs::remove_dir(dest)
                    .with_context(|| format!("replacing {}", dest.display()))?;
            }
            std::fs::rename(&partial, dest)
                .with_context(|| format!("moving the copy into {}", dest.display()))?;
            Ok(fetched)
        }
        Err(e) => {
            let _ = std::fs::remove_dir_all(&partial);
            Err(e)
        }
    }
}

// tonic's interceptor signature fixes the (large) error type.
#[allow(clippy::result_large_err)]
async fn receive(opts: &FollowerOptions, follower_id: &str, into: &Path) -> Result<Fetched> {
    let channel = connect(opts).await?;
    let token = bearer(&opts.token);
    let mut client = ReplicationServiceClient::with_interceptor(
        channel,
        move |mut req: tonic::Request<()>| -> std::result::Result<_, tonic::Status> {
            if let Some(t) = &token {
                req.metadata_mut().insert("authorization", t.clone());
            }
            Ok(req)
        },
    )
    .max_decoding_message_size(crate::replication::MAX_BATCH_MESSAGE_BYTES);

    let mut stream = client
        .fetch_checkpoint(FetchCheckpointRequest {
            follower_id: follower_id.to_string(),
        })
        .await
        .map_err(|s| anyhow::Error::new(s).context("requesting the checkpoint"))?
        .into_inner();

    let header = match stream.message().await?.and_then(|m| m.payload) {
        Some(Payload::Header(h)) => h,
        _ => bail!("the leader's stream did not start with a checkpoint header"),
    };
    for f in &header.files {
        if !valid_file_name(&f.name) {
            bail!(
                "the leader named a file {:?}, which is not a plain file name",
                f.name
            );
        }
    }
    if header
        .files
        .iter()
        .map(|f| &f.name)
        .collect::<std::collections::HashSet<_>>()
        .len()
        != header.files.len()
    {
        bail!("the leader's checkpoint lists a file twice");
    }

    let mut next_file = 0usize;
    let mut current: Option<(std::fs::File, u64, u64)> = None; // (handle, written, size)
    let mut bytes = 0u64;
    let footer = loop {
        let message = stream
            .message()
            .await?
            .ok_or_else(|| anyhow!("the stream ended before the checkpoint was complete"))?;
        match message.payload {
            Some(Payload::File(chunk)) => {
                // Move on to the file this chunk belongs to; the previous one must
                // be whole.
                if current.is_none() {
                    // Zero-length files carry no chunk, so create any that precede
                    // this one.
                    while next_file < header.files.len()
                        && header.files[next_file].name != chunk.name
                    {
                        let f = &header.files[next_file];
                        if f.size != 0 {
                            bail!("{} arrived out of order or was skipped", f.name);
                        }
                        std::fs::File::create(into.join(&f.name))?.sync_all()?;
                        next_file += 1;
                    }
                    let Some(f) = header.files.get(next_file) else {
                        bail!(
                            "the leader sent a chunk of {:?}, which the header did not list",
                            chunk.name
                        );
                    };
                    current = Some((std::fs::File::create(into.join(&f.name))?, 0, f.size));
                    next_file += 1;
                }
                let (file, written, size) = current.as_mut().expect("just set");
                let expected_name = &header.files[next_file - 1].name;
                if &chunk.name != expected_name || chunk.offset != *written {
                    bail!(
                        "chunk out of order: expected {expected_name} at {written}, got {} at {}",
                        chunk.name,
                        chunk.offset
                    );
                }
                *written += chunk.data.len() as u64;
                if *written > *size {
                    bail!("{expected_name} is longer than the header said");
                }
                file.write_all(&chunk.data)?;
                bytes += chunk.data.len() as u64;
                if *written == *size {
                    let (file, _, _) = current.take().expect("set");
                    file.sync_all()?;
                }
            }
            Some(Payload::Footer(f)) => break f,
            Some(Payload::Header(_)) | None => bail!("unexpected message in the checkpoint stream"),
        }
    };
    if current.is_some() {
        bail!("the stream ended in the middle of a file");
    }
    // Trailing empty files have no chunks.
    while next_file < header.files.len() {
        let f = &header.files[next_file];
        if f.size != 0 {
            bail!("{} was announced ({} bytes) but never sent", f.name, f.size);
        }
        std::fs::File::create(into.join(&f.name))?.sync_all()?;
        next_file += 1;
    }
    if footer.files != header.files.len() as u64 || footer.bytes != bytes {
        bail!(
            "the footer says {} files / {} bytes, but {} / {} arrived",
            footer.files,
            footer.bytes,
            header.files.len(),
            bytes
        );
    }
    Ok(Fetched {
        sequence: header.sequence,
        epoch: header.epoch,
        files: footer.files,
        bytes,
    })
}

/// [`fetch`], retried while the leader is not reachable or not yet ready, for up to
/// `wait`. Refusals that retrying cannot fix (no permission, bad request) end it.
pub async fn fetch_with_retry(
    opts: &FollowerOptions,
    follower_id: &str,
    dest: &Path,
    wait: Duration,
) -> Result<Fetched> {
    let deadline = Instant::now() + wait;
    let mut pause = Duration::from_millis(500);
    loop {
        let error = match fetch(opts, follower_id, dest).await {
            Ok(done) => return Ok(done),
            Err(e) => e,
        };
        let permanent = error
            .chain()
            .filter_map(|c| c.downcast_ref::<tonic::Status>())
            .any(|s| {
                matches!(
                    s.code(),
                    Code::PermissionDenied | Code::Unauthenticated | Code::InvalidArgument
                )
            })
            || error.to_string().contains("already holds data");
        if permanent || Instant::now() + pause > deadline {
            return Err(error);
        }
        tracing::warn!(error = %format!("{error:#}"), retry_in_ms = pause.as_millis() as u64, "bootstrap attempt failed");
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(Duration::from_secs(5));
    }
}

/// What a pod does when it starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartDecision {
    /// The volume already holds a database; its persisted role decides.
    KeepData,
    /// Another node is the leader: build a replica from it.
    JoinLeader,
    /// Nothing exists yet and this pod is the designated first leader.
    FirstLeader,
    /// Cannot tell whether a leader exists; starting could create a second one.
    Wait(&'static str),
}

/// What a peer answered when asked for its role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRole {
    Leader,
    /// Reachable and not the leader.
    Other,
    Unreachable,
}

/// The rule for a starting pod.
///
/// * A volume with data keeps it: the persisted role (which a failover updates)
///   decides, never the configuration.
/// * An empty volume joins a leader if any peer is one.
/// * With no leader reachable, only the designated first leader, and only when the
///   operator said this is a new cluster (`bootstrap_leader`), starts as one.
///   Everything else waits: a peer that is merely unreachable might still be the
///   leader, and a second leader loses data.
pub fn decide_start(
    volume_has_data: bool,
    peers: &[PeerRole],
    is_designated_first_leader: bool,
    bootstrap_leader: bool,
) -> StartDecision {
    if volume_has_data {
        return StartDecision::KeepData;
    }
    if peers.contains(&PeerRole::Leader) {
        return StartDecision::JoinLeader;
    }
    if is_designated_first_leader && bootstrap_leader {
        // A reachable peer that is not the leader (a follower) means a cluster
        // exists whose leader is somewhere else; do not start a rival.
        if peers.contains(&PeerRole::Other) {
            return StartDecision::Wait("a peer is up but no leader is reachable");
        }
        return StartDecision::FirstLeader;
    }
    if is_designated_first_leader {
        return StartDecision::Wait(
            "no leader is reachable and bootstrapLeader is false: set it only when creating a new cluster",
        );
    }
    StartDecision::Wait("no leader is reachable yet")
}

#[cfg(test)]
mod tests {
    use super::*;
    use PeerRole::*;

    #[test]
    fn file_names_are_plain() {
        for ok in [
            "000012.sst",
            "MANIFEST-000005",
            "CURRENT",
            "OPTIONS-000007",
            "a_b.c",
        ] {
            assert!(valid_file_name(ok), "{ok}");
        }
        for bad in ["", ".", "..", "../x", "a/b", "a\\b", "C:x", "a b", "x\0y"] {
            assert!(!valid_file_name(bad), "{bad:?}");
        }
    }

    #[test]
    fn a_volume_with_data_always_keeps_it() {
        for peers in [vec![], vec![Leader], vec![Unreachable]] {
            assert_eq!(
                decide_start(true, &peers, true, true),
                StartDecision::KeepData
            );
            assert_eq!(
                decide_start(true, &peers, false, false),
                StartDecision::KeepData
            );
        }
    }

    #[test]
    fn an_empty_volume_joins_a_reachable_leader_even_if_it_is_the_designated_first() {
        assert_eq!(
            decide_start(false, &[Leader], true, true),
            StartDecision::JoinLeader
        );
        assert_eq!(
            decide_start(false, &[Leader], false, false),
            StartDecision::JoinLeader
        );
        assert_eq!(
            decide_start(false, &[Other, Leader], true, false),
            StartDecision::JoinLeader
        );
    }

    #[test]
    fn only_the_designated_pod_of_a_new_cluster_starts_as_leader() {
        // The first pod of a fresh install: no peer exists yet.
        assert_eq!(
            decide_start(false, &[Unreachable], true, true),
            StartDecision::FirstLeader
        );
        assert_eq!(
            decide_start(false, &[], true, true),
            StartDecision::FirstLeader
        );
        // Any other pod, or the same pod without the new-cluster flag, never does.
        assert!(matches!(
            decide_start(false, &[Unreachable], false, true),
            StartDecision::Wait(_)
        ));
        assert!(matches!(
            decide_start(false, &[Unreachable], true, false),
            StartDecision::Wait(_)
        ));
    }

    #[test]
    fn a_live_follower_with_no_leader_blocks_a_rival_leader() {
        assert!(matches!(
            decide_start(false, &[Other], true, true),
            StartDecision::Wait(_)
        ));
    }
}
