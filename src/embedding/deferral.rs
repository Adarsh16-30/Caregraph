//! Hub deferral policy: which nodes' embedding updates move off the write path.
//!
//! # The problem this exists for
//!
//! One mutation on the real clinical graph can name a "hub" among its endpoints
//! or neighbours: a medication, a provider or a common condition shared by
//! thousands of patients. Everything such a node is adjacent to has a changed
//! embedding, so the resolver used to put up to `fanout_cap` of its neighbours
//! in the affected set and then read *their* neighbourhoods too. That one
//! mutation cost hundreds of milliseconds to over a second
//! (`docs/benchmark_report.md` §2.3, §7.6), against a 100 ms target, and the
//! nodes beyond the cap were left stale with only a truncation flag to say so.
//!
//! # What deferral does
//!
//! A node whose live neighbour count exceeds the **hub degree** is a hub. For a
//! mutation the resolver then:
//!
//! * recomputes, synchronously and exactly, every non-hub node it names as
//!   affected (the endpoints and their small neighbourhoods);
//! * does **not** recompute the hub, nor the hub's neighbours when the hub is an
//!   endpoint. Each of those gets a durable `CF_REPAIR` marker in the *same
//!   atomic batch* as the mutation (the marker mechanism the model-failure path
//!   already used), and the repair worker (`repair_worker.rs`) refreshes them
//!   shortly after, off the request path.
//!
//! The synchronous result is still exact for the nodes it reports: a hub is
//! still read as an input (its neighbour list is fetched, so its layer-1 value
//! is correct); it is only its *own* output, and its neighbours' outputs, that
//! wait. Nothing is silently approximated: every deferred node has a marker, a
//! counter increments, and the commit's provenance records the hub degree in
//! force and how many nodes were deferred.
//!
//! # Setting
//!
//! `CAREGRAPH_DEFER_HUB_DEGREE=N`. Unset or `0` disables deferral, which is the
//! default for tests, benchmarks and any process that has not opted in, so the
//! exactness tests keep comparing the old behaviour. The server sets it from its
//! configuration at start-up.

use std::sync::atomic::{AtomicUsize, Ordering};

pub const ENV: &str = "CAREGRAPH_DEFER_HUB_DEGREE";

/// Sentinel: the environment variable has not been read yet.
const UNREAD: usize = usize::MAX;

static HUB_DEGREE: AtomicUsize = AtomicUsize::new(UNREAD);

/// The process-wide hub degree, or `None` when deferral is off.
pub fn hub_degree() -> Option<usize> {
    let mut value = HUB_DEGREE.load(Ordering::Relaxed);
    if value == UNREAD {
        value = std::env::var(ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        HUB_DEGREE.store(value, Ordering::Relaxed);
    }
    (value > 0).then_some(value)
}

/// Overrides the environment. The server calls this once at start-up with its
/// validated configuration; nothing else should.
pub fn set_hub_degree(degree: Option<usize>) {
    HUB_DEGREE.store(degree.unwrap_or(0), Ordering::Relaxed);
}
