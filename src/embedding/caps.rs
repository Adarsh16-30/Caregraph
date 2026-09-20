//! Self-tuning receptive-field caps (Phase 9, Feature 2) — a discrete
//! hill-climb over a small set of measured `(fanout_cap, max_expanded_nodes)`
//! rungs, aimed at closing the incremental-update latency miss
//! `docs/benchmark_report.md` §2.3-§2.4 discloses (GraphSAGE p95 1548.96ms,
//! GAT p95 1531.04ms, against a 100ms target).
//!
//! # Why a discrete ladder, not a continuous AIMD controller
//!
//! `docs/benchmark_report.md` §7.6 measured `MAX_EXPANDED_NODES`
//! non-monotonically: 1,000 → ~617ms, **1,500 → ~234ms**, 2,000 → ~1,190ms.
//! Smaller is not reliably faster. A continuous controller that always
//! shrinks its bound on a latency miss would hill-climb straight past that
//! 1,500 optimum into a worse setting — it has no way to know the relationship
//! isn't monotone. [`LADDER`] instead only ever compares a small, fixed set of
//! paired settings, moves at most one rung per decision
//! ([`CapController::observe`]), and requires [`MIN_SAMPLES_PER_RUNG`] real
//! samples at the current rung before acting on its p95 — so a single slow or
//! fast outlier never causes a rung change.
//!
//! # "On by default, fully recorded" — what that actually buys and costs
//!
//! The controller is bidirectional: it also loosens back toward rung 0 when
//! latency has real headroom, rather than only ever tightening. That is a
//! stronger claim than a tighten-only ratchet, and it comes with a real
//! failure mode a tighten-only design would not have — oscillation right at
//! the target boundary, tightening then immediately loosening then
//! tightening again. [`LOOSEN_MARGIN`] guards against that with an
//! asymmetric band: the controller tightens as soon as p95 exceeds the
//! target, but only loosens once p95 is comfortably *below* it, not merely
//! under it.
//!
//! Every commit's effective caps and the rung that produced them are recorded
//! in [`crate::embedding::meta::EffectiveCaps`], part of the
//! [`crate::embedding::meta::CommitMeta`] committed atomically alongside the
//! embedding — so a past commit's degree of truncation is always
//! reconstructable, and a live cap change is never invisible the way a hidden
//! global mutable would be. The real cost this recording makes visible,
//! honestly, rather than hiding it behind an improved latency number: a
//! tighter `max_expanded_nodes` means more ring-two truncation, i.e.
//! embeddings that are more stale-by-design — see
//! [`crate::embedding::state::ResolutionTruncation::expansion_capped`], which
//! already exists and already surfaces on `MutationResponse` unchanged by
//! this controller.

use std::time::Duration;

/// One rung of the cap ladder — a paired `(fanout_cap, max_expanded_nodes)`
/// setting. Widest first: `LADDER[0]` is Phase 4/5's own production default,
/// so a caller that never observes a latency sample sees no behavior change
/// from before this controller existed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapRung {
    pub fanout_cap: usize,
    pub max_expanded_nodes: usize,
}

pub const LADDER: [CapRung; 5] = [
    CapRung {
        fanout_cap: 512,
        max_expanded_nodes: 1_500,
    },
    CapRung {
        fanout_cap: 384,
        max_expanded_nodes: 1_000,
    },
    CapRung {
        fanout_cap: 256,
        max_expanded_nodes: 750,
    },
    CapRung {
        fanout_cap: 128,
        max_expanded_nodes: 500,
    },
    CapRung {
        fanout_cap: 64,
        max_expanded_nodes: 250,
    },
];

/// PRD Phase 5's own success criterion: p95 incremental-update latency under
/// this is what the controller hill-climbs toward.
pub const TARGET_P95_MS: f64 = 100.0;

/// The controller only loosens back toward a wider rung once p95 is under
/// this fraction of [`TARGET_P95_MS`] — comfortably below the target, not
/// merely at it — so it does not immediately re-tighten on the very next
/// sample. See the module doc's oscillation discussion.
const LOOSEN_MARGIN: f64 = 0.5;

/// Real samples required at the current rung before the controller will act
/// on its p95. Avoids reacting to a single unrepresentative mutation.
const MIN_SAMPLES_PER_RUNG: usize = 16;

/// Ring-buffer capacity for the nearest-rank p95 estimate — the same window
/// size and the same percentile method (`nearest-rank`) the `caregraph-bench-*`
/// binaries use, so a live p95 and a benchmark p95 mean the same thing.
const WINDOW: usize = 64;

/// What [`CapController::observe`] decided this observation should do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapAdjustment {
    Unchanged,
    MovedTo(usize),
}

/// Fixed-capacity ring buffer of recent latency samples, in milliseconds.
///
/// `pub` only so it can appear as a field type inside the `pub enum
/// CapController::Adaptive` variant (Rust requires a variant field's type to
/// be at least as visible as the variant itself) — nothing outside this
/// module constructs one; there is no public constructor.
pub struct RingBuffer {
    samples: Vec<f64>,
    next: usize,
}

impl RingBuffer {
    fn new() -> Self {
        RingBuffer {
            samples: Vec::with_capacity(WINDOW),
            next: 0,
        }
    }

    fn push(&mut self, ms: f64) {
        if self.samples.len() < WINDOW {
            self.samples.push(ms);
        } else {
            self.samples[self.next] = ms;
            self.next = (self.next + 1) % WINDOW;
        }
    }

    fn len(&self) -> usize {
        self.samples.len()
    }

    /// Nearest-rank p95 — mirrors `src/bin/bench_pit.rs`'s own `percentiles`
    /// helper exactly, so this number is comparable to a benchmark's.
    fn p95(&self) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let rank = ((0.95 * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
        Some(sorted[rank])
    }
}

/// A self-tuning cap controller. `Adaptive` hill-climbs [`LADDER`] toward
/// [`TARGET_P95_MS`]; `Pinned` never adjusts, for callers that need a fixed,
/// reproducible cap pair regardless of measured latency (benchmarks,
/// correctness tests, fault injection) — their determinism is then a property
/// stated in code via [`CapController::pinned`], not an accident of never
/// having called [`CapController::observe`].
pub enum CapController {
    Adaptive { rung: usize, window: RingBuffer },
    Pinned(CapRung),
}

impl CapController {
    /// Start at the ladder's widest (most accurate, slowest) rung.
    pub fn new() -> Self {
        CapController::Adaptive {
            rung: 0,
            window: RingBuffer::new(),
        }
    }

    pub fn pinned(fanout_cap: usize, max_expanded_nodes: usize) -> Self {
        CapController::Pinned(CapRung {
            fanout_cap,
            max_expanded_nodes,
        })
    }

    /// The cap pair currently in force, and — for an adaptive controller —
    /// which ladder rung produced it. `None` for a pinned controller: there
    /// is no ladder position to report, by construction.
    pub fn current(&self) -> (CapRung, Option<usize>) {
        match self {
            CapController::Adaptive { rung, .. } => (LADDER[*rung], Some(*rung)),
            CapController::Pinned(r) => (*r, None),
        }
    }

    /// Feed one real embedding-update duration into the controller. A no-op
    /// for a pinned controller — pinned means pinned.
    pub fn observe(&mut self, duration: Duration) -> CapAdjustment {
        let CapController::Adaptive { rung, window } = self else {
            return CapAdjustment::Unchanged;
        };

        window.push(duration.as_secs_f64() * 1000.0);
        if window.len() < MIN_SAMPLES_PER_RUNG {
            return CapAdjustment::Unchanged;
        }
        let Some(p95) = window.p95() else {
            return CapAdjustment::Unchanged;
        };

        if p95 > TARGET_P95_MS && *rung + 1 < LADDER.len() {
            *rung += 1;
            *window = RingBuffer::new();
            CapAdjustment::MovedTo(*rung)
        } else if p95 <= TARGET_P95_MS * LOOSEN_MARGIN && *rung > 0 {
            *rung -= 1;
            *window = RingBuffer::new();
            CapAdjustment::MovedTo(*rung)
        } else {
            CapAdjustment::Unchanged
        }
    }
}

impl Default for CapController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pinned_controller_never_adjusts() {
        let mut c = CapController::pinned(512, 1_500);
        assert_eq!(
            c.observe(Duration::from_millis(9_999)),
            CapAdjustment::Unchanged
        );
        let (rung, idx) = c.current();
        assert_eq!(rung.fanout_cap, 512);
        assert_eq!(rung.max_expanded_nodes, 1_500);
        assert_eq!(idx, None);
    }

    #[test]
    fn a_single_slow_sample_does_not_move_the_rung() {
        let mut c = CapController::new();
        assert_eq!(
            c.observe(Duration::from_millis(2_000)),
            CapAdjustment::Unchanged
        );
        assert_eq!(c.current().1, Some(0));
    }

    #[test]
    fn sustained_latency_over_target_tightens_by_exactly_one_rung() {
        let mut c = CapController::new();
        let mut last = CapAdjustment::Unchanged;
        for _ in 0..MIN_SAMPLES_PER_RUNG {
            last = c.observe(Duration::from_millis(1_500));
        }
        assert_eq!(last, CapAdjustment::MovedTo(1));
        assert_eq!(c.current().1, Some(1));
    }

    #[test]
    fn tightening_never_skips_past_the_narrowest_rung() {
        let mut c = CapController::new();
        // Feed far more samples than needed to move once per window — the
        // rung must still stop at the ladder's last index, never overflow it.
        for _ in 0..(MIN_SAMPLES_PER_RUNG * (LADDER.len() + 3)) {
            c.observe(Duration::from_millis(5_000));
        }
        assert_eq!(c.current().1, Some(LADDER.len() - 1));
    }

    #[test]
    fn comfortable_headroom_loosens_back_toward_rung_zero() {
        let mut c = CapController::new();
        for _ in 0..MIN_SAMPLES_PER_RUNG {
            c.observe(Duration::from_millis(1_500));
        }
        assert_eq!(c.current().1, Some(1));

        let mut last = CapAdjustment::Unchanged;
        for _ in 0..MIN_SAMPLES_PER_RUNG {
            last = c.observe(Duration::from_millis(10));
        }
        assert_eq!(last, CapAdjustment::MovedTo(0));
    }

    #[test]
    fn latency_just_under_target_does_not_loosen_an_already_tight_rung() {
        // Guards the oscillation case the module doc describes: sitting right
        // at the target should not immediately loosen back up.
        let mut c = CapController::new();
        for _ in 0..MIN_SAMPLES_PER_RUNG {
            c.observe(Duration::from_millis(1_500));
        }
        assert_eq!(c.current().1, Some(1));

        let mut last = CapAdjustment::Unchanged;
        for _ in 0..MIN_SAMPLES_PER_RUNG {
            last = c.observe(Duration::from_millis(TARGET_P95_MS as u64 - 1));
        }
        assert_eq!(last, CapAdjustment::Unchanged);
        assert_eq!(c.current().1, Some(1));
    }
}
