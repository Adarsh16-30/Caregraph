//! `caregraph-fault-injection-worker` — the child process the Rule 5 fault
//! injection suite (`tests/fault_injection/`) spawns and kills.
//!
//! Deliberately minimal: open a database the parent has already seeded with
//! two bare nodes, spawn the real embedding model, print `MODEL_READY` the
//! instant it's up, then make exactly one `AtomicCommitter::commit` call and
//! print `DONE`. The parent races a kill against the gap between those two
//! lines — see `tests/fault_injection/main.rs` for why that gap, not a
//! fixed-time guess, is what makes the kill land near the actual commit.
//!
//! Not a general-purpose tool. It exists only so the test harness has a
//! separate OS process to terminate — `AtomicCommitter` itself has no
//! process-boundary concept and does not need one.

use std::io::Write;

use anyhow::{bail, Context, Result};
use caregraph::embedding::atomic_commit::AtomicCommitter;
use caregraph::embedding::model_bridge::EmbeddingModel;
use caregraph::embedding::state::GraphMutation;
use caregraph::storage::RocksKv;
use caregraph::temporal::record::EdgeValue;
use caregraph::types::{EdgeType, ModelKind, NodeId, Timestamp};

struct Args {
    db: String,
    model_id: String,
    src: u64,
    dst: u64,
    edge_type: EdgeType,
    ts: u64,
    remove: bool,
}

fn parse_edge_type(s: &str) -> Result<EdgeType> {
    Ok(match s {
        "DiagnosedWith" => EdgeType::DiagnosedWith,
        "PrescribedMedication" => EdgeType::PrescribedMedication,
        "UnderwentProcedure" => EdgeType::UnderwentProcedure,
        "TreatedByProvider" => EdgeType::TreatedByProvider,
        "HasLabResult" => EdgeType::HasLabResult,
        "HasEncounter" => EdgeType::HasEncounter,
        other => bail!("unknown edge type: {other}"),
    })
}

fn parse_args() -> Result<Args> {
    let mut db = None;
    let mut model_id = None;
    let mut src = None;
    let mut dst = None;
    let mut edge_type = EdgeType::DiagnosedWith;
    let mut ts = None;
    let mut remove = false;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut next = || it.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--db" => db = Some(next()?),
            "--model" => model_id = Some(next()?),
            "--src" => src = Some(next()?.parse()?),
            "--dst" => dst = Some(next()?.parse()?),
            "--edge-type" => edge_type = parse_edge_type(&next()?)?,
            "--ts" => ts = Some(next()?.parse()?),
            "--remove" => remove = true,
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Args {
        db: db.context("--db is required")?,
        model_id: model_id.context("--model is required")?,
        src: src.context("--src is required")?,
        dst: dst.context("--dst is required")?,
        edge_type,
        ts: ts.context("--ts is required")?,
        remove,
    })
}

fn emit(line: &str) {
    println!("{line}");
    let _ = std::io::stdout().flush();
}

fn main() -> Result<()> {
    let args = parse_args()?;

    let store = RocksKv::open(&args.db).context("opening database")?;
    let model = EmbeddingModel::spawn(&args.model_id).context("spawning embedding model")?;
    if let Some(pid) = model.worker_pid() {
        emit(&format!("PYTHON_PID {pid}"));
    }
    emit("MODEL_READY");

    let mutation = if args.remove {
        GraphMutation::RemoveEdge {
            src: NodeId(args.src),
            dst: NodeId(args.dst),
            edge_type: args.edge_type,
            ts: Timestamp(args.ts),
        }
    } else {
        GraphMutation::AddEdge {
            src: NodeId(args.src),
            dst: NodeId(args.dst),
            edge_type: args.edge_type,
            ts: Timestamp(args.ts),
        }
    };

    let committer = AtomicCommitter::new(&store).context("building AtomicCommitter")?;
    committer.commit(
        mutation,
        &EdgeValue::new(serde_json::json!({})),
        ModelKind::GraphSAGE,
        &model,
        512,
        1_500,
    )?;

    emit("DONE");
    Ok(())
}
