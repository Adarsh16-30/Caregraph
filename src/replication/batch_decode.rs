//! Reading the operations inside a serialized RocksDB `WriteBatch`.
//!
//! The `rocksdb` crate's `WriteBatch::iterate` reports only the default column
//! family (RocksDB's C++ handler refuses the rest), so it cannot see CareGraph's
//! batches, which touch several families at once. The C API's
//! `rocksdb_writebatch_iterate_cf` reports the numeric column-family id with each
//! operation; this module wraps it.
//!
//! Two things need this:
//!
//! * the **column-family id map**. A replicated batch names families by numeric
//!   id, so leader and follower must agree on which id is which family. The map is
//!   recovered by serialising a probe batch that puts one marker key into each
//!   family and reading the ids back, which needs no access to RocksDB internals;
//! * **cache invalidation** on a follower: a batch that touches `CF_NODES` makes
//!   the node-type cache stale.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::os::raw::c_char;

use rocksdb::WriteBatch;

use crate::error::{CareGraphError, Result};
use crate::storage::{cf, RocksKv};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Put,
    Delete,
    Merge,
}

/// One operation of a batch. Values are not kept: no caller needs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
    pub cf: u32,
    pub kind: Kind,
    pub key: Vec<u8>,
}

type Sink<'a> = &'a mut dyn FnMut(u32, Kind, &[u8]);

unsafe extern "C" fn on_put(
    state: *mut c_void,
    cf: u32,
    k: *const c_char,
    klen: usize,
    _v: *const c_char,
    _vlen: usize,
) {
    // SAFETY: `state` is the `&mut Sink` passed to `for_each` below and is
    // live for the whole synchronous iterate call; `k` points to `klen` bytes
    // RocksDB keeps valid for the duration of this callback.
    let sink = unsafe { &mut *(state as *mut Sink<'_>) };
    let key = unsafe { std::slice::from_raw_parts(k as *const u8, klen) };
    sink(cf, Kind::Put, key);
}

unsafe extern "C" fn on_delete(state: *mut c_void, cf: u32, k: *const c_char, klen: usize) {
    // SAFETY: as `on_put`.
    let sink = unsafe { &mut *(state as *mut Sink<'_>) };
    let key = unsafe { std::slice::from_raw_parts(k as *const u8, klen) };
    sink(cf, Kind::Delete, key);
}

unsafe extern "C" fn on_merge(
    state: *mut c_void,
    cf: u32,
    k: *const c_char,
    klen: usize,
    _v: *const c_char,
    _vlen: usize,
) {
    // SAFETY: as `on_put`.
    let sink = unsafe { &mut *(state as *mut Sink<'_>) };
    let key = unsafe { std::slice::from_raw_parts(k as *const u8, klen) };
    sink(cf, Kind::Merge, key);
}

/// Calls `visit(cf_id, kind, key)` for every operation in `data`, a batch as
/// produced by `WriteBatch::data()`.
pub fn for_each(data: &[u8], mut visit: impl FnMut(u32, Kind, &[u8])) {
    let mut sink: Sink<'_> = &mut visit;
    // SAFETY: `data` is a valid slice for the call. The batch created from it
    // is destroyed before returning, and `sink` outlives the iterate call that
    // is the only user of the `state` pointer.
    unsafe {
        let batch = librocksdb_sys::rocksdb_writebatch_create_from(
            data.as_ptr() as *const c_char,
            data.len(),
        );
        librocksdb_sys::rocksdb_writebatch_iterate_cf(
            batch,
            &mut sink as *mut Sink<'_> as *mut c_void,
            Some(on_put),
            Some(on_delete),
            Some(on_merge),
        );
        librocksdb_sys::rocksdb_writebatch_destroy(batch);
    }
}

pub fn decode(data: &[u8]) -> Vec<Op> {
    let mut ops = Vec::new();
    for_each(data, |cf, kind, key| {
        ops.push(Op {
            cf,
            kind,
            key: key.to_vec(),
        })
    });
    ops
}

/// The ids of the column families `data` writes to.
pub fn touched_cfs(data: &[u8]) -> BTreeSet<u32> {
    let mut ids = BTreeSet::new();
    for_each(data, |cf, _, _| {
        ids.insert(cf);
    });
    ids
}

/// Column-family name → numeric id, as this database numbers them.
pub fn cf_id_map(store: &RocksKv) -> Result<BTreeMap<String, u32>> {
    let mut probe = WriteBatch::default();
    for name in cf::ALL {
        let handle = store.cf_handle(name)?;
        probe.put_cf(&handle, name.as_bytes(), b"");
    }
    let mut map = BTreeMap::new();
    for op in decode(probe.data()) {
        let name = String::from_utf8(op.key)
            .map_err(|_| CareGraphError::Io(std::io::Error::other("probe key is not UTF-8")))?;
        map.insert(name, op.cf);
    }
    if map.len() != cf::ALL.len() {
        return Err(CareGraphError::Io(std::io::Error::other(
            "could not recover every column family id",
        )));
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_keys_come_back_for_every_family_in_a_mixed_batch() {
        let dir = tempfile::tempdir().unwrap();
        let store = RocksKv::open(dir.path()).unwrap();
        let ids = cf_id_map(&store).unwrap();
        assert_eq!(ids.len(), cf::ALL.len());
        let distinct: BTreeSet<u32> = ids.values().copied().collect();
        assert_eq!(distinct.len(), ids.len(), "each family has its own id");

        let edges = store.cf_handle(cf::CF_EDGES).unwrap();
        let nodes = store.cf_handle(cf::CF_NODES).unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(&edges, b"e1", b"v");
        batch.delete_cf(&nodes, b"n1");
        batch.put_cf(&nodes, b"n2", b"v");
        let ops = decode(batch.data());
        assert_eq!(
            ops,
            vec![
                Op {
                    cf: ids[cf::CF_EDGES],
                    kind: Kind::Put,
                    key: b"e1".to_vec()
                },
                Op {
                    cf: ids[cf::CF_NODES],
                    kind: Kind::Delete,
                    key: b"n1".to_vec()
                },
                Op {
                    cf: ids[cf::CF_NODES],
                    kind: Kind::Put,
                    key: b"n2".to_vec()
                },
            ]
        );
        assert_eq!(
            touched_cfs(batch.data()),
            BTreeSet::from([ids[cf::CF_EDGES], ids[cf::CF_NODES]])
        );
    }

    #[test]
    fn an_empty_batch_has_no_operations() {
        assert!(decode(WriteBatch::default().data()).is_empty());
    }
}
