# Runbook: backup, verify and restore

Commands are `caregraph-admin`. Online commands (`remote ...`) talk to the running
server's admin service and need the `admin` role; offline commands open files and
need the server stopped. Add `--json` to any command for machine-readable output.

Key flags for offline commands (give exactly one source): `--plaintext`,
`--key-hex` / `CAREGRAPH_ENCRYPTION_KEY`, or `--kek-file` with `--wrapped-dek`
(`CAREGRAPH_KEK_FILE`, `CAREGRAPH_WRAPPED_DEK`). Remote flags: `--addr`, `--ca`,
`--cert`, `--key`, `--token` (or `CAREGRAPH_ADMIN_*`).

## Take a backup

The server needs `CAREGRAPH_BACKUP_DIR`. Take backups from the standby once
replication exists; until then, from the only node.

```
caregraph-admin remote create-backup --addr https://caregraph-write:50051 \
    --ca ca.pem --cert admin.pem --key admin-key.pem
caregraph-admin remote list-backups ...
```

Backups are incremental: unchanged files are shared with earlier ones. Alert on
`backup_last_success_timestamp` being older than 26 hours and on
`backup_failures_total` increasing.

Copy the backup directory to storage the server cannot write to, with object lock
or equivalent. **The data key is not in the backup.** Keep `dek.json` (the wrapped
data key) and the key-encryption key in a different place, with its own retention.
A backup without them is unreadable by design.

## Verify (routinely, not only in an emergency)

A flipped byte passes a size check but not this one.

```
caregraph-admin backup verify --dir /backups --kek-file kek --wrapped-dek dek.json
```

It restores into a scratch directory, opens it, reads every key of every column
family and verifies the audit chain, then removes the scratch copy. Run it against
the copy in cold storage, not only the live backup directory.

## Restore

1. Stop the server. Keep the damaged database aside; do not delete it yet.
2. Restore into a **new** directory. The command refuses a non-empty target.

```
caregraph-admin backup restore --dir /backups --id 7 --to /data/db-restored \
    --kek-file kek --wrapped-dek dek.json
caregraph-admin check --db /data/db-restored --kek-file kek --wrapped-dek dek.json
```

3. Point `CAREGRAPH_DB_PATH` at the restored directory and start the server. Check
   `/readyz`, then `caregraph-admin remote status`: the sequence should match what
   `backup list` reported for that backup.
4. Everything committed after the backup is gone. Compare the backup's creation
   time with the last good commit to size the loss. Clients that retried writes
   with a `request_id` will find those requests are no longer recorded as done.

### Errors you may see

| Message | Meaning |
|---|---|
| `this backup directory was created under a different data key` | Wrong `dek.json` / KEK for these backups. Find the key they were made with. |
| `this backup directory is encrypted; a data key is required` | You passed `--plaintext` or no key. |
| `... is not empty; restore into a new directory` | The target has files. Choose another directory. |
| `Corruption: ... expected checksum ... computed checksum` | A backup file is damaged. Try an earlier id; this is why you keep several. |

## Checkpoints

`caregraph-admin remote checkpoint NAME` hard-links a consistent copy of the
database under `<backup dir>/checkpoints/NAME` (same filesystem, almost no extra
space). It is for bootstrapping a replica, not archival: it shares files with the
live database.

## Retention

`caregraph-admin backup purge --dir /backups --keep 14` keeps the newest 14 and
deletes files only the older ones used. Do not purge while a backup is running.
