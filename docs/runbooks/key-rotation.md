# Runbook: rotating keys and tokens

Three different secrets, three different costs.

| Secret | Rotation | Cost |
|---|---|---|
| Key-encryption key (KEK) | re-wrap the stored data key | seconds; the database is not touched |
| Data-encryption key (DEK) | copy the database under a new key | minutes at this scale; needs a maintenance window |
| Bearer tokens | add an overlapping token, retire the old one | none; no restart |

TLS certificates rotate by replacing the mounted files; the server picks them up
without a restart (`tls_cert_expiry_seconds` is exported for alerting).

## KEK rotation (routine, e.g. yearly or on suspected exposure of the KEK)

```
caregraph-admin kek new --out kek.new
caregraph-admin kek rotate --wrapped-dek dek.json --old-kek-file kek --new-kek-file kek.new
```

The wrapped file is replaced atomically and records the new KEK's id. Deliver
`kek.new` as the pod's `CAREGRAPH_KEK_FILE` and restart. The old KEK can no longer
open `dek.json`. Backups made earlier are still readable: the data key did not
change, so they open with the new KEK and the re-wrapped `dek.json`.

## DEK rotation (after suspected compromise of the data key, or on policy)

The data key encrypts every file, so rotating it re-encrypts the database. The old
database is not modified until you retire it.

1. Take a backup and verify it (see `backup-and-restore.md`).
2. Stop the server.
3. Copy under a new data key:

```
caregraph-admin reencrypt --src /data/db --dst /data/db-new \
    --kek-file kek --wrapped-dek dek.json \
    --new-kek-file kek --new-wrapped-dek dek-new.json
```

   The tool writes the new wrapped key first, copies every key of every column
   family, then reads both databases back and refuses to succeed unless key counts
   and the audit chain are identical.
4. Point `CAREGRAPH_DB_PATH` and `CAREGRAPH_WRAPPED_DEK` at the new files and start
   the server. Check `/readyz` and `caregraph-admin remote status`.
5. Keep the old database and its wrapped key until the new one has served traffic
   and a fresh backup of it has verified; then destroy them, including old backups
   if the old key is considered compromised.

The per-pod audit database (`CAREGRAPH_AUDIT_DIR`) is encrypted under the same key
and is not rewritten by this tool. It holds access records only, so start a new
audit directory after rotating and archive the old one. A database copied this way
has new RocksDB sequence numbers, so it cannot continue as a replica of the old
one; re-bootstrap replicas from it.

## Token rotation (no restart)

```
caregraph-admin token new --id ingest-2027-01 --principal spiffe://caregraph/ingest --days 90
```

Prints the token once and a `[[token]]` entry holding only its SHA-256. Add the
entry to the tokens file (a mounted Secret), hand the token to the client, switch
the client over, then remove the old entry. Both are valid in the meantime. The
server re-reads the file within five seconds, and keeps the previous set if the
new file does not parse.
