# Security model

What CareGraph protects, how, and what an operator has to provide. Every
control below is covered by an automated test; the test names are given so a
reviewer can read the evidence.

## Summary

| Concern | Control | Evidence |
|---|---|---|
| Transport | Mutual TLS 1.3, certificates reloaded without restart, optional CRL | `security_test::a_renewed_server_certificate_is_served_without_a_restart` |
| Who is calling | Principal from the client certificate (SPIFFE URI, else DNS SAN) | `security_test::a_certificate_with_no_usable_identity_is_unauthenticated` |
| What they may do | Role policy file, hot-reloaded; default deny | `security_test::each_role_can_call_exactly_what_it_is_allowed_to`, `rbac::tests` |
| Second factor | Hashed bearer tokens bound to the certificate's principal | `security_test::a_token_must_be_valid_unexpired_and_belong_to_the_certificates_principal` |
| Audit of changes | Hash chain staged into each commit batch | `audit_test`, fault-injection suite |
| Audit of access | Per-pod hash chain, durable before the read is served, fail-closed | `audit_test::reads_are_refused_not_served_unaudited_when_the_log_cannot_keep_up` |
| Data at rest | AES-256-CTR (RustCrypto `aes`), wrapped data key, KEK rotation | `encryption_at_rest`, `keys::tests` |
| Abuse | Input limits, rate limits, message caps, bulkheads | `limits_test` |
| Error leakage | Internal errors are generic plus a log reference | `api::internal` |

`CAREGRAPH_ENV=production` refuses to start without mTLS, an encryption key,
synchronous writes, a non-legacy timestamp policy, an RBAC policy and an audit
directory (`config::tests::production_refuses_each_unsafe_setting`).

## Identity and access

**Transport.** The server terminates TLS 1.3 itself (`security/tls.rs`). Clients
must present a certificate chaining to `CAREGRAPH_TLS_CLIENT_CA` and not listed
in `CAREGRAPH_TLS_CRL` if one is configured. The server's certificate, key, the
client CA and the CRL are watched; when any changes, new connections use the new
material and existing ones are undisturbed. A file that fails to parse leaves the
previous material in force and increments `tls_reload_failures`.
`tls_cert_expiry_seconds` is exported for alerting.

**Identity.** The caller's name is the first `spiffe://` URI in the certificate's
subject-alternative-names, else the first DNS name. The common name is not used.
A valid certificate with no usable name is refused as unauthenticated.

**Roles.** `CAREGRAPH_RBAC_POLICY` is a TOML file:

```toml
[principals]
"spiffe://caregraph/ingest"   = ["writer"]
"spiffe://caregraph/analyst"  = ["reader"]
"spiffe://caregraph/ops"      = ["admin"]
"spiffe://caregraph/follower" = ["replicator"]
```

| Role | May call |
|---|---|
| `reader` | `Traverse`, `Snapshot`, `SimilarCarePathways`, `SimilarityDelta` |
| `writer` | the reads, plus `AddEdge`, `RemoveEdge` |
| `admin` | the admin service only (backup, checkpoint, audit verification, status) |
| `replicator` | write-ahead-log streaming only |

A principal not in the file has no access. A denial returns `PERMISSION_DENIED`
with the fixed message `not permitted` (it does not describe roles). The file is
re-read within five seconds of changing; a syntax error or unknown role keeps the
previous policy and logs the error, so a typo can neither lock everyone out nor
widen access.

**Tokens (optional second factor).** With `CAREGRAPH_TOKENS_FILE`, every request
must also carry `authorization: Bearer <token>`, and the token must belong to the
same principal as the certificate. A token alone never authenticates. The file
holds only SHA-256 hashes:

```toml
[[token]]
id = "ingest-2026-10"
sha256 = "<64 hex chars: printf %s "$TOKEN" | sha256sum>"
principal = "spiffe://caregraph/ingest"
not_after_unix = 1798675200
```

Tokens expire at `not_after_unix`; several can be valid at once, which is how
rotation works. A wrong token, an expired token and another principal's valid
token all produce the same message.

**Development mode** (no TLS configured) authenticates with the single shared
`CAREGRAPH_API_KEY` (compared in constant time) and, without a policy file,
allows every call. It exists for local use; production configuration rejects it.

## Audit

Two hash chains. Each record's SHA-256 covers the previous record's hash, so
editing, removing, inserting or reordering any record breaks every hash after it.

**Mutations** (`CF_AUDIT`, main database). One record per committed mutation,
written in the same atomic batch as the edge, its embeddings and the high-water
mark: a mutation cannot be durable without its record, or the reverse. Records
carry the principal, RPC, request id, edge endpoints, affected-node count,
outcome (`committed` or `committed_stale_embeddings`) and commit sequence. They
replicate with the data. The fault-injection suite checks after every kill that
a committed mutation has exactly one record and a valid chain, and an uncommitted
one has none.

**Reads, denials, admin actions** (`CAREGRAPH_AUDIT_DIR`, one database per pod,
not replicated). Each read is written durably (group-committed and fsynced)
**before** it is served, naming the principal and the subject node. If the log
cannot accept the event the read fails with `UNAVAILABLE`; it is never served
unaudited. Denials are queued without waiting (the request is already refused)
and counted if dropped.

Each record is also emitted as one JSON line on the `audit` log target for a
SIEM, and every 60 seconds an `audit_anchor` line carries `(pod, seq, head hash)`.
Ship those lines to write-once storage: a verifier can then detect removal of the
tail of either chain, which a self-contained chain cannot prove on its own
(`audit_test::removing_the_tail_is_caught_by_the_recorded_head` covers the
mutation chain's in-database head).

**What the log is not.** It records that a principal read a given subject, not
the returned values. Node ids are in the audit store, so it is encrypted with the
same key as the main database and must be access-controlled like it.

## Data at rest

The database and the audit store are encrypted with AES-256 in CTR mode through
RocksDB's own encrypted environment, so SST files, the write-ahead log and the
manifest are all ciphertext (`encryption_at_rest` reads the raw files to prove
it). The block cipher is the RustCrypto `aes` crate (AES-NI where available,
constant-time software otherwise). It replaced a hand-written implementation; a
database written by the old one is committed as a fixture and must still open
(`a_database_written_by_the_previous_cipher_opens_and_reads_under_the_current_one`).

**Keys.** Either `CAREGRAPH_ENCRYPTION_KEY` (the data key itself) or, preferably,
`CAREGRAPH_KEK_FILE` plus `CAREGRAPH_WRAPPED_DEK`: the data key stored wrapped
(AES-256-GCM) beside the database and unwrapped at startup with a key-encryption
key delivered as a mounted secret. Rotating the KEK is an online re-wrap that does
not touch the database (`keys::rewrap`); rotating the data key re-encrypts every
file (see the runbook). Unwrapped keys are held in memory that is zeroed on drop.
A KMS or Vault provider implements the `KeyProvider` trait; only the file
provider is implemented and tested here.

## Abuse resistance

Request bounds, rate limits and bulkheads are described in `api_reference.md`
and the README. In short: `properties_json` at most 64 KiB and 32 deep, client
timestamps at most five minutes ahead (a far-future timestamp under the strict
policy would otherwise lock out every other writer), `top_k` at most 1000,
requests at most 1 MiB, a whole-request deadline, bounded concurrent reads and
writes, a single concurrent `explain`, and per-principal token buckets.

## Errors and logs

Internal failures return `internal error (reference <id>)`; the detail goes to
the server log under that id. Validation errors name the offending field but not
stored data. Logs do not carry node ids, properties or embeddings (the audit
target is the deliberate exception and is treated as sensitive).

## Not covered

- FIPS 140 validated cryptography (not requested; the TLS provider is `ring`).
- External KMS / Vault providers.
- Anchoring the audit heads to object storage: the lines are emitted; the uploader
  is deployment work (see the Helm chart).
- Authentication of the replication stream and promotion (added with replication).
- Repairing stale embeddings: `caregraph-admin repair-status` lists the nodes
  waiting for a fresh embedding; re-embedding them is not implemented.

## Administration

The admin service and `caregraph-admin` are described in `api_reference.md`. The
points that matter for security:

- Admin calls need the `admin` role, are audited durably *before* they run, and
  fail closed.
- Backups are encrypted under the database's data key and a directory refuses a
  different key (or none), so an encrypted database cannot be backed up to
  plaintext by mistake (`backup_restore_test::encrypted_backups_hide_the_data_and_need_the_same_key`).
  The data key is not stored in the backup: keep the wrapped key and the
  key-encryption key where the backup storage cannot reach them.
- A restore never overwrites an existing database.
- Checkpoint names are plain file names chosen by the caller and placed by the
  server, so a caller cannot choose where it writes.
- Key and token tooling (`kek new`, `kek rotate`, `reencrypt`, `token new`) writes
  secrets with owner-only permissions on Unix and never into logs. See
  `docs/runbooks/key-rotation.md`.
- The per-pod audit database is not part of the backup: it records what this pod
  served, and a restored database starts a new access history.
