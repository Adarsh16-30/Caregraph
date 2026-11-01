# Design: object-storage uploads and KMS key wrapping

**Status: proposed.** It closes two gaps `docs/security.md` lists: the audit anchors and the
backups never leave the pod because no uploader ships, and the data key can only be wrapped by
a key file, not by a KMS.

The design is for S3-compatible storage and AWS KMS, but it is built so that none of it needs an
AWS account to develop or test: S3 is spoken to MinIO, KMS to `local-kms`, both run in Docker.
That is a real API test of the code, not a mock of it; what it cannot show is IAM, regional
endpoints and the behaviour of the real service, which is listed under "Not proven".

## Principles

* **The server's default build gains no dependency.** The AWS SDK is large. It is behind the
  cargo feature `aws` (off by default), so the standard binaries, images and `cargo deny` run
  are unchanged. The uploader binary and the KMS provider are built only when asked for.
* **Use the official SDK for credentials and signing.** The credential chain (environment,
  web identity for IRSA, instance role), SigV4 and retries are exactly the code one does not
  write by hand and then has to get right.
* **Uploading is not on the write path.** A slow or down object store must never slow or stop
  a commit. The uploader only reads files the server has already finished writing.
* **Fail loud.** A backup that did not upload, or an anchor that did not, is an alert, not a
  log line.

## 1. The uploader: `caregraph-uploader`

A separate small binary that fits the chart's existing `backup.uploader` sidecar contract
(image, command, args, env). One process, two jobs, each a loop:

**Audit anchors.** The server appends one JSON line per interval to `anchors.jsonl`
(`src/security/anchor.rs`). The uploader tails it and writes each new line as its own
immutable object, `anchors/<pod>/<chain>/<seq>-<head-prefix>.json`, with S3 **Object Lock in
compliance mode** and a retention period. One object per anchor is what makes the lock useful:
a single growing object cannot be locked and appended to. The object's own checksum
(SHA-256) is sent with the upload and verified by the service. It records how far it has got
in a small local state file and resumes after a restart without re-uploading. Uploading is
idempotent: a repeat of the same anchor is the same key and the same bytes, and a
`PutObject` on a locked key with identical content is treated as already done.

**Backups.** The backup manager (`src/storage/backup.rs`) produces numbered backups in a
directory. When one is complete and `verify`'d, the uploader packs it, uploads it with
SHA-256 checksums (multipart above a size threshold), then verifies by `HeadObject` that the
stored size and checksum match. Backups are ciphertext under the data key already; the
uploader never sees the key. Server-side encryption is also requested (`aws:kms` with the
configured key) so the bucket policy can require it. It writes a small manifest object last,
so a half-uploaded backup is never listed as restorable. A matching
`caregraph-uploader fetch` retrieves a backup for a restore.

**Retention** is by lifecycle rules documented for the bucket, not by the uploader deleting
anything. An uploader that can delete is an uploader that can be abused to delete.

**Observability.** Metrics on a small HTTP port: `uploader_last_success_timestamp_seconds{kind}`,
`uploader_pending_objects{kind}`, `uploader_failures_total{kind,reason}`,
`uploader_bytes_total`. Alerts: no successful anchor upload for N intervals; no successful
backup upload for N backup intervals; failures climbing. Runbook entry for each.

**Least privilege** (documented IAM policy, one statement each): `s3:PutObject`,
`s3:PutObjectRetention`, `s3:GetObject` on the anchor prefix; `s3:PutObject`, `s3:GetObject`,
`s3:ListBucket` on the backup prefix; **no** `s3:DeleteObject`, **no** `s3:BypassGovernanceRetention`.
Bucket requirements: versioning on, Object Lock on, default retention set, public access
blocked, TLS-only policy.

## 2. KMS key wrapping

Today `KeyProvider::kek()` returns the raw key-encryption key, which is how a mounted file
works. A KMS never releases a key; it only wraps and unwraps. So the trait grows two
operations, with the current behaviour as the default implementation:

```rust
trait KeyProvider {
    fn kek_id(&self) -> String;
    fn kek(&self) -> Result<SecretKey>;              // file providers (unchanged)
    fn wrap_dek(&self, dek: &SecretKey) -> Result<WrappedDek>   { /* AES-GCM under kek() */ }
    fn unwrap_dek(&self, w: &WrappedDek) -> Result<SecretKey>   { /* AES-GCM under kek() */ }
}
```

`AwsKmsProvider` overrides both with `kms:Encrypt` / `kms:Decrypt` on the configured key,
passing an **encryption context** (`{"caregraph": "dek", "format": "1"}`) that is checked on
the way back, so a wrapped key cannot be replayed for another purpose. The `WrappedDek` file
format gains a `provider` field (`"local-aes-gcm"` or `"aws-kms"`), version 2; version-1 files
are read as before. `kek_id` for KMS is the key's ARN.

* **Unwrapping happens once, at start-up.** The DEK is held in memory, as it is today. A
  KMS outage affects only a *restart*, not a running node; that is a stated, tested property.
* **Rotation.** KMS key rotation is the service's (new key material, same ARN, old versions
  still decrypt). `caregraph-admin keys rewrap` already re-wraps under a different KEK; with
  the new provider it rewraps to a different KMS key. DEK rotation is unchanged.
* **Audit.** The wrap and unwrap calls are CloudTrail events, which is the point of using KMS.
* **Access.** The pod's role needs `kms:Decrypt` (and `kms:Encrypt` on the admin path that
  wraps) on that one key, nothing else; the key policy names the role.

### FIPS

KMS has FIPS 140-validated endpoints, and the SDK can be pointed at them
(`use_fips(true)`); that is exposed as `CAREGRAPH_KMS_FIPS=true`. The server's *own* use of
AES-GCM, SHA-2 and TLS is by Rust crates that are not FIPS-validated modules. Making those
FIPS-validated means building against `aws-lc-rs` in FIPS mode (rustls and the AES-GCM path)
and RocksDB's encryption shim over a validated module. That is a separate, larger change and
**is not part of this design**. What this design gives is the part that matters most for the
data key: it never exists outside a validated module except in this process's memory.

## 3. How it is tested without AWS

| Behaviour | Tested against |
|---|---|
| Anchor upload, one locked object per anchor, resume, idempotent repeat | **MinIO** (supports S3 and Object Lock) in Docker |
| Backup upload, checksum verification, manifest-last, fetch and restore round trip | MinIO; the restored database is opened and the audit chain verified |
| A failed or interrupted upload leaves no restorable partial backup | MinIO, with the connection cut mid-upload |
| KMS wrap/unwrap, encryption-context mismatch rejected, v1 files still read, rewrap | **local-kms** in Docker |
| A running server keeps working with KMS unreachable; a restart does not | the same, with the container stopped |
| Credentials, IAM policies, bucket policy, real Object Lock retention enforcement, FIPS endpoints | **not testable here** (see below) |

CI runs the MinIO and local-kms tests in a job of their own (service containers) with
`--features aws`, so a change that breaks them is caught, and `cargo deny` also runs with
the feature on.

## Not proven by this work

These need a real account and are the first things to check in one: that the IAM policy above
is sufficient and not more; that Object Lock in compliance mode refuses deletion before the
retention date (MinIO implements it, but it is not the service); that regional and FIPS
endpoints resolve; that credentials arrive through IRSA or the instance role; throughput on a
real network. None of them changes the design; all of them can fail a first deployment.
