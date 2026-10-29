# Runbook: TLS certificate rotation and expiry

Applies to `CareGraphCertExpiresSoon` (21 days), `CareGraphCertExpiresVerySoon`
(3 days) and `CareGraphCertReloadFailing`.

The server terminates TLS 1.3 itself and **watches its certificate, key, client CA and
CRL files**; when they change, new connections use the new material and existing
connections are not interrupted (`security_test::a_renewed_server_certificate_is_served_without_a_restart`).
A file that fails to parse leaves the old material in force and increments
`tls_reload_failures`, so a bad rotation cannot take a pod down, but the old
certificate keeps ticking toward expiry.

Which certificates there are:

| Certificate | Mounted from | Used for |
|---|---|---|
| Serving certificate | `tls.secretName` (`tls.crt`, `tls.key`) | the gRPC listener; **also the replicas' client certificate** to the leader |
| Client CA | `tls.secretName` (`ca.crt`) | what clients (including replicas and the backup job) must chain to |
| Admin / backup client certificate | `tls.adminSecretName` | the backup job's identity (needs the `admin` role) |
| Application clients' certificates | their own Secrets | their identity (RBAC maps it to a role) |

The serving certificate needs the Kubernetes Service DNS names in its SANs
(`RELEASE-write`, `RELEASE-write.NS.svc`, ...) because replicas verify the leader by
name, and both `serverAuth` and `clientAuth` extended key usages.

## Routine rotation (before expiry)

With cert-manager or external-secrets the Secret is updated for you; the kubelet then
refreshes the mounted files (within about a minute; mounts must not use `subPath`,
and the chart does not). Check the result:

```
kubectl -n NS port-forward POD 9100:9100 &
curl -s localhost:9100/metrics | grep -E '^tls_(cert_expiry_seconds|reloads|reload_failures)'
```

(or read `tls_cert_expiry_seconds / 86400` on the security dashboard). After the
renewal the value jumps up on every pod, and `tls_reloads` increments.

Doing it by hand:

1. Issue the new certificate from the same CA (same name, same SANs).
2. Update the Secret (`kubectl create secret tls ... --dry-run=client -o yaml | kubectl apply -f -`
   plus `ca.crt`).
3. Wait for `tls_reloads` to increase on **each** pod, writers and readers.
4. Confirm a client connects, and that replication is still connected
   (`replication_connected_followers` on the leader).

## Rotating the client CA

Two certificates chain to two CAs for a while:

1. Put **both** the old and the new CA in `ca.crt` and let every pod reload.
2. Re-issue every client certificate (including the replicas', which is the serving
   certificate, and the backup job's) from the new CA.
3. Once every client has moved, remove the old CA from `ca.crt`.

## Revoking a client certificate

Add it to the CRL file and point `CAREGRAPH_TLS_CRL` at it; the server reloads it.
Removing the principal from the RBAC policy (hot-reloaded) is faster and also works
for a certificate that cannot be revoked.

## When the reload fails

`CareGraphCertReloadFailing`: read the pod log for `tls reload failed`; the reason is
named (no certificate in the file, key does not match, unreadable CA). Fix the Secret
content; the next watch tick (10 s) retries. The old certificate is still serving,
so there is time, but see its expiry.

## If a certificate has already expired

New connections fail the handshake; replicas will not reconnect and clients are
refused. Replace the Secret as above; once the files reload, connections succeed
again. A restart is not required. Replicas that were disconnected resume from the log
if it still reaches back to them, otherwise they report `needs_bootstrap`
(`replica-bootstrap.md`).
