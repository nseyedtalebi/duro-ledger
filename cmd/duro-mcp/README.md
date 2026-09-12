# Duro MCP writer deployment contract

`duro-mcp` exposes only `/mcp` and unauthenticated `GET /health`. It reads its bearer token and PostgreSQL DSN once at startup from separate files; it does not read secrets from environment variables. `/mcp` requires a bearer token, cross-origin protection, and the configured body limit. `/health` always returns `{"ok":true}` and does not open SQLite or PostgreSQL.

## Required flags

The service supplies every setting through flags:

- `--listen`: a dedicated private-network listener address. Do not use a wildcard or public address.
- `--token-file`: separate `duro-mcp`-owned `0600` bearer-token file.
- `--postgres-file`: separate `duro-mcp`-owned `0600` PostgreSQL DSN file.
- `--queue`: dedicated SQLite queue path, not a shared Duro queue.
- `--blob-store`: `postgres` (default) or `filesystem`.
- `--blob-root`: required absolute filesystem root only with `--blob-store filesystem`; the service account needs read/write access to it.
- `--writer-principal` and `--writer-surface`: deployment-owned provenance values.
- `--max-request-bytes`: positive request limit.
- `--memory-scope`: optional nonempty opaque deployment scope. When set, the server exposes exactly `record_hermes_memory_turn`, `redact_hermes_memory_turn`, and `recall_hermes_memory`; clients never supply a scope.

For a filesystem deployment, pass both blob flags with a real absolute root and
grant that root through the service manager's filesystem policy; do not pass an
empty optional `--blob-root` value.

## Scoped memory instance contract

A deployment that enables memory uses a separate `duro-mcp` instance, bearer
token, SQLite queue, and opaque `--memory-scope` from every other deployment.
The deployment must enforce a per-profile authorization boundary; scope prevents
accidental cross-profile recall but is not tenant isolation. The memory
projection has no blobs, embeddings, vector database, or persistent projection
state: it lexically replays at most 10,000 matching canonical events and returns
`truncated: true` at that ceiling.

The DSN must use `sslmode=verify-full`, may include non-secret TLS settings such as `sslrootcert`, and must not carry a password in URL userinfo, URL query parameters, or key-value form. Use a least-privilege PostgreSQL role that can perform only the canonical writer operations required by this service.

Rollback: stop service; preserve queue/canonical events.
