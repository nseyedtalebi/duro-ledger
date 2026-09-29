# Duro — minimal contract

**Status: draft for review.**

## 1. Scope

Duro provides:

- Append an event to PostgreSQL (`event put`).
- Retrieve an event by its UUIDv7 (`event get`).
- Store an artifact on the filesystem.
- Retrieve an artifact by its digest.

## 2. Events

### Stored fields

| Field | Contract |
|---|---|
| `id` | Database-generated UUIDv7 stored as PostgreSQL `UUID`; primary key. |
| `received_at` | Database-assigned insertion timestamp. |
| `event_type` | Required nonblank text, at most 255 UTF-8 bytes. |
| `actor` | Database-assigned authenticated PostgreSQL login identity. |
| `content` | JSON object; defaults to `{}`. |
| `refs` | Consumer-defined JSON object; defaults to `{}`. |

Each JSON object is at most 1 MiB, measured using the UTF-8 representation of PostgreSQL's `jsonb::text`. PostgreSQL enforces this limit and rejects explicit `null`, arrays, and scalars.

PostgreSQL `jsonb` normalizes formatting and retains the last value of duplicate object keys. Artifacts preserve exact source bytes.

### Append

- Input: `event_type`, optional `content`, optional `refs`.
- One call performs one direct database transaction.
- Success returns the complete stored event **after commit confirmation**.
- Repeating the same input creates another event.
- Validation or confirmed transaction failure returns an error.
- A lost connection with an uncertain commit outcome returns **outcome unknown**. No automatic resubmission.

### Get

- Input: an event ID, as a hyphenated UUIDv7 (case-insensitive hexadecimal, RFC variant).
- Reject malformed IDs, other UUID versions, and other variants before database access.
- Return the complete stored event; the CLI emits one newline-terminated JSON object.
- A missing event returns a distinct not-found error. Database or permission failures return errors, not empty records.
- Retrieval is read-only and requires only reader privileges. CLI failures produce stderr diagnostics and a nonzero exit.

### Identity and reading

Read visible events in ascending `id` order. This is a deterministic ordering of identifiers; concurrent transactions may commit in a different order.

Projectors reread the full ledger. A single query uses its database snapshot; a rebuild spanning multiple queries should use one read-only, repeatable-read transaction. Events outside that snapshot are considered on the next rebuild.

The PostgreSQL deployment must support database-side UUIDv7 generation.

### Database boundary

- PostgreSQL enforces field validation and server-owned values.
- **Writer:** may append, but cannot forge identity/time/actor or update, delete, truncate, or alter canonical tables.
- **Reader:** may select.
- **Administrator/migrator:** separate authority, outside ordinary-writer protections.
- Actor is the login identity (`session_user`).

These restrictions must be verified with restricted credentials.

## 3. Artifacts

### Shared rules

- Filesystem storage under a configured root.
- Artifacts preserve opaque bytes exactly.
- Identity: SHA-256, represented as `sha256:<64 lowercase hexadecimal characters>`.
- Put, get, and verification use bounded working memory per active operation, independent of artifact size.
- Artifact size has no application-imposed cap. Filesystem, storage-capacity, and byte-count limits produce explicit errors rather than overflow or truncation.
- Caller-provided sources must remain unchanged during ingestion.

### Put

**Input:** a source file.

**Receipt:** `digest`, `size_bytes`, and the complete committed observation event.

1. Stream into a private temporary file while hashing and counting bytes.
2. Fail on read/write errors or a mismatch with the source's expected length.
3. Publish only a complete artifact, without overwriting an existing digest path.
4. Complete required file and directory synchronization before appending the observation event described in section 4.
5. Return full success only after the event's commit is confirmed.

If that digest already exists, stream-verify the existing artifact. Corruption returns an error and leaves the existing artifact untouched.

Handled failures remove this operation's temporary file where possible. Crashes may leave identifiable temporary files; they must never be returned as completed artifacts. Failed operations restart from the beginning.

Repeating put reuses verified content-addressed bytes and appends a new observation event.

### Get

**Input:** digest and output destination.

- Reject malformed digests.
- Stream bytes while verifying their digest.
- Missing, corrupt, unreadable, or unwritable output produces an error.
- File output uses staging and is published only after successful verification; existing destinations are not silently overwritten.
- Stream output may deliver bytes before final verification. Success requires full consumption and a successful terminal result; partial output must not be treated as verified.

## 4. Event–artifact boundary

### Observation event

- Event type: `artifact.observed`.
- `content`: `digest`, `size_bytes`, `source_host`, and `source_path`.
- `source_host` names the observing host; `source_path` is the absolute source-file path used for ingestion on that host. Together they record where the ingested bytes were observed.
- The digest identifies the bytes ingested during this invocation. The source location may subsequently change or disappear.
- `refs`: `{}`.
- Database-generated UUIDv7, `received_at`, and authenticated `actor` follow the append contract.
- Each successful `put` appends one observation event, including invocations that reuse existing artifact bytes.

### Failure behavior

- Event append requires successful artifact storage or verification. A failed store or integrity check terminates the operation.
- Filesystem publication and database append are separate commits. A failure between them can leave a stored artifact without an event.
- If bytes are stored but event append fails, retain the artifact and return an error carrying `digest`, `size_bytes`, `artifact_stored=true`, and `event_outcome=not_committed` or `unknown`, as supported by the evidence.
- Lost event-commit confirmation returns `unknown` and retains the artifact. Retrying is an explicit caller action and may create another observation event.
- Full success requires confirmed event commit. If a success response itself is lost, the caller may still have an unknown outcome.

The events provide the artifact observation history.

## 5. Acceptance gate

Required verification:

- Real PostgreSQL append, committed readback, and duplicate-input/new-event behavior.
- Event lookup by ID through the library and CLI: full-record equality, restricted-reader access, invalid IDs, missing records, and database failures.
- Database-generated IDs verified as UUIDv7; caller identity overrides rejected.
- Restricted-role tests for validation, actor binding, and forbidden mutations.
- Concurrent full-read tests consistent with the documented ordering/snapshot semantics.
- Artifact round-trip, verified duplicate put, and concurrent same-digest puts.
- Exact observation-event readback: digest, byte count, source host/path, and authenticated actor; each successful put, including duplicate-content puts, emits a new event.
- Stored-artifact/event-append failure and unknown-commit cases, including accurate partial-outcome reporting and retention of stored artifacts.
- Corruption, disk/write failure, interruption, and partial-output tests.
- Memory measurements across increasing artifact sizes at fixed concurrency.
- CLI and library paths both exercised; skipped integration tests do not count as passes.


---

# Implementation

Everything above this line is the contract (`CONTRACT.md`, reproduced
verbatim). Everything below describes this implementation of it: the CLI, the
Go library, operator setup, and the tests that hold it to the contract.

## Requirements

- **PostgreSQL 18 or newer.** The schema uses server-side `uuidv7()`, added in
  18. Earlier servers cannot generate the contract's ids and are not
  supported; `duro init` fails against them.
- The database must be `UTF8`-encoded (`duro init` refuses anything else,
  because the 1 MiB and 255-byte limits are measured in UTF-8 bytes).
- Go 1.25+ to build. The only dependency is `github.com/jackc/pgx/v5`.
- A filesystem for artifacts that supports hard links within the store root
  (publication uses `link(2)` so it can never clobber an existing digest).
- **Durable server settings for a meaningful commit acknowledgement.** Duro
  reports an event as stored once PostgreSQL acknowledges the commit. That
  acknowledgement is only crash-durable if the server has `fsync = on` (the
  default; `off` risks the whole cluster on any crash) and
  `synchronous_commit = on` (the default). Either one relaxed -- clusterwide,
  per-database, per-role, or in the writer's own DSN
  (`options=-c synchronous_commit=off`) -- weakens what an acknowledged append
  guarantees: a recently acknowledged event can disappear on crash recovery.
  Duro does not read, set, or enforce these settings; the operator owns them.
- **Traversal and read access along the artifact store root.** Making a
  directory entry durable means opening that directory and `fsync`ing it, so
  the process must be able to open the store root *and every ancestor up to
  the filesystem root* for reading. A root under a directory the process
  cannot open (for example a mode `0711` home directory in the path) fails at
  open time rather than reporting durability Duro did not establish.

```
go build ./cmd/duro
```

## Quick start

```sh
# 1. A PostgreSQL 18 server (see compose.yaml for a local one).
export DURO_POSTGRES_PASSWORD=...           # required by compose.yaml
docker compose up -d postgres18

# 2. Apply the schema with an administrator/provisioning DSN.
export ADMIN_DSN="postgres://duro:...@127.0.0.1:5433/duro"
duro init --postgres "$ADMIN_DSN"

# 3. Create the restricted login roles, then let init grant them.
#    duro never creates roles or sets passwords; see "Provisioning" below.
psql "$ADMIN_DSN" -c "CREATE ROLE duro_writer LOGIN PASSWORD 'w...'" \
                  -c "CREATE ROLE duro_reader LOGIN PASSWORD 'r...'"
duro init --postgres "$ADMIN_DSN" --writer duro_writer --reader duro_reader

# 4. Append an event and store an artifact, as the writer.
export DURO_POSTGRES_DSN="postgres://duro_writer:w...@127.0.0.1:5433/duro"
export DURO_CAS_ROOT=/var/lib/duro/artifacts
duro event put --type document.tagged --content '{"tag":"reviewed"}'
duro artifact put --file ./report.pdf
duro artifact get --sha256 sha256:<64 hex> --out ./restored.pdf
```

---

# CLI reference

Exit codes are the same everywhere: **0** on success or for any help output,
**1** for every failure. Success output is a single JSON object on stdout
(one line, newline-terminated) except for `artifact get` without `--out`,
which writes raw artifact bytes. Errors go to stderr: a plain message,
except for `artifact put`'s partial-outcome case, which is a JSON object
(documented below).

Flags are stdlib `flag` flags: `--flag value` and `--flag=value` both work,
`-flag` also works, and flag parsing stops at the first non-flag argument.
Positional arguments are never accepted; any leftover argument is an error.

Two flags fall back to the environment when omitted:

| Flag | Environment fallback |
|---|---|
| `--postgres` | `DURO_POSTGRES_DSN` |
| `--root` | `DURO_CAS_ROOT` |

**Precedence:** a flag given a nonempty value always wins. A flag that is
absent *or* explicitly empty (`--postgres ''`) falls back to the environment
variable, so an empty flag value is not a way to force "no DSN". There is no
config file and no other implicit source. A DSN is whatever `pgx` accepts
(URL or keyword/value form), including `sslmode` and `connect_timeout`.

## `duro`

```
duro init          --postgres DSN [--writer ROLE] [--reader ROLE]
duro event put     --postgres DSN --type EVENT_TYPE [--content JSON] [--refs JSON]
duro event get     --postgres DSN --id UUID
duro artifact put  --postgres DSN --root DIR --file PATH
duro artifact get  --root DIR --sha256 sha256:HEX [--out PATH]
duro help | --help | -h
```

Run with no arguments, or as `duro help`, `duro --help`, `duro -h`, to print
the root usage and exit 0. `duro help <command...>` is shorthand for
`duro <command...> --help`, so `duro help artifact put` prints `artifact
put`'s help. Every help path exits 0 and performs no database or filesystem
I/O. An unknown command or subcommand is an error (exit 1).

## `duro init`

Applies the canonical schema (one table, `public.events`) using an
**administrator/provisioning** connection, and optionally grants restricted
privilege to existing writer/reader login roles.

Schema application is one advisory-locked transaction, so concurrent `init`
runs are safe. The optional role grants are **separate** transactions -- one
per role, each atomic in itself: if `--reader` fails after `--writer`
succeeded, the schema and the writer's grants remain, and rerunning `init` is
safe. A single `init` invocation is therefore not one all-or-nothing unit.
Idempotent: against an already-initialized, contract-compliant database it
changes nothing and succeeds. Against a `public.events` that differs from the
contract in **any** way Duro can see -- columns, types, `NOT NULL`, defaults,
primary key, or `CHECK` constraints -- it refuses and changes nothing. It
never drops, alters, or converts an existing table, and never touches
existing rows. (The comparison builds a throwaway copy of the current schema
as a temporary table inside the same transaction and compares PostgreSQL's
own catalog descriptors, then rolls back, so it cannot drift from
`schema.sql` and cannot be fooled by a table that merely has the right
column names and types.)

Column descriptors cannot express everything that decides whether
`public.events` *means* the canonical ledger, so `init` also refuses a
relation that is not an ordinary table (a view is not the store), is not
permanent (an unlogged table loses every event on crash recovery), carries a
user trigger or rule (either can rewrite or silently drop an append), or has
**row-level security**: `relrowsecurity`, `relforcerowsecurity`, or any
`pg_policy` attached to `public.events`. RLS turns the canonical table into a
per-role filtered projection -- a reader can be shown a subset as if it were
the whole ledger, and a policy's `WITH CHECK` can reject an append the schema
accepts -- so `init` refuses rather than quietly inheriting it. Enabled RLS
with no policy is refused too, since it denies every non-owner row.

| Argument | Required | Default | Notes |
|---|---|---|---|
| `--postgres DSN` | yes | `DURO_POSTGRES_DSN` | Administrator/provisioning DSN. Must be able to `CREATE TABLE` in schema `public` and `GRANT` on it. |
| `--writer ROLE` | no | none | Existing **login** role to grant append-only privilege. |
| `--reader ROLE` | no | none | Existing **login** role to grant `SELECT` and nothing else. |

- **stdout on success:** `{"initialized":true}`
- **stderr, exit 1:** connection failure, non-UTF8 database, missing
  `uuidv7()` (server older than 18), insufficient privilege, an incompatible
  existing `public.events`, or a refused role (see *Provisioning* below).
- **Side effects:** creates `public.events` and revokes `PUBLIC` privileges
  on it; adds only the grants named by `--writer`/`--reader`. Nothing else in
  the database is modified. The `REVOKE ... FROM PUBLIC` is part of the
  schema DDL, so it runs **only when this `init` creates the table**. A rerun
  against an existing compliant table is a no-op that re-revokes nothing: if
  someone has since granted `PUBLIC` privileges on `public.events`, `init`
  will not take them away. Audit and revoke that yourself.

```sh
duro init --postgres "postgres://admin@localhost:5432/duro" \
  --writer duro_writer --reader duro_reader
```

## `duro event`

A group with `put` and `get` subcommands. `duro event`, `duro event --help`,
and `duro event -h` print group help and exit 0. Use
`duro event put --help` / `duro event get --help`, or
`duro help event put` / `duro help event get`, for command-specific help.
An unknown subcommand is an error. No help command opens a database.

## `duro event put`

Appends one event in one direct transaction and prints the committed row.
PostgreSQL assigns `id`, `received_at`, and `actor`; this command cannot
override them (with writer credentials it has no privilege to try).
Despite its name, `put` never upserts or replaces: each successful invocation
appends a new event with a fresh database-generated ID.

| Argument | Required | Default | Notes |
|---|---|---|---|
| `--postgres DSN` | yes | `DURO_POSTGRES_DSN` | A writer-role DSN is enough; no administrator privilege needed. |
| `--type EVENT_TYPE` | yes | -- | Nonblank (whitespace-only is blank, including Unicode spaces such as U+00A0 and U+3000), at most 255 UTF-8 bytes. |
| `--content JSON` | no | `{}` | A JSON object. `--content 'null'`, `--content ''`, arrays, and scalars are rejected. At most 1 MiB measured as UTF-8 `jsonb::text` **after** normalization. |
| `--refs JSON` | no | `{}` | Same rules as `--content`. |

Omitting `--content`/`--refs` stores `{}`. Passing an explicitly empty value
is **not** the same as omitting it: it is invalid input and is rejected.
`jsonb` normalizes whitespace and keeps the last value of duplicate keys, so
input that is over 1 MiB as raw text but small once normalized is accepted,
and compact input that is still over 1 MiB after normalization is rejected.

- **stdout on success:** the complete stored event, after commit:
  ```json
  {"id":"0199...","received_at":"2026-09-28T23:38:51.47155Z","event_type":"document.tagged","actor":"duro_writer","content":{"tag":"reviewed"},"refs":{}}
  ```
  `received_at` is RFC 3339 with nanoseconds, as returned by the server.
- **stderr, exit 1:**
  - Validation failures (blank/oversized `--type`, malformed
    `--content`/`--refs`) are reported before any connection is opened.
  - A confirmed transaction failure (a value PostgreSQL rejects, a
    permission denial) prints `postgres: append not_committed: <cause>`.
  - A lost connection with an uncertain commit outcome prints `postgres:
    append unknown: <cause>`: the event **may or may not** have committed.
- **No automatic retry, ever.** Repeating the command creates another event.
  After an `unknown` outcome, retrying can leave two events for what you
  meant as one append; read the ledger first if that matters.

## `duro event get`

Retrieves one event by its UUIDv7 primary key, without modifying the ledger.

| Argument | Required | Default | Notes |
|---|---|---|---|
| `--postgres DSN` | yes | `DURO_POSTGRES_DSN` | A reader-role DSN is enough. A nonempty flag overrides the environment. |
| `--id UUID` | yes | -- | Hyphenated UUIDv7 with the RFC variant; hexadecimal is case-insensitive. No positional ID argument. |

- **stdout on success:** one newline-terminated JSON object, with the same
  six fields as `event put`: `id`, `received_at`, `event_type`, `actor`,
  `content`, and `refs`. It returns the stored values, including the original
  writer's actor, not the reader's identity. No format flag is needed.
- **stderr, exit 1:** malformed IDs (including other UUID versions/variants)
  are rejected before opening a connection; missing events report
  `postgres: event not found`. Connection, permission, and other database
  failures are errors, not missing records. These failures leave stdout empty.
- **Side effects:** none. The library uses a single parameterized SELECT
  against `public.events`; no initialization or schema changes occur.

```sh
duro event get --postgres "$READER_DSN" \
  --id 01926a3e-1c2d-7000-8abc-0123456789ab
```

Compose a write with a read using `jq` (an optional pipeline tool, not a Duro
dependency). In Bash, `pipefail` preserves failures upstream:

```bash
set -euo pipefail
id=$(duro event put --postgres "$WRITER_DSN" --type document.tagged \
  --content '{"tag":"reviewed"}' | jq -er '.id')
duro event get --postgres "$READER_DSN" --id "$id"
```

Use DSNs for the same database. Do not automatically repeat `event put` if
the pipeline fails: the event may already have committed. A known ID lets
you repeat the read without creating another event.

## `duro artifact`

A group with two subcommands, `put` and `get`. `duro artifact`,
`duro artifact --help`, and `duro artifact -h` print the group's help and
exit 0; an unknown subcommand is an error.

## `duro artifact put`

Streams `--file` into the content-addressed store under `--root`, then
appends one `artifact.observed` event. Full success requires both.

| Argument | Required | Default | Notes |
|---|---|---|---|
| `--postgres DSN` | yes | `DURO_POSTGRES_DSN` | Writer role is enough. |
| `--root DIR` | yes | `DURO_CAS_ROOT` | Store root; created (with `sha256/` and `.tmp/`) if missing. |
| `--file PATH` | yes | -- | A regular file. The contract requires the source to stay **unchanged** for the whole ingestion. Duro detects a length change (grown or truncated between stat and end-of-read) and rejects the put; an in-place edit that preserves the length cannot be detected, and the recorded digest then describes the bytes actually read, not the file's earlier contents. Relative paths are resolved to absolute, and the absolute path is what the event records. The resolved path (and this host's name) must be valid UTF-8: a Unix filename is arbitrary bytes, but JSON is not, and `source_path`/`source_host` are recorded exactly or the put is refused before any byte is read. |

Identity is `sha256:<64 lowercase hex>`. Bytes are deduplicated, observations
are not: putting bytes that already exist stream-verifies the stored copy and
reuses it, and still appends a **new** event. Working memory is bounded (a
fixed 32 KiB copy buffer) regardless of artifact size.

- **stdout on full success:**
  ```json
  {"digest":"sha256:9f86...","size_bytes":13000,"event":{"id":"0199...","received_at":"...","event_type":"artifact.observed","actor":"duro_writer","content":{"digest":"sha256:9f86...","size_bytes":13000,"source_host":"ingest-1","source_path":"/srv/in/report.pdf"},"refs":{}}}
  ```
- **stderr, exit 1, plain message:** the bytes were not stored or not
  verified -- unreadable source, not a regular file, write failure, length
  mismatch, `fsync` failure, or an existing artifact at that digest that
  fails verification (corruption). Nothing is recorded: no event, and the
  existing artifact is left untouched. A failure after the hard link but
  during the final directory `fsync` can leave the bytes on disk with no
  event; content addressing makes a later retry safe either way.
- **stderr, exit 1, JSON object:** the bytes are stored (or verified as an
  existing duplicate) but the event did not commit:
  ```json
  {"digest":"sha256:9f86...","size_bytes":13000,"artifact_stored":true,"event_outcome":"not_committed","error":"postgres: append not_committed: ..."}
  ```
  `event_outcome` is `not_committed` (the server refused, definitely no
  event) or `unknown` (confirmation was lost; there may or may not be an
  event). **The artifact is retained on disk in both cases** -- do not delete
  it. Retrying is safe for the bytes and appends another observation event;
  after `unknown` that can mean two events for one put.
- **Side effects:** creates a private temp file under `ROOT/.tmp`, up to two
  levels of shard directories, and one read-only blob file. No existing file
  under `--root` is ever overwritten. Handled failures remove their own temp
  file; a crash can leave a `ROOT/.tmp/duro-put-*` file, which is never
  served as an artifact and is safe to delete when no put is running.

```sh
duro artifact put --postgres "postgres://duro_writer@localhost:5432/duro" \
  --root /var/lib/duro/artifacts --file ./report.pdf
```

## `duro artifact get`

Retrieves an artifact by digest, verifying bytes as they stream. **No
PostgreSQL connection is used or required.**

| Argument | Required | Default | Notes |
|---|---|---|---|
| `--root DIR` | yes | `DURO_CAS_ROOT` | Store root. |
| `--sha256 IDENT` | yes | -- | `sha256:<64 lowercase hex>`. A bare hex digest, uppercase hex, or any other spelling is rejected without touching the filesystem. |
| `--out PATH` | no | stdout | Destination file. Must not already exist. |

- **`--out` given, success:** `{"digest":"sha256:...","path":"./restored.pdf"}`
  on stdout. The file is staged beside the destination, verified, and only
  then published by hard link; an existing destination is never overwritten
  (and neither is one created by a racing writer in between). **The
  destination is mode `0600`, owned by the invoking user** -- it inherits the
  staging temp file's permissions, unaffected by your `umask`. That is
  deliberate (a partially written artifact is never briefly world-readable),
  but it means a retrieved artifact is not readable by other users until you
  `chmod` it.
- **`--out` omitted:** raw bytes on stdout. A stream has no staging area, so
  partial bytes may already have been written when an error occurs. **Exit 0
  is the only signal that the stream was complete and verified** -- treat any
  nonzero exit as a failed transfer and discard what you received.
- **stderr, exit 1:** malformed digest, artifact not found, unreadable
  stored file, content that does not hash to the requested digest
  (corruption), an existing `--out`, or an unwritable destination.
- **Side effects:** opening the store creates `--root` and its `sha256/` and
  `.tmp/` subdirectories if they are missing **and `fsync`s them plus every
  ancestor** -- a read operation has write and durability side effects,
  because opening a store is the same operation for reads and writes -- so a
  `get` against a wrong or empty `--root` leaves an empty store behind, and
  fails outright if the root's ancestors cannot be opened for reading. A
  malformed `--sha256` is
  rejected first and touches nothing. With `--out`, a temporary
  `duro-get-*` file is staged in the destination's directory and removed on
  failure.

```sh
duro artifact get --root /var/lib/duro/artifacts \
  --sha256 sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08 \
  --out ./report.pdf
```

---

# Library usage

The Go library has four small packages: `pkg/event` (input shape and
client-side validation), `pkg/postgres` (the canonical ledger), `pkg/cas`
(the content-addressed store), and `pkg/artifact` (the two-store put/get
orchestration). The `duro` Python package is a small event writer for ETLs and
other orchestration; artifacts remain CLI/Go-library operations.

## Python event writer

Install this repository as a Python package, then append a JSON-object event
without writing SQL or managing a PostgreSQL transaction:

```python
from duro import AppendError, append_event

try:
    receipt = append_event(
        "rador.etl.completed",
        content={"run_id": "run-1"},
        refs={"source": "rador"},
        # dsn defaults to DURO_POSTGRES_DSN.
    )
except AppendError as error:
    if error.outcome == "unknown":
        # Commit confirmation was lost: the event may exist. Never retry
        # automatically; read the ledger first if duplication matters.
        raise
    raise

print(receipt.id, receipt.actor, receipt.received_at)
```

`content` and `refs` must be JSON objects and default to `{}`. PostgreSQL
assigns `id`, `received_at`, and `actor`; each successful call appends a new
event. `AppendError.outcome` is `not_committed` for a definite failure or
`unknown` when the commit result cannot be confirmed. The client never retries.

```go
// Administrator/provisioning path, once per deployment.
if err := postgres.Initialize(adminDSN); err != nil { return err }
if err := postgres.ProvisionWriter(adminDSN, "duro_writer"); err != nil { return err }
if err := postgres.ProvisionReader(adminDSN, "duro_reader"); err != nil { return err }

// Appending, with a writer DSN.
ledger, err := postgres.Open(writerDSN)
if err != nil { return err }
defer ledger.Close()

stored, err := ledger.Append(event.New{
    EventType: "document.tagged",
    Content:   json.RawMessage(`{"tag":"reviewed"}`), // nil means {}
})
var appendErr *postgres.AppendError
switch {
case err == nil:
    // stored.ID, stored.ReceivedAt, stored.Actor are the committed,
    // database-assigned values.
case errors.As(err, &appendErr):
    if appendErr.Outcome == postgres.OutcomeUnknown {
        // Commit confirmation was lost. The event may exist. Never resubmit
        // automatically; read the ledger or accept a possible duplicate.
    }
default:
    // Client-side validation error: nothing was sent to the database.
}
```

```go
// Artifacts.
store, err := cas.Open("/var/lib/duro/artifacts")
if err != nil { return err }

result, err := artifact.Put(store, ledger, "./report.pdf")
if err != nil {
    var putErr *artifact.PutError
    if errors.As(err, &putErr) {
        // putErr.ArtifactStored is true: the bytes are on disk under
        // putErr.Digest (size putErr.Size) and must be kept. Only the event
        // is in doubt: putErr.EventOutcome is "not_committed" or "unknown".
    }
    // Either way result is the zero value: never read it after an error.
    return err
}
// Only now are result.Digest, result.Size, and result.Event (the committed
// observation) meaningful.

err = artifact.GetToFile(store, result.Digest, "./restored.pdf") // staged, verified, then published
err = artifact.GetToWriter(store, result.Digest, w)              // may emit unverified bytes; a non-nil error means failed
```

`artifact.Put`'s ledger parameter is an interface with a single
`Append(event.New) (postgres.StoredEvent, error)` method, so tests can
substitute a fake ledger to exercise the failure boundary;
`*postgres.Store` is the production implementation.

`cas.Store` also exposes `Write(io.Reader)` for bytes that are not a file,
`Verify(digest, size)`, `Has(digest)`, and `Path(digest)`. `cas.ParseIdentity`
/ `cas.FormatIdentity` convert between `sha256:<hex>` and bare hex.

## Reading the ledger directly

There is no read API in the Go library on purpose: reading is plain SQL, and
a projector should own its own transaction. Read in ascending `id` order.

A single query is already a consistent snapshot. A rebuild that spans several
queries must hold one read-only, repeatable-read transaction:

```sql
BEGIN TRANSACTION READ ONLY ISOLATION LEVEL REPEATABLE READ;
  SELECT id, received_at, event_type, actor, content, refs
    FROM public.events
   ORDER BY id;
  -- further queries in this transaction see exactly the same snapshot
COMMIT;
```

`id` ascends with generation time, which is **not** commit order: a
transaction that inserted earlier can commit later, so an event with a lower
id can appear after a rebuild already passed that point. Events outside the
snapshot are picked up by the next rebuild. Nothing is ever updated or
deleted, so a rebuild never has to handle a changed row.

---

# Provisioning: three distinct authorities

Create the login roles with your own password/authentication policy, then let
`duro init` grant them. Duro does not manage roles, passwords, or
authentication.

**Administrator/migrator** -- outside ordinary-writer protections. Owns
`public.events`, runs `duro init`, and is the only identity that can change
the schema. Do not use it for appends.

```sql
-- as a superuser
CREATE ROLE duro_admin LOGIN PASSWORD '...';
CREATE DATABASE duro OWNER duro_admin;
```

**Writer** -- may append, cannot forge identity/time/actor, cannot update,
delete, truncate, or alter:

```sql
CREATE ROLE duro_writer LOGIN PASSWORD '...';
```
```sh
duro init --postgres "$ADMIN_DSN" --writer duro_writer
```
This grants `USAGE` on schema `public`, **column-level** `INSERT` on
`(event_type, content, refs)` only, and `SELECT` (needed for `INSERT ...
RETURNING`). Because `id`, `received_at`, and `actor` carry no `INSERT`
grant, the writer cannot supply them -- PostgreSQL's defaults (`uuidv7()`,
`clock_timestamp()`, `session_user`) apply instead. `SET ROLE` does not help:
`actor` is `session_user`, the authenticated login identity.

**Reader** -- `SELECT` only:

```sql
CREATE ROLE duro_reader LOGIN PASSWORD '...';
```
```sh
duro init --postgres "$ADMIN_DSN" --reader duro_reader
```

**What provisioning refuses.** Adding a restricted grant to a role that is
already powerful would report a secure writer while leaving the boundary
unenforced, so `ProvisionWriter`/`ProvisionReader` refuse -- in the same
transaction that would do the granting, so a refusal leaves no partial
grants -- when the role does not exist, cannot log in, or when **it or any
role it can reach** holds disqualifying authority.

That transaction makes the eligibility check and the grants one atomic unit:
you never get half the grants, and the grants are never applied over a check
that failed. It does **not** lock privileges against an administrator. A
superuser or role-granting admin can grant the role new authority immediately
after (or concurrently with) provisioning, and nothing here prevents or
detects it. The administrator/migrator is outside the writer protections by
design; these checks constrain what *Duro* will hand out, not what the
cluster's own privileged roles can do afterwards. Reachable means the role
itself, roles it inherits from, *and* roles it can `SET ROLE` to: a
`NOINHERIT` member of a privileged role passes an inherited-privilege check
and then simply runs `SET ROLE`, so both are checked with
`pg_has_role(..., 'USAGE')` and `pg_has_role(..., 'SET')`.

Disqualifying authority is: the `SUPERUSER`, `CREATEROLE`, `CREATEDB`,
`BYPASSRLS`, or `REPLICATION` attribute; ownership of `public.events`,
schema `public`, or the database; `UPDATE` (table-wide or on any column),
`DELETE`, `TRUNCATE`, or `TRIGGER` on `public.events`; and `INSERT` on any
server-owned column (`id`, `received_at`, `actor`) -- which a table-wide
`INSERT` grant includes. A reader additionally must not hold `INSERT` on any
column. Revoke the offending privilege or use a separate login role; the
error names every reason it found.

Re-provisioning an already-provisioned role is still fine: a writer's
`INSERT` on `event_type`/`content`/`refs` and a `SELECT` grant are not
disqualifying.

---

# Deployment

`compose.yaml` runs a local PostgreSQL 18 for development:

| Variable | Default | Purpose |
|---|---|---|
| `DURO_POSTGRES_PASSWORD` | -- (required) | Password for the initial role. |
| `DURO_POSTGRES_DB` | `duro` | Database name. |
| `DURO_POSTGRES_USER` | `duro` | Initial (owning) role. |
| `DURO_POSTGRES_BIND` | `127.0.0.1` | Host bind address. |
| `DURO_POSTGRES_PORT` | `5433` | Host port. |
| `DURO_POSTGRES_DATA_DIR` | `./.duro-postgres18-data` | Host data directory. |

The PostgreSQL 18 images keep the cluster in `/var/lib/postgresql/18/docker`
(that is the running server's own `data_directory`; `18` is the version
directory and `docker` the cluster name) and declare `/var/lib/postgresql` as
the volume, so the compose file mounts the parent directory -- not `.../data`,
which is what the 16 images wanted. The
default host directory is new and git-ignored; an existing PostgreSQL 16
`./.duro-postgres-data` is **never** reused (pointing 18 at it would refuse
to start). Artifact storage is a plain directory you choose; it is not part
of compose.

## Breaking changes

This is a deliberate reduction to the contract above. Removed, with no
replacement: the local SQLite queue and offline append path, `sync`/pull
between stores, the MCP server (`cmd/duro-mcp`), the knowledge graph,
retrieval/projection packages, the pluggable blob-store configuration and
PostgreSQL blob backend, and the artifact catalog/locator commands. The
remaining surface is exactly `init`, `event put`, `event get`, `artifact put`,
`artifact get`, and the four packages above.

The former root `duro append` command is now `duro event put`; there is no
compatibility alias. Update scripts accordingly. The underlying append
semantics and Go `Store.Append` API are unchanged; `Store.Get(id)` adds a
read-only lookup and returns `postgres.ErrEventNotFound` for absent events.

The event table is also different from earlier Duro schemas (database-owned
`id`/`received_at`/`actor`, `jsonb` `content`/`refs` with server-side
limits). **`duro init` will not upgrade an older table**: it refuses and
changes nothing. Deploy against a fresh PostgreSQL 18 database. Copying old
data forward is a separate exercise, deliberately out of scope here: the
schemas are incompatible, so preserving existing rows needs a migration
planned and verified on its own terms, and none is provided or implied by
this package.

---

# Tests

```sh
export DURO_POSTGRES_TEST_DSN="postgresql://postgres@127.0.0.1:5433/postgres?sslmode=disable"
go test -race ./...
uv run python -m unittest discover -s python/tests -v
```

`DURO_POSTGRES_TEST_DSN` must be an administrator DSN for a **disposable**
PostgreSQL 18+ cluster: the tests `CREATE DATABASE` and `CREATE ROLE` with
unique names, exercise real appends, real restricted-credential denials, and
real artifact I/O, then drop everything they created. They never touch a
shared `public.events`.

If the variable is unset the tests **fail** rather than skip: the contract's
acceptance gate does not count a skipped integration test as a pass.

Covered: committed append and readback, event get full-record equality and
restricted-reader access, invalid/missing IDs and database errors, duplicate-input events, UUIDv7
version/variant bits, `received_at` as insertion time, `actor` bound to
`session_user` (including under `SET ROLE`), every field constraint at the
database boundary, oversized-versus-normalized JSON, incompatible-schema
refusal (six variants that a column-shape-only check would wrongly accept,
plus unlogged storage, a view, a user trigger, and a rule), concurrent
`Initialize`, writer/reader privilege boundaries and forbidden
mutations, provisioning refusals for every
disqualifying attribute, ownership, privilege, and `SET ROLE`-reachable
authority, commit-order inversion and
repeatable-read snapshot reads, commit-outcome classification, artifact
round-trip, duplicate and concurrent same-digest puts, corruption of stored
bytes left untouched, interrupted reads, staging/`fsync`/destination
failures, temp-file cleanup, deterministic source-grow/shrink mismatch via a
private stat/read seam, short-write and ENOSPC propagation, fsync of the blob
and of every ancestor directory up to the store root on both fresh and
deduplicating puts (asserted on the handles actually synced, including after a
failed ancestor sync and while a concurrent creator is pinned mid-sync),
exact observation-event
readback, stored-artifact-with-failed-append reporting, allocation per
operation across a 16x artifact-size range at fixed concurrency, and the
real compiled CLI for every help surface, argument rejection,
environment/flag precedence, end-to-end init/event put/event get/artifact put/artifact get, and the
partial-outcome JSON error schema.

One case is not covered by these tests: a connection lost *after* `COMMIT`
is sent, which is the only way to produce a genuine `unknown` outcome
against a live server. Reproducing it requires interrupting the wire between
client and server (for example a TCP proxy that forwards the `COMMIT` and
then drops the response); the classification logic itself is unit-tested.
