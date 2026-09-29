# Duro — minimal contract

**Status: draft for review.**

## 1. Scope

Duro provides:

- Append an event to PostgreSQL.
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
- Database-generated IDs verified as UUIDv7; caller identity overrides rejected.
- Restricted-role tests for validation, actor binding, and forbidden mutations.
- Concurrent full-read tests consistent with the documented ordering/snapshot semantics.
- Artifact round-trip, verified duplicate put, and concurrent same-digest puts.
- Exact observation-event readback: digest, byte count, source host/path, and authenticated actor; each successful put, including duplicate-content puts, emits a new event.
- Stored-artifact/event-append failure and unknown-commit cases, including accurate partial-outcome reporting and retention of stored artifacts.
- Corruption, disk/write failure, interruption, and partial-output tests.
- Memory measurements across increasing artifact sizes at fixed concurrency.
- CLI and library paths both exercised; skipped integration tests do not count as passes.


