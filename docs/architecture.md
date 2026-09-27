# Duro architecture

Duro is a small event ledger for systems that need a retryable record of facts
without making every producer own database semantics, blob storage, or a
projection framework.

It has one rule: **one canonical event history, with optional local durability
and optional verified bytes.**

```text
producer
  └─ SQLite queue (optional, local only)
       └─ PostgreSQL canonical events
            ├─ ordered pull / replay
            ├─ optional PostgreSQL blob bytes
            └─ optional filesystem CAS bytes + catalog metadata
```

This is intentionally not multi-master replication, an application framework,
or a generic data model.

## Authority boundaries

| Concern | Authority | Why |
| --- | --- | --- |
| Event acceptance, canonical order, exact-retry outcome | PostgreSQL | A buggy or alternate client must not create conflicting canonical history. |
| Offline work and retry bookkeeping | one local SQLite queue | A client can survive a crash or network outage without claiming to be canonical. |
| Event meaning | producer-owned JSON objects | Duro carries facts without prematurely imposing a domain schema. |
| Inline canonical blob bytes | PostgreSQL, when `--blob-store postgres` | The simplest deployment keeps bytes and events in one backup boundary. |
| Filesystem artifact bytes | filesystem CAS, when selected | Large immutable bytes stay in a verified, deduplicating content-addressed store. |
| Filesystem artifact catalog | PostgreSQL metadata | Catalog queries and provenance do not duplicate the bytes. |
| Read models | projectors | They are rebuilt from canonical events rather than becoming a second authority. |

The separation is deliberate. A path or URL can change; a SHA-256 digest names
specific bytes. A projector can be rebuilt; the accepted event history is what
must survive.

**Review:** [event envelope](../pkg/event/event.go#L24-L114), [canonical
schema](../pkg/postgres/schema.sql#L1-L105), [local queue](../pkg/local/store.go#L91-L173).

## Canonical events: PostgreSQL decides

An event has a UUID, occurrence time, type, actor, JSON `content`, JSON `refs`,
and an optional `resource_uri`. Duro validates this shape locally for useful
errors and offline operation, but PostgreSQL is the final acceptance boundary.

The database assigns the canonical sequence, rejects malformed canonical rows,
keeps event IDs unique, binds a newly accepted event's actor to the authenticated
PostgreSQL role, and preserves the original actor for an exact retry. A repeated
ID with the same logical payload is `already_present`; a changed payload is a
conflict. Corrections are new events, not updates.

This is why Duro uses PostgreSQL rather than trusting a client-side log: clients
can crash, retry after uncertain delivery, or be wrong. The client remains the
ordinary producer interface; the database owns only the invariants that must
survive bypassing clients.

**Review:** [event validation](../pkg/event/event.go#L68-L132),
[idempotent insert and retry classification](../pkg/postgres/store.go#L141-L209),
[actor-binding trigger](../pkg/postgres/schema.sql#L80-L101).

## SQLite: a durable queue, not a second ledger

`append` and `file` can first write to a local SQLite database. That gives one
producer a durable retry point while offline. The queue records accepted,
rejected, and conflicting delivery attempts; it is not a shared server, a
run catalog, or a replacement event history.

`sync` pushes pending rows oldest-first. Accepted and already-present rows are
marked synchronized; conflicts and rejections remain visible with a bounded
reason. `pull` applies a canonical batch and its cursor advance in one local
transaction, so a crash cannot advance past unapplied events.

**Review:** [local queue contract](../pkg/local/store.go#L1-L5),
[enqueue idempotency](../pkg/local/store.go#L151-L173),
[sync push outcomes](../pkg/sync/sync.go#L63-L127),
[pull cursor handling](../pkg/sync/sync.go#L137-L175).

## Bytes: choose one backend explicitly

Duro stores exact bytes separately from JSON event content. `content` preserves
structured meaning; a blob preserves the exact original payload.

The default backend stores bytes in PostgreSQL. The filesystem backend uses a
SHA-256 content-addressed store with two-level sharding, streamed hashing,
temporary-file `fsync`, atomic rename, verified deduplication, and verified
reads. It is selected explicitly with `--blob-store filesystem --blob-root
ABSOLUTE_PATH`.

When filesystem bytes are used for an event, `sync` writes or verifies the bytes
first and only then links their digest into the canonical event. The two actions
are not magically atomic across PostgreSQL and the filesystem; retry is the
recovery mechanism. A failed catalog or append after a durable CAS write is a
partial success, so retry the same operation rather than deleting verified
bytes.

**Review:** [small backend contract](../pkg/blob/blob.go#L12-L31),
[filesystem CAS durability](../pkg/cas/cas.go#L92-L170),
[verified reads and verification](../pkg/cas/cas.go#L198-L268),
[external-backend sync ordering](../pkg/sync/sync.go#L39-L60).

## Artifact catalog: filesystem bytes, PostgreSQL inventory

`duro artifact` is the general Duro surface for cataloging a filesystem-CAS
artifact without inventing an event type or a second artifact database:

```sh
duro artifact put --postgres DSN --blob-root /srv/duro-blobs --file output.parquet \
  --locator file:///incoming/delivery/output.parquet

duro artifact verify --postgres DSN --blob-root /srv/duro-blobs --sha256 DIGEST

duro artifact locate --postgres DSN --prefix file:///incoming/
```

`put` streams a file into the CAS, then records the digest, immutable byte size,
verification time, and optional **source** locator observations. A retry dedups
the CAS bytes and fills a missing catalog row. `verify` rehashes one stored blob
before updating its verification time. `locate` returns JSON observations by a
literal URI prefix; `%`, `_`, and backslash are not wildcards.

The catalog deliberately reuses `blobs` for digest and size. The only added
facts are `last_verified_at` and a many-to-one observation table. Duro does not
store the derived CAS destination path: it contains no information beyond the
digest and a host-local root.

**Review:** [artifact CLI](../cmd/duro/main.go#L417-L588),
[catalog schema](../pkg/postgres/schema.sql#L49-L67),
[catalog transactions and literal-prefix query](../pkg/postgres/artifact.go#L43-L169),
[live CLI test](../cmd/duro/main_test.go#L534-L583).

## Two kinds of URI

A `resource_uri` names the logical resource or relation an event asserts at that
time. It is opaque, absolute, preserved exactly, and part of exact-retry
identity. Duro never resolves, normalizes, or interprets its scheme.

An artifact `--locator` is different: it is a physical source observation such
as a file or object URI where bytes were seen. Several locators may refer to one
digest. Locator lookup never fetches bytes; the CAS reads by digest.

Keeping these separate prevents a moving source path from becoming canonical
identity, and prevents an event's semantic meaning from being confused with a
storage observation.

**Review:** [historical resource-URI contract lock](resource-uri-forward-contract-2026-09-12-lock.md),
[URI validation](../pkg/event/event.go#L104-L114),
[physical locator schema](../pkg/postgres/schema.sql#L54-L67).

## Projections are consumers, not authorities

Duro's knowledge-graph projection replays `knowledge.fact` and
`knowledge.fact.invalidated` events in canonical sequence order. Retrieval
replays text document events whose bytes are in PostgreSQL into an in-memory
vector collection supplied by the caller. Both retain canonical event/blob
provenance and can be rebuilt from sequence zero. Retrieval is not yet a
filesystem-CAS consumer.

This keeps domain-specific indexing, embeddings, and "latest" policies outside
the core ledger. Duro owns ordered facts and exact bytes; consumers own their
interpretation.

**Review:** [knowledge-graph replay](../pkg/knowledgegraph/knowledgegraph.go#L57-L186),
[retrieval projection boundary](../pkg/retrieval/retrieval.go#L29-L105).

## Deployment and extension boundaries

- `duro init` applies schema with an administrator/provisioning DSN. Ordinary
  `sync` and `pull` do not perform DDL.
- A direct PostgreSQL connection is the first transport. Duro does not add an
  HTTP event service merely for abstraction.
- `duro-mcp` is an optional deployment adapter with its own private listener,
  token file, PostgreSQL DSN file, and queue; see its
  [deployment contract](../cmd/duro-mcp/README.md).
- SAC remains a separate local-first CAS utility. Duro's generic blob-store
  seam can support an external verifier, but Duro does not ship a SAC adapter.

## Intentional non-goals

Duro does **not** currently provide multi-master replication, a generic schema
registry, a URI resolver, an RDF store, destructive CAS pruning, bulk artifact
reconciliation, or a multi-store replica model. Those features would add policy
and failure modes before a demonstrated user needs them.

The current design stays small by making each layer responsible for one job:
local retry, canonical acceptance, verified bytes, catalog observations, or a
rebuildable read model.

## Evidence and change history

The current direction is recorded in the following decision and implementation
records:

- `drawer_wing_academy_decisions_44fd3f6ec8ab834175021af9` — PostgreSQL as
  default canonical path; SQLite as optional local spool.
- `drawer_wing_academy_decisions_23d8cb6598f77be71b9d32a5` — canonical
  invariants belong in PostgreSQL, with Go as the producer/fast-error layer.
- `drawer_wing_academy_decisions_3e90a9da10f3761dc18bcfef` — general Duro
  filesystem CAS plus PostgreSQL catalog direction.
- `drawer_wing_academy_implementation_10d0d1d39826588ddba1e246` — generic
  external blob verification during sync.
- `drawer_wing_academy_implementation_b9d234ef16bf25139f369a64` — artifact
  catalog commands and live verification record.
