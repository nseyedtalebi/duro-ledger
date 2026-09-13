# Resource URI forward-contract lock — 2026-09-12

**Status:** owner-authorized forward migration block. This lock authorizes only
the generic Duro field and its durable local/canonical transport. It does not
authorize Cura writer activation, projector deployment, URI-prefix reader
filters, historical mapping/backfill, reader routing, or MemPalace retirement.

## Decision

Add optional `resource_uri` to Duro's immutable `event.Event` envelope.

- It is an exact, opaque, absolute URI at event time.
- Empty is permitted for compatibility with existing and non-resource events.
- Duro validates absolute-URI syntax only. It neither dereferences,
  normalizes, derives, nor interprets scheme/path semantics.
- A resource URI is neither artifact identity (`urn:sha256:<digest>`) nor an
  observed physical locator (`file://…`). It is not a replacement for `source`.
- Exact retry requires an equal `resource_uri`; a changed URI with the same
  event ID is a conflict. A filing correction is a new event under a later
  event-family contract.

## First vertical slice

1. `pkg/event`: add `ResourceURI string` plus standard-library absolute-URI
   validation and a backwards-compatible constructor for callers that provide it.
2. `pkg/local`: persist/recover the field through the SQLite queue and local
   replica, including idempotency/collision comparison. Existing local queues
   migrate with an empty field.
3. `pkg/postgres`: migrate `events.resource_uri TEXT NULL`; insert, retry
   classification, and pull preserve it. Existing canonical rows read as empty.
4. `cmd/duro`: add optional `--resource-uri` to `append` and `file` so a real
   forward writer can supply it without embedding it ambiguously in `content`.

## Locked acceptance cases

- `cura://palace/wing/room/drawer-id`, `rador://…`, and
  `urn:sha256:<digest>` validate and round-trip byte-for-byte as URI strings.
- Relative, malformed, and control-character URI input is rejected before a
  local row or canonical insert.
- Empty stays supported and round-trips for legacy events.
- Same ID + same URI is an exact retry; same ID + changed URI conflicts in both
  local and canonical paths.
- SQLite reopen and canonical pull preserve an accepted URI.
- The CLI `file --resource-uri` writes the field to the local event envelope.
- Existing tests remain green. PostgreSQL integration tests may only be called
  passed when run with an explicitly disposable `DURO_POSTGRES_TEST_DSN`.

## Explicit non-goals

No scheme registry, URI canonicalizer, locator catalog, URI index, prefix
lookup, filesystem dereference, automatic legacy URI generation, data backfill,
Cura-specific validation, RADOR implementation, or reader redirect.

## Evidence basis

- Cura direction: `nseyedtalebi/cura` commit
  `1d85e9533e47b847225f2d0ce2c35b048877f25f`.
- Palace design correction:
  `drawer_wing_academy_migration_154d59c82a21b2d5a2444a38`.
- Palace documentation-sweep record:
  `drawer_wing_academy_migration_d7af69a2c90a4f7af91a9316`.
