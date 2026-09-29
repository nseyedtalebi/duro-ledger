// Package artifact orchestrates Duro's two-store artifact contract: bytes
// live in the filesystem content-addressed store (pkg/cas), and each
// successful put is recorded as an artifact.observed event in the
// canonical PostgreSQL ledger (pkg/postgres). The two are separate
// commits; PutError reports the boundary precisely when they disagree.
package artifact

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode/utf8"

	"github.com/nseyedtalebi/duro-ledger/pkg/cas"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

// appender is the subset of the PostgreSQL ledger that Put depends on. The
// production ledger is *postgres.Store; tests may substitute a fake
// implementation to simulate append outcomes without a live database.
//
// The error boundary is part of this interface. Only *postgres.AppendError
// carries a classification of whether the event committed; Put reports any
// other error as EventOutcome "unknown", because an implementation that does
// not classify its failures has given no evidence that nothing was written.
// An appender that can prove a non-commit must say so with an
// *postgres.AppendError, or its callers must treat every failure as a
// possible commit: retrying an unknown outcome can duplicate the observation
// event, and not retrying can lose it.
type appender interface {
	Append(event.New) (postgres.StoredEvent, error)
}

// EventType is the canonical event type recorded for every successful put.
const EventType = "artifact.observed"

// PutResult is the receipt for a fully successful put: bytes are durably
// stored and their observation event is committed.
type PutResult struct {
	Digest string // sha256:<64 lowercase hex characters>
	Size   int64
	Event  postgres.StoredEvent
}

// PutError reports that artifact bytes were stored (or already existed and
// were verified), but the observation event's commit did not succeed or its
// outcome could not be confirmed. The bytes are retained regardless:
// callers must not delete or retry storage on this error, only decide
// whether to retry the append.
type PutError struct {
	Digest         string
	Size           int64
	ArtifactStored bool
	// EventOutcome is "not_committed" or "unknown", mirroring
	// postgres.CommitOutcome.
	EventOutcome string
	Err          error
}

func (e *PutError) Error() string {
	return fmt.Sprintf("artifact: put %s stored=%v event_outcome=%s: %v", e.Digest, e.ArtifactStored, e.EventOutcome, e.Err)
}
func (e *PutError) Unwrap() error { return e.Err }

// Put streams sourcePath into store, then appends its observation event to
// ledger. Every successful put appends a fresh event, including a put whose
// bytes are byte-for-byte identical to an already-stored artifact: dedup
// applies to bytes, never to the observation history.
//
// If storage or verification of the bytes themselves fails, Put returns a
// plain error and appends no event. Such a failure usually leaves nothing
// behind, but it is not a guarantee that no bytes remain: a failure after
// the digest path is linked (for example a later fsync failure) can leave a
// complete artifact on disk with no event recorded. Content addressing makes
// that harmless -- a retry reuses and reverifies those bytes -- but callers
// must not assume a plain error means an empty store.
//
// If bytes are stored (or verified-identical) but the event append fails or
// its outcome is unknown, Put returns *PutError describing exactly that: the
// artifact is retained on disk regardless.
func Put(store *cas.Store, ledger appender, sourcePath string) (PutResult, error) {
	absPath, err := filepath.Abs(sourcePath)
	if err != nil {
		return PutResult{}, fmt.Errorf("artifact: resolving absolute path for %s: %w", sourcePath, err)
	}
	// Resolve the observing host before storing anything: source_host is
	// part of the observation event's meaning, and a silently empty host
	// would record bytes as observed nowhere.
	host, err := os.Hostname()
	if err != nil {
		return PutResult{}, fmt.Errorf("artifact: resolving source_host: %w", err)
	}
	if host == "" {
		return PutResult{}, fmt.Errorf("artifact: this host reports an empty hostname; source_host must name the observing host")
	}
	// Unix filenames are arbitrary bytes, but the observation event is JSON:
	// encoding/json replaces every invalid UTF-8 byte with U+FFFD instead of
	// failing, which would record a silently different source_path or
	// source_host as if it were exact. Reject before any byte is ingested, so
	// a path Duro cannot represent exactly leaves no blob and no event.
	for _, f := range []struct{ name, value string }{{"source_path", absPath}, {"source_host", host}} {
		if !utf8.ValidString(f.value) {
			return PutResult{}, fmt.Errorf("artifact: %s %q is not valid UTF-8; the observation event records it exactly or not at all", f.name, f.value)
		}
	}

	digest, size, _, err := store.WriteFile(absPath)
	if err != nil {
		return PutResult{}, err
	}
	identity := cas.FormatIdentity(digest)

	content, err := json.Marshal(map[string]any{
		"digest":      identity,
		"size_bytes":  size,
		"source_host": host,
		"source_path": absPath,
	})
	if err != nil {
		return PutResult{}, fmt.Errorf("artifact: encoding observation event: %w", err)
	}

	se, err := ledger.Append(event.New{EventType: EventType, Content: content, Refs: json.RawMessage(`{}`)})
	if err != nil {
		// Default to the conservative outcome: only an *postgres.AppendError
		// carries evidence about whether the event committed.
		outcome := "unknown"
		var ae *postgres.AppendError
		if errors.As(err, &ae) {
			outcome = ae.Outcome.String()
		}
		return PutResult{}, &PutError{Digest: identity, Size: size, ArtifactStored: true, EventOutcome: outcome, Err: err}
	}
	return PutResult{Digest: identity, Size: size, Event: se}, nil
}

// GetToFile streams the artifact identified by identity (sha256:<hex>) from
// store into destPath. destPath is never overwritten if it already exists.
func GetToFile(store *cas.Store, identity, destPath string) error {
	digest, err := cas.ParseIdentity(identity)
	if err != nil {
		return err
	}
	return store.GetToFile(digest, destPath)
}

// GetToWriter streams the artifact identified by identity (sha256:<hex>)
// from store to w. w may receive partial or corrupt bytes before a non-nil
// error is returned; callers MUST treat any error as a failed operation.
func GetToWriter(store *cas.Store, identity string, w io.Writer) error {
	digest, err := cas.ParseIdentity(identity)
	if err != nil {
		return err
	}
	return store.GetToWriter(digest, w)
}
