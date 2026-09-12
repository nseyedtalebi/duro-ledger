// Package sync drains Duro's local SQLite queue into the PostgreSQL
// canonical store and pulls canonical events back into the local replica,
// directly over a database connection. It introduces no transport of its
// own.
package sync

import (
	"errors"
	"fmt"

	"github.com/nseyedtalebi/duro-ledger/pkg/blob"
	"github.com/nseyedtalebi/duro-ledger/pkg/local"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

// maxReasonLen bounds the error reason recorded against a retained
// conflict/rejected row, so a pathological remote error can't grow a
// local row without limit.
const maxReasonLen = 2000

// PushResult tallies the outcomes of one Push call.
type PushResult struct {
	Accepted, AlreadyPresent, Conflict, Rejected int
}

// Push drains up to limit pending rows from src, oldest first, into dst,
// embedding any staged blob content directly in PostgreSQL. It is the
// PostgreSQL-default wrapper around pushRows; see PushWithBlobStore for the
// explicit-backend variant.
func Push(src *local.Store, dst *postgres.Store, limit int) (PushResult, error) {
	return pushRows(src, limit, func(row local.PendingRow, b local.Blob, hasBlob bool) (postgres.Outcome, int64, error) {
		if !hasBlob {
			return dst.Insert(row.Event)
		}
		return dst.InsertWithBlob(row.Event, b.Content, b.SHA256, b.MediaType)
	})
}

// PushWithBlobStore is Push, but a staged blob's bytes are durably written
// to backend first, and only then linked into the canonical event via
// InsertWithBlobRef -- PostgreSQL never sees the blob content itself. A
// filesystem-prestaged blob is reverified before the same link. If either
// backend action fails, the row remains pending for retry.
func PushWithBlobStore(src *local.Store, dst *postgres.Store, backend blob.Store, limit int) (PushResult, error) {
	return pushRows(src, limit, func(row local.PendingRow, b local.Blob, hasBlob bool) (postgres.Outcome, int64, error) {
		if !hasBlob {
			return dst.Insert(row.Event)
		}
		if b.External {
			if backend.Kind() != "filesystem" {
				return postgres.Rejected, 0, fmt.Errorf("sync: blob %s was staged in filesystem storage; sync requires --blob-store=filesystem", b.SHA256)
			}
			if err := backend.Verify(b.SHA256, b.Size); err != nil {
				return postgres.Rejected, 0, fmt.Errorf("sync: verify prestaged blob %s in %s backend: %w", b.SHA256, backend.Kind(), err)
			}
			return dst.InsertWithBlobRef(row.Event, b.SHA256, b.Size, b.MediaType)
		}
		if err := backend.Put(b.SHA256, b.Content); err != nil {
			return postgres.Rejected, 0, fmt.Errorf("sync: write blob %s to %s backend: %w", b.SHA256, backend.Kind(), err)
		}
		return dst.InsertWithBlobRef(row.Event, b.SHA256, b.Size, b.MediaType)
	})
}

// pushRows drains up to limit pending rows from src, oldest first, calling
// insert to record each one against the canonical store. Rows reported
// accepted or already present are marked synchronized with their canonical
// sequence. Rows reported as conflict or rejected are retained pending with
// a bounded error reason, so they stay visible for inspection or retry
// instead of being silently dropped.
//
// pushRows is safe to retry after a crash: src.Pending only returns rows
// not yet marked synchronized, and idempotent canonical inserts classify a
// retried ID as AlreadyPresent instead of duplicating work.
func pushRows(src *local.Store, limit int, insert func(row local.PendingRow, b local.Blob, hasBlob bool) (postgres.Outcome, int64, error)) (PushResult, error) {
	var res PushResult
	pending, err := src.Pending()
	if err != nil {
		return res, fmt.Errorf("sync: list pending: %w", err)
	}
	if limit > 0 && len(pending) > limit {
		pending = pending[:limit]
	}

	for _, row := range pending {
		b, hasBlob, err := src.PendingBlob(row.Event.ID)
		if err != nil {
			return res, fmt.Errorf("sync: read staged blob %s: %w", row.Event.ID, err)
		}

		outcome, seq, err := insert(row, b, hasBlob)
		if outcome == postgres.Rejected {
			var validationErr *postgres.ValidationError
			if err != nil && !errors.As(err, &validationErr) {
				return res, fmt.Errorf("sync: push %s: %w", row.Event.ID, err)
			}
			reason := "rejected by canonical store"
			if err != nil {
				reason = err.Error()
			}
			if err := src.MarkRejected(row.Event.ID, bound(reason)); err != nil {
				return res, fmt.Errorf("sync: mark rejected %s: %w", row.Event.ID, err)
			}
			res.Rejected++
			continue
		}
		if err != nil {
			return res, fmt.Errorf("sync: push %s: %w", row.Event.ID, err)
		}
		switch outcome {
		case postgres.Accepted, postgres.AlreadyPresent:
			if err := src.MarkAccepted(row.Event.ID, seq); err != nil {
				return res, fmt.Errorf("sync: mark accepted %s: %w", row.Event.ID, err)
			}
			if outcome == postgres.Accepted {
				res.Accepted++
			} else {
				res.AlreadyPresent++
			}
		case postgres.Conflict:
			if err := src.MarkRejected(row.Event.ID, bound("conflict: canonical event exists with different payload")); err != nil {
				return res, fmt.Errorf("sync: mark conflict %s: %w", row.Event.ID, err)
			}
			res.Conflict++
		default:
			return res, fmt.Errorf("sync: unknown outcome %v for %s", outcome, row.Event.ID)
		}
	}
	return res, nil
}

func bound(reason string) string {
	if len(reason) > maxReasonLen {
		return reason[:maxReasonLen]
	}
	return reason
}

// Pull fetches canonical events after dst's local cursor, in batches of
// at most batchSize, and applies each batch to dst before advancing the
// cursor past it. It returns the total number of events applied.
//
// Pull is safe to retry after a crash: each batch is applied and its
// cursor advance committed together (see local.Store.ApplyRemoteBatch),
// so the cursor only ever reflects a fully committed batch and the next
// Pull resumes from there rather than skipping or duplicating events.
func Pull(src *postgres.Store, dst *local.Store, batchSize int) (int, error) {
	total := 0
	for {
		cursor, err := dst.Cursor()
		if err != nil {
			return total, fmt.Errorf("sync: read cursor: %w", err)
		}
		rows, err := src.Pull(cursor, batchSize)
		if err != nil {
			return total, fmt.Errorf("sync: pull: %w", err)
		}
		if len(rows) == 0 {
			return total, nil
		}

		applied := make([]local.RemoteEvent, len(rows))
		last := cursor
		for i, r := range rows {
			applied[i] = local.RemoteEvent{Event: r.Event, Sequence: r.Sequence}
			last = r.Sequence
		}
		if err := dst.ApplyRemoteBatch(applied, last); err != nil {
			return total, fmt.Errorf("sync: apply batch: %w", err)
		}
		total += len(rows)

		if len(rows) < batchSize {
			return total, nil
		}
	}
}
