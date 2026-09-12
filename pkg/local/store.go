// Package local is the durable SQLite queue events sit in before (and,
// once rejected, after) an attempt to reach the PostgreSQL canonical
// store. It is a queue, not a projection: it knows nothing about runs,
// artifacts, or hash chains.
package local

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	_ "embed"

	_ "modernc.org/sqlite"

	"github.com/nseyedtalebi/duro-ledger/pkg/cas"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
)

//go:embed schema.sql
var schema string

// ErrConflict is returned by Enqueue when an event ID already in the queue
// carries different logical content than the one being enqueued.
var ErrConflict = errors.New("local: event id already used with different content")

// ErrDigestMismatch is returned by PendingBlob when the bytes read back for
// a staged blob no longer hash to the digest recorded when they were
// written. That is storage-level corruption, not an ordinary application
// error, so it must never be silently treated as valid content ready to
// upload.
var ErrDigestMismatch = errors.New("local: blob content does not match stored digest")

const timeLayout = time.RFC3339Nano

// encodeLocalCreated and decodeLocalCreated give local_created a
// lexicographically-correct sortable representation: RFC3339Nano text
// sorts wrong across a second boundary because a bare "...00Z" (no
// fraction) compares greater than "...00.5Z" ('.' < 'Z' in ASCII).
// Unix nanoseconds sort correctly as plain integers.
func encodeLocalCreated(t time.Time) int64 { return t.UTC().UnixNano() }

func decodeLocalCreated(nanos int64) time.Time { return time.Unix(0, nanos).UTC() }

// jsonEqual reports whether a and b are the same JSON object modulo key
// order and insignificant whitespace. Array order and each number's
// literal value are preserved (decoding via UseNumber avoids float64
// rounding, so e.g. 9007199254740992 and 9007199254740993 stay distinct).
// A parse failure on either side is an error, not a false match: stored
// JSON is expected to already be valid, so a failure here means it was
// tampered with or corrupted, and that must not be silently accepted.
func jsonEqual(a, b string) (bool, error) {
	av, err := canonicalJSON(a)
	if err != nil {
		return false, fmt.Errorf("stored value is not valid JSON: %w", err)
	}
	bv, err := canonicalJSON(b)
	if err != nil {
		return false, fmt.Errorf("candidate value is not valid JSON: %w", err)
	}
	return reflect.DeepEqual(av, bv), nil
}

func canonicalJSON(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	// A second Decode is the only reliable trailing-data check: Decoder.More
	// just peeks for a following non-whitespace byte, so it can't tell
	// "trailing whitespace only" (fine) from "trailing garbage" (not) the
	// way trying to decode it and requiring io.EOF does.
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing data after JSON value")
		}
		return nil, err
	}
	return v, nil
}

// Store is the SQLite-backed local event queue.
type Store struct {
	db *sql.DB
}

// Open connects to (creating if needed) the local queue database at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// One connection: the first client is single-process, and Enqueue's
	// read-then-write conflict check is only safe for a single writer.
	// ponytail: single writer; add locking if concurrent writers become real.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// PendingRow is a queued event alongside its local queue bookkeeping.
type PendingRow struct {
	Event        event.Event
	LocalCreated time.Time
	LastError    string
}

// Enqueue persists ev if its ID is not already queued. Re-enqueuing an
// event with the same ID and the same logical content is a no-op
// (fresh=false, err=nil). Re-enqueuing the same ID with different content
// returns ErrConflict and leaves the stored row untouched.
func (s *Store) Enqueue(ev event.Event) (fresh bool, err error) {
	if err := ev.Validate(); err != nil {
		return false, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	fresh, err = checkOrInsertEvent(tx, ev)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return fresh, nil
}

// EnqueueWithBlob is Enqueue, but also stores blob content alongside ev in
// the same SQLite transaction: either both are persisted or neither is.
// The blob's SHA-256 digest is computed from content and stored so
// PendingBlob can later detect corruption before the bytes are uploaded.
// Content is kept in its own table, distinct from ev's JSON content/refs.
// As with Enqueue, a retry with the same ID and the same logical event
// payload is a no-op; the blob table is only written on a fresh insert.
func (s *Store) EnqueueWithBlob(ev event.Event, content []byte, mediaType string) (fresh bool, err error) {
	if err := ev.Validate(); err != nil {
		return false, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	fresh, err = checkOrInsertEvent(tx, ev)
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	storedContent := content
	if storedContent == nil {
		storedContent = []byte{}
	}
	if !fresh {
		existing, err := existingBlob(tx, ev.ID)
		if err == sql.ErrNoRows || (err == nil && (existing.SHA256 != digest || existing.MediaType != mediaType || existing.Size != int64(len(storedContent)))) {
			return false, ErrConflict
		}
		if err != nil {
			return false, err
		}
	}
	if fresh {
		if _, err := tx.Exec(
			`INSERT INTO local_blobs(event_id, sha256, media_type, size_bytes, content) VALUES (?,?,?,?,?)`,
			ev.ID, digest, mediaType, len(storedContent), storedContent,
		); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return fresh, nil
}

// EnqueueWithBlobRef queues an event whose blob bytes are already durable in
// the filesystem backend. It stores only the verified content identity and
// metadata locally, avoiding a second SQLite copy of a large artifact.
func (s *Store) EnqueueWithBlobRef(ev event.Event, digest string, size int64, mediaType string) (fresh bool, err error) {
	if err := ev.Validate(); err != nil {
		return false, err
	}
	if !cas.ValidDigest(digest) {
		return false, fmt.Errorf("local: invalid SHA-256 digest %q", digest)
	}
	if size < 0 {
		return false, fmt.Errorf("local: blob size must not be negative")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	fresh, err = checkOrInsertEvent(tx, ev)
	if err != nil {
		return false, err
	}
	if !fresh {
		existing, err := existingBlob(tx, ev.ID)
		if err == sql.ErrNoRows || (err == nil && (existing.SHA256 != digest || existing.MediaType != mediaType || existing.Size != size)) {
			return false, ErrConflict
		}
		if err != nil {
			return false, err
		}
	} else if _, err := tx.Exec(
		`INSERT INTO local_blob_refs(event_id, sha256, media_type, size_bytes) VALUES (?,?,?,?)`,
		ev.ID, digest, mediaType, size,
	); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return fresh, nil
}

// checkOrInsertEvent checks whether ev's ID is already queued inside tx,
// inserting it if not. It never commits: callers that also need to write a
// blob row in the same transaction do so before committing, so the event
// and its blob land atomically.
func checkOrInsertEvent(tx *sql.Tx, ev event.Event) (fresh bool, err error) {
	var occurredAt, eventType, actor, content, refs string
	err = tx.QueryRow(
		`SELECT occurred_at, event_type, actor, content, refs FROM local_events WHERE id = ?`, ev.ID,
	).Scan(&occurredAt, &eventType, &actor, &content, &refs)
	switch {
	case err == sql.ErrNoRows:
		_, err = tx.Exec(
			`INSERT INTO local_events(id, occurred_at, event_type, actor, content, refs, local_created)
			 VALUES (?,?,?,?,?,?,?)`,
			ev.ID, ev.OccurredAt.UTC().Format(timeLayout), ev.EventType, ev.Actor,
			string(ev.Content), string(ev.Refs), encodeLocalCreated(time.Now()),
		)
		if err != nil {
			return false, err
		}
		return true, nil
	case err != nil:
		return false, err
	default:
		contentEqual, err := jsonEqual(content, string(ev.Content))
		if err != nil {
			return false, fmt.Errorf("local: comparing content for %s: %w", ev.ID, err)
		}
		refsEqual, err := jsonEqual(refs, string(ev.Refs))
		if err != nil {
			return false, fmt.Errorf("local: comparing refs for %s: %w", ev.ID, err)
		}
		if occurredAt == ev.OccurredAt.UTC().Format(timeLayout) && eventType == ev.EventType && actor == ev.Actor && contentEqual && refsEqual {
			return false, nil
		}
		return false, ErrConflict
	}
}

// Blob is opaque bytes staged alongside a pending event. It is kept
// distinct from the event's JSON content/refs so blob bytes are never
// confused with event content.
type Blob struct {
	SHA256    string
	MediaType string
	Size      int64
	Content   []byte
	External  bool
}

func existingBlob(tx *sql.Tx, id string) (Blob, error) {
	var b Blob
	err := tx.QueryRow(`SELECT sha256, media_type, size_bytes FROM local_blobs WHERE event_id = ?`, id).Scan(&b.SHA256, &b.MediaType, &b.Size)
	if err == nil {
		return b, nil
	}
	if err != sql.ErrNoRows {
		return Blob{}, err
	}
	err = tx.QueryRow(`SELECT sha256, media_type, size_bytes FROM local_blob_refs WHERE event_id = ?`, id).Scan(&b.SHA256, &b.MediaType, &b.Size)
	if err != nil {
		return Blob{}, err
	}
	b.External = true
	return b, nil
}

// PendingBlob returns the blob content staged for event id, if any. It
// recomputes the SHA-256 digest from the bytes just read and compares it
// to the digest recorded when they were written, returning
// ErrDigestMismatch on disagreement so a caller about to upload never
// ships bytes it can no longer vouch for.
func (s *Store) PendingBlob(id string) (Blob, bool, error) {
	var syncedAt sql.NullString
	err := s.db.QueryRow(`SELECT synced_at FROM local_events WHERE id = ?`, id).Scan(&syncedAt)
	if err == sql.ErrNoRows || syncedAt.Valid {
		return Blob{}, false, nil
	}
	if err != nil {
		return Blob{}, false, err
	}
	var digest, mediaType string
	var size int64
	var content []byte
	err = s.db.QueryRow(
		`SELECT sha256, media_type, size_bytes, content FROM local_blobs WHERE event_id = ?`, id,
	).Scan(&digest, &mediaType, &size, &content)
	if err == sql.ErrNoRows {
		err = s.db.QueryRow(
			`SELECT sha256, media_type, size_bytes FROM local_blob_refs WHERE event_id = ?`, id,
		).Scan(&digest, &mediaType, &size)
		if err == sql.ErrNoRows {
			return Blob{}, false, nil
		}
		if err != nil {
			return Blob{}, false, err
		}
		return Blob{SHA256: digest, MediaType: mediaType, Size: size, External: true}, true, nil
	}
	if err != nil {
		return Blob{}, false, err
	}
	sum := sha256.Sum256(content)
	got := hex.EncodeToString(sum[:])
	if got != digest {
		return Blob{}, false, fmt.Errorf("%w: event %s stored %s, computed %s", ErrDigestMismatch, id, digest, got)
	}
	return Blob{SHA256: digest, MediaType: mediaType, Size: size, Content: content}, true, nil
}

// Pending returns queued events not yet marked accepted, oldest first.
// Rejected rows remain pending so callers can inspect or retry them.
func (s *Store) Pending() ([]PendingRow, error) {
	rows, err := s.db.Query(
		`SELECT id, occurred_at, event_type, actor, content, refs, local_created, last_error
		 FROM local_events WHERE synced_at IS NULL ORDER BY local_created ASC, rowid ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PendingRow
	for rows.Next() {
		var id, occurredAt, eventType, actor, content, refs string
		var localCreated int64
		var lastError sql.NullString
		if err := rows.Scan(&id, &occurredAt, &eventType, &actor, &content, &refs, &localCreated, &lastError); err != nil {
			return nil, err
		}
		occurred, err := time.Parse(timeLayout, occurredAt)
		if err != nil {
			return nil, fmt.Errorf("local: parse occurred_at for %s: %w", id, err)
		}
		out = append(out, PendingRow{
			Event: event.Event{
				ID: id, OccurredAt: occurred, EventType: eventType, Actor: actor,
				Content: json.RawMessage(content), Refs: json.RawMessage(refs),
			},
			LocalCreated: decodeLocalCreated(localCreated),
			LastError:    lastError.String,
		})
	}
	return out, rows.Err()
}

// MarkAccepted records that id was accepted by the canonical store at
// remoteSequence, takes it out of Pending, and reclaims any blob bytes that
// were retained only for retrying the upload.
func (s *Store) MarkAccepted(id string, remoteSequence int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(
		`UPDATE local_events SET remote_sequence = ?, synced_at = ?, last_error = NULL WHERE id = ?`,
		remoteSequence, time.Now().UTC().Format(timeLayout), id,
	)
	if err != nil {
		return err
	}
	if err := checkAffected(res, id); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`INSERT INTO local_blob_refs(event_id, sha256, media_type, size_bytes)
		 SELECT event_id, sha256, media_type, size_bytes FROM local_blobs WHERE event_id = ?
		 ON CONFLICT(event_id) DO NOTHING`, id,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM local_blobs WHERE event_id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// RemoteSequence returns the canonical sequence recorded for id, if it has one.
func (s *Store) RemoteSequence(id string) (int64, bool, error) {
	var sequence sql.NullInt64
	err := s.db.QueryRow(`SELECT remote_sequence FROM local_events WHERE id = ?`, id).Scan(&sequence)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return sequence.Int64, sequence.Valid, nil
}

// MarkRejected records a rejection reason for id without removing it from
// Pending, so it stays visible for retry or inspection. It only applies to
// rows still pending (synced_at IS NULL); an already-accepted id is left
// untouched and reported as an error, since rewriting last_error on an
// accepted row would contradict Pending's accepted-rows-excluded contract.
func (s *Store) MarkRejected(id string, reason string) error {
	res, err := s.db.Exec(`UPDATE local_events SET last_error = ? WHERE id = ? AND synced_at IS NULL`, reason, id)
	if err != nil {
		return err
	}
	return checkAffected(res, id)
}

func checkAffected(res sql.Result, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("local: unknown event id %q", id)
	}
	return nil
}

// RemoteEvent pairs a canonical event with the sequence PostgreSQL
// assigned it, for applying into the local replica during a pull.
type RemoteEvent struct {
	Event    event.Event
	Sequence int64
}

// ApplyRemoteBatch applies rows to the local replica and advances the
// cursor to newCursor in one transaction, so a crash mid-batch leaves the
// cursor unmoved and the next pull re-fetches the same range rather than
// silently skipping or duplicating it. A row whose ID is already known
// locally (e.g. an event this node originally pushed) only has its sync
// bookkeeping backfilled. Canonical actor identity is authoritative, so a
// self-pushed row may have its caller-supplied actor replaced with the
// canonical actor; ApplyRemoteBatch never overwrites stored content. A same-ID
// row with different occurred_at, event type, content, or refs is a
// data-integrity collision: it aborts and rolls back the entire batch,
// including the cursor advance, rather than silently discarding local content
// or canonical truth.
func (s *Store) ApplyRemoteBatch(rows []RemoteEvent, newCursor int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(timeLayout)
	for _, r := range rows {
		if err := r.Event.Validate(); err != nil {
			return fmt.Errorf("local: applying remote event %s: %w", r.Event.ID, err)
		}

		var occurredAt, eventType, content, refs string
		err := tx.QueryRow(
			`SELECT occurred_at, event_type, content, refs FROM local_events WHERE id = ?`, r.Event.ID,
		).Scan(&occurredAt, &eventType, &content, &refs)
		switch {
		case err == sql.ErrNoRows:
			_, err = tx.Exec(
				`INSERT INTO local_events(id, occurred_at, event_type, actor, content, refs, local_created, remote_sequence, synced_at)
				 VALUES (?,?,?,?,?,?,?,?,?)`,
				r.Event.ID, r.Event.OccurredAt.UTC().Format(timeLayout), r.Event.EventType, r.Event.Actor,
				string(r.Event.Content), string(r.Event.Refs), encodeLocalCreated(time.Now()),
				r.Sequence, now,
			)
			if err != nil {
				return fmt.Errorf("local: applying remote event %s: %w", r.Event.ID, err)
			}
		case err != nil:
			return fmt.Errorf("local: applying remote event %s: %w", r.Event.ID, err)
		default:
			contentEqual, err := jsonEqual(content, string(r.Event.Content))
			if err != nil {
				return fmt.Errorf("local: applying remote event %s: comparing content: %w", r.Event.ID, err)
			}
			refsEqual, err := jsonEqual(refs, string(r.Event.Refs))
			if err != nil {
				return fmt.Errorf("local: applying remote event %s: comparing refs: %w", r.Event.ID, err)
			}
			storedOccurredAt, err := time.Parse(timeLayout, occurredAt)
			if err != nil {
				return fmt.Errorf("local: applying remote event %s: parse stored occurred_at: %w", r.Event.ID, err)
			}
			// PostgreSQL TIMESTAMPTZ has microsecond resolution, so a
			// self-pushed event pulled back canonical loses sub-microsecond
			// precision the local row still carries. Truncate both sides
			// the same way before comparing so that round trip isn't
			// misreported as a payload collision.
			sameOccurredAt := storedOccurredAt.Truncate(time.Microsecond).Equal(r.Event.OccurredAt.UTC().Truncate(time.Microsecond))
			same := sameOccurredAt &&
				eventType == r.Event.EventType && contentEqual && refsEqual
			if !same {
				return fmt.Errorf("local: applying remote event %s: local row has different occurred_at/event_type/content/refs than canonical", r.Event.ID)
			}
			if _, err := tx.Exec(
				`UPDATE local_events SET actor = ?, remote_sequence = ?, synced_at = ? WHERE id = ? AND synced_at IS NULL`,
				r.Event.Actor, r.Sequence, now, r.Event.ID,
			); err != nil {
				return fmt.Errorf("local: applying remote event %s: %w", r.Event.ID, err)
			}
		}
	}
	if _, err := tx.Exec(
		`INSERT INTO local_cursor(id, sequence) VALUES (1, ?)
		 ON CONFLICT(id) DO UPDATE SET sequence = excluded.sequence`,
		newCursor,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// Cursor returns the last remote sequence pulled into the local replica,
// or 0 if nothing has been pulled yet.
func (s *Store) Cursor() (int64, error) {
	var seq int64
	err := s.db.QueryRow(`SELECT sequence FROM local_cursor WHERE id = 1`).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return seq, err
}

// SetCursor advances the last remote sequence pulled into the local
// replica.
func (s *Store) SetCursor(seq int64) error {
	_, err := s.db.Exec(
		`INSERT INTO local_cursor(id, sequence) VALUES (1, ?)
		 ON CONFLICT(id) DO UPDATE SET sequence = excluded.sequence`,
		seq,
	)
	return err
}
