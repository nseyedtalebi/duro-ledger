// Package postgres is Duro's canonical event store: PostgreSQL is the only
// authority for accepted events. It assigns each accepted event a
// monotonically increasing sequence and never overwrites an existing row.
package postgres

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	_ "embed"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nseyedtalebi/duro-ledger/pkg/event"
)

//go:embed schema.sql
var schema string

// Outcome is the result of attempting to insert an event.
type Outcome int

const (
	// Rejected means the event failed validation and was never attempted
	// against canonical storage.
	Rejected Outcome = iota
	// Accepted means a new canonical row was created.
	Accepted
	// AlreadyPresent means an event with this ID already exists with the
	// same logical payload; nothing was written.
	AlreadyPresent
	// Conflict means an event with this ID already exists with a
	// different logical payload; nothing was written or overwritten.
	Conflict
)

func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case AlreadyPresent:
		return "already_present"
	case Conflict:
		return "conflict"
	case Rejected:
		return "rejected"
	default:
		return fmt.Sprintf("outcome(%d)", int(o))
	}
}

// Store is the PostgreSQL-backed canonical event store.
type Store struct {
	db *sql.DB
}

// ValidationError wraps a boundary validation failure from event.Validate.
// Insert returns Rejected for both a validation failure and an operational
// failure encountered while classifying a retried ID; ValidationError lets
// callers tell the two apart instead of retrying a request that will never
// succeed.
type ValidationError struct {
	Err error
}

func (e *ValidationError) Error() string { return e.Err.Error() }
func (e *ValidationError) Unwrap() error { return e.Err }

// Open connects to an initialized canonical PostgreSQL store. It does not run
// DDL: ordinary ingest credentials must not need schema-mutation privileges.
func Open(dsn string) (*Store, error) {
	db, err := openDB(dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Initialize applies Duro's idempotent canonical schema using an
// administrator/provisioning connection. Run it before handing a DSN to an
// ordinary ingest client.
func Initialize(dsn string) error {
	db, err := openDB(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	return nil
}

// openDB connects to the canonical store selected by the deployment. Duro
// authenticates event actors from PostgreSQL's current role; whether transport
// encryption is required belongs to the private-network or loopback deployment
// boundary, not the generic ledger client.
func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error { return s.db.Close() }

// transactionActor is the actor the schema trigger binds to every newly
// accepted event in this transaction. Retry classification must compare this
// authority, not the caller-supplied Event.Actor that the trigger replaced.
func transactionActor(tx *sql.Tx) (string, error) {
	var actor string
	if err := tx.QueryRow(`SELECT current_user`).Scan(&actor); err != nil {
		return "", err
	}
	return actor, nil
}

// Insert attempts to add ev to canonical storage inside its own
// transaction. A new ID is Accepted and assigned a sequence. A retried ID
// with an identical logical payload (occurred_at, event_type, actor,
// content, refs) is AlreadyPresent, returning the original sequence. A
// retried ID with a different payload is a Conflict, and the original row
// is left untouched. An event failing boundary validation is Rejected
// before any database access.
//
// The insert itself is INSERT ... ON CONFLICT (id) DO NOTHING RETURNING
// sequence rather than SELECT-then-INSERT: the latter leaves a window
// where two concurrent Insert calls for the same new ID both see no row
// and both attempt to insert, so one gets an unhandled unique-violation
// error instead of a clean AlreadyPresent/Conflict outcome. The
// ON CONFLICT form makes "did I win the race" atomic; only the loser
// falls through to read back the row the winner (or an earlier caller)
// committed, to classify it as AlreadyPresent or Conflict.
func (s *Store) Insert(ev event.Event) (Outcome, int64, error) {
	if err := ev.Validate(); err != nil {
		return Rejected, 0, &ValidationError{Err: err}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return Rejected, 0, err
	}
	defer tx.Rollback()

	var seq int64
	err = tx.QueryRow(
		`INSERT INTO events(id, occurred_at, event_type, actor, content, refs)
		 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO NOTHING RETURNING sequence`,
		ev.ID, ev.OccurredAt.UTC(), ev.EventType, ev.Actor, []byte(ev.Content), []byte(ev.Refs),
	).Scan(&seq)
	switch {
	case err == nil:
		if err := tx.Commit(); err != nil {
			return Rejected, 0, err
		}
		return Accepted, seq, nil
	case err != sql.ErrNoRows:
		return Rejected, 0, err
	}

	// The insert was a no-op: a row with this id already exists. Read it
	// back to classify the retry as AlreadyPresent or Conflict.
	var existingSeq int64
	var occurredAt time.Time
	var eventType, actor string
	var content, refs []byte
	if err := tx.QueryRow(
		`SELECT sequence, occurred_at, event_type, actor, content, refs FROM events WHERE id = $1`, ev.ID,
	).Scan(&existingSeq, &occurredAt, &eventType, &actor, &content, &refs); err != nil {
		return Rejected, 0, err
	}

	contentEqual, err := jsonEqual(string(content), string(ev.Content))
	if err != nil {
		return Rejected, 0, fmt.Errorf("postgres: comparing content for %s: %w", ev.ID, err)
	}
	refsEqual, err := jsonEqual(string(refs), string(ev.Refs))
	if err != nil {
		return Rejected, 0, fmt.Errorf("postgres: comparing refs for %s: %w", ev.ID, err)
	}
	// TIMESTAMPTZ has microsecond resolution; truncate the submitted
	// value the same way before comparing so a full-nanosecond retry
	// of the exact same event isn't misreported as a conflict.
	sameOccurredAt := occurredAt.Equal(ev.OccurredAt.UTC().Truncate(time.Microsecond))
	canonicalActor, err := transactionActor(tx)
	if err != nil {
		return Rejected, 0, err
	}
	if sameOccurredAt && eventType == ev.EventType && actor == canonicalActor && contentEqual && refsEqual {
		return AlreadyPresent, existingSeq, nil
	}
	return Conflict, existingSeq, nil
}

// InsertWithBlob is Insert, but also stores blobContent as a canonical
// blob and links it to ev, atomically in the same transaction: either the
// event and its blob are both committed, or neither is. Blobs are
// deduplicated by their own SHA-256 digest, so two events whose blob bytes
// are identical share one row instead of storing the bytes twice.
//
// wantDigest is the digest the caller recorded when the bytes were staged
// locally (see local.Store.PendingBlob). InsertWithBlob recomputes the
// digest from blobContent itself and rejects the call, before writing
// anything, if the two disagree -- that mismatch means the bytes were
// corrupted somewhere between local staging and this call, and the
// canonical store must not vouch for content it can't verify.
//
// As with Insert, a retried event ID with an identical logical payload
// (including the same blob digest and media type) is AlreadyPresent; a retried ID whose
// blob digest, media type, or event payload differs is a Conflict, and the original
// row and blob link are left untouched.
func (s *Store) InsertWithBlob(ev event.Event, blobContent []byte, wantDigest, mediaType string) (Outcome, int64, error) {
	if err := ev.Validate(); err != nil {
		return Rejected, 0, &ValidationError{Err: err}
	}
	sum := sha256.Sum256(blobContent)
	digest := sum[:]
	storedContent := blobContent
	if storedContent == nil {
		storedContent = []byte{}
	}
	if wantDigest != "" && hex.EncodeToString(digest) != wantDigest {
		return Rejected, 0, &ValidationError{Err: fmt.Errorf("postgres: blob digest mismatch: want %s, computed %s", wantDigest, hex.EncodeToString(digest))}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return Rejected, 0, err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`INSERT INTO blobs(id, sha256, media_type, size_bytes, content) VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (sha256) DO NOTHING`,
		uuid.NewString(), digest, mediaType, len(storedContent), storedContent,
	); err != nil {
		return Rejected, 0, err
	}

	return insertEventLinkedToBlob(tx, ev, digest, mediaType)
}

// InsertWithBlobRef is InsertWithBlob for a blob whose durable bytes already
// live in an external backend (e.g. pkg/blob's filesystem store) rather
// than in PostgreSQL: it links ev to the blob by digest/size/mediaType and,
// if no blobs row for this digest exists yet, creates one with content
// NULL rather than duplicating the bytes into PostgreSQL. As with
// InsertWithBlob, the caller must have already verified digest against the
// bytes themselves -- InsertWithBlobRef has no content to recompute it
// from, so it validates digest's shape only, not its truth.
func (s *Store) InsertWithBlobRef(ev event.Event, digest string, size int64, mediaType string) (Outcome, int64, error) {
	if err := ev.Validate(); err != nil {
		return Rejected, 0, &ValidationError{Err: err}
	}
	if len(digest) != sha256.Size*2 {
		return Rejected, 0, &ValidationError{Err: fmt.Errorf("postgres: SHA-256 digest must be %d hex characters", sha256.Size*2)}
	}
	digestBytes, err := hex.DecodeString(digest)
	if err != nil {
		return Rejected, 0, &ValidationError{Err: fmt.Errorf("postgres: invalid SHA-256 digest %q", digest)}
	}
	if hex.EncodeToString(digestBytes) != digest {
		return Rejected, 0, &ValidationError{Err: fmt.Errorf("postgres: SHA-256 digest must be lowercase hex")}
	}
	if size < 0 {
		return Rejected, 0, &ValidationError{Err: fmt.Errorf("postgres: blob size must not be negative")}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return Rejected, 0, err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`INSERT INTO blobs(id, sha256, media_type, size_bytes, content) VALUES ($1,$2,$3,$4,NULL)
		 ON CONFLICT (sha256) DO NOTHING`,
		uuid.NewString(), digestBytes, mediaType, size,
	); err != nil {
		return Rejected, 0, err
	}
	var storedSize int64
	if err := tx.QueryRow(`SELECT size_bytes FROM blobs WHERE sha256 = $1`, digestBytes).Scan(&storedSize); err != nil {
		return Rejected, 0, err
	}
	if storedSize != size {
		return Rejected, 0, &ValidationError{Err: fmt.Errorf("postgres: blob %s stored size %d, submitted size %d", digest, storedSize, size)}
	}

	return insertEventLinkedToBlob(tx, ev, digestBytes, mediaType)
}

// insertEventLinkedToBlob inserts ev linked to the blob identified by
// digest/mediaType, or classifies a retried id as AlreadyPresent/Conflict
// against the row already there. It assumes the caller has already
// upserted (or confirmed the existence of) the blobs row for digest in the
// same transaction.
func insertEventLinkedToBlob(tx *sql.Tx, ev event.Event, digest []byte, mediaType string) (Outcome, int64, error) {
	var seq int64
	err := tx.QueryRow(
		`INSERT INTO events(id, occurred_at, event_type, actor, content, refs, blob_sha256, blob_media_type)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING RETURNING sequence`,
		ev.ID, ev.OccurredAt.UTC(), ev.EventType, ev.Actor, []byte(ev.Content), []byte(ev.Refs), digest, mediaType,
	).Scan(&seq)
	switch {
	case err == nil:
		if err := tx.Commit(); err != nil {
			return Rejected, 0, err
		}
		return Accepted, seq, nil
	case err != sql.ErrNoRows:
		return Rejected, 0, err
	}

	// The insert was a no-op: a row with this id already exists. Read it
	// back to classify the retry as AlreadyPresent or Conflict, comparing
	// the blob link alongside the rest of the logical payload.
	var existingSeq int64
	var occurredAt time.Time
	var eventType, actor string
	var content, refs, existingDigest []byte
	var existingMediaType sql.NullString
	if err := tx.QueryRow(
		`SELECT e.sequence, e.occurred_at, e.event_type, e.actor, e.content, e.refs, e.blob_sha256,
		        COALESCE(e.blob_media_type, b.media_type)
		 FROM events e LEFT JOIN blobs b ON b.sha256 = e.blob_sha256 WHERE e.id = $1`, ev.ID,
	).Scan(&existingSeq, &occurredAt, &eventType, &actor, &content, &refs, &existingDigest, &existingMediaType); err != nil {
		return Rejected, 0, err
	}

	contentEqual, err := jsonEqual(string(content), string(ev.Content))
	if err != nil {
		return Rejected, 0, fmt.Errorf("postgres: comparing content for %s: %w", ev.ID, err)
	}
	refsEqual, err := jsonEqual(string(refs), string(ev.Refs))
	if err != nil {
		return Rejected, 0, fmt.Errorf("postgres: comparing refs for %s: %w", ev.ID, err)
	}
	sameOccurredAt := occurredAt.Equal(ev.OccurredAt.UTC().Truncate(time.Microsecond))
	sameBlob := bytes.Equal(existingDigest, digest) && existingMediaType.String == mediaType
	canonicalActor, err := transactionActor(tx)
	if err != nil {
		return Rejected, 0, err
	}
	if sameOccurredAt && eventType == ev.EventType && actor == canonicalActor && contentEqual && refsEqual && sameBlob {
		return AlreadyPresent, existingSeq, nil
	}
	return Conflict, existingSeq, nil
}

// Blob is a canonical opaque body identified by its SHA-256 digest.
type Blob struct {
	SHA256    string
	MediaType string
	Size      int64
	Content   []byte
}

// Document is the newest source-addressed document.filed event together with
// its verified canonical text bytes.
type Document struct {
	Sequence   int64
	EventID    string
	Source     string
	BlobSHA256 string
	MediaType  string
	Body       []byte
}

// PulledEvent pairs a canonical event with the sequence it was assigned and,
// when present, the SHA-256 digest of its canonical body blob.
type PulledEvent struct {
	Sequence   int64
	Event      event.Event
	BlobSHA256 string
}

// ReadBlob returns a canonical blob by digest. It verifies both the stored
// size and digest before returning bytes so a projector never indexes content
// whose canonical association it can no longer verify.
func (s *Store) ReadBlob(digestText string) (Blob, bool, error) {
	return s.readBlob(digestText, 0)
}

// ReadBlobLimited is ReadBlob with a byte ceiling checked before blob content
// is read from PostgreSQL.
func (s *Store) ReadBlobLimited(digestText string, maxBytes int64) (Blob, bool, error) {
	if maxBytes <= 0 {
		return Blob{}, false, fmt.Errorf("postgres: max bytes must be positive")
	}
	return s.readBlob(digestText, maxBytes)
}

func (s *Store) readBlob(digestText string, maxBytes int64) (Blob, bool, error) {
	if len(digestText) != sha256.Size*2 {
		return Blob{}, false, fmt.Errorf("postgres: SHA-256 digest must be %d hex characters", sha256.Size*2)
	}
	digest, err := hex.DecodeString(digestText)
	if err != nil || len(digest) != sha256.Size {
		return Blob{}, false, fmt.Errorf("postgres: invalid SHA-256 digest %q", digestText)
	}

	var storedDigest []byte
	var mediaType sql.NullString
	var blob Blob
	err = s.db.QueryRow(`SELECT sha256, media_type, size_bytes FROM blobs WHERE sha256 = $1`, digest).Scan(
		&storedDigest, &mediaType, &blob.Size,
	)
	if err == sql.ErrNoRows {
		return Blob{}, false, nil
	}
	if err != nil {
		return Blob{}, false, err
	}
	if maxBytes > 0 && blob.Size > maxBytes {
		return Blob{}, false, fmt.Errorf("postgres: blob %s exceeds %d-byte read limit", digestText, maxBytes)
	}
	if err := s.db.QueryRow(`SELECT content FROM blobs WHERE sha256 = $1`, digest).Scan(&blob.Content); err != nil {
		return Blob{}, false, err
	}
	if blob.Size != int64(len(blob.Content)) {
		return Blob{}, false, fmt.Errorf("postgres: blob %s size mismatch: stored %d, content %d", digestText, blob.Size, len(blob.Content))
	}
	sum := sha256.Sum256(blob.Content)
	if !bytes.Equal(storedDigest, sum[:]) {
		return Blob{}, false, fmt.Errorf("postgres: blob %s digest mismatch", digestText)
	}
	blob.SHA256 = hex.EncodeToString(storedDigest)
	blob.MediaType = mediaType.String
	return blob, true, nil
}

// ReadLatestDocumentMetadata returns provenance for the most recently accepted
// document.filed event for source without reading blob bytes. Source is opaque
// to Duro; choosing the highest sequence is the caller-visible "newest source
// wins" projection rule.
func (s *Store) ReadLatestDocumentMetadata(source string) (Document, bool, error) {
	if source == "" {
		return Document{}, false, fmt.Errorf("postgres: source is required")
	}
	var doc Document
	var digest []byte
	err := s.db.QueryRow(
		`SELECT e.sequence, e.id, e.content->>'source', e.blob_sha256,
		        COALESCE(e.blob_media_type, b.media_type)
		 FROM events e JOIN blobs b ON b.sha256 = e.blob_sha256
		 WHERE e.event_type = 'document.filed'
		   AND e.content->>'source' = $1
		 ORDER BY e.sequence DESC LIMIT 1`,
		source,
	).Scan(&doc.Sequence, &doc.EventID, &doc.Source, &digest, &doc.MediaType)
	if err == sql.ErrNoRows {
		return Document{}, false, nil
	}
	if err != nil {
		return Document{}, false, err
	}
	doc.BlobSHA256 = hex.EncodeToString(digest)
	return doc, true, nil
}

// ReadLatestDocument returns the most recently accepted document.filed event
// together with its verified PostgreSQL-backed body. Filesystem-backed
// callers use ReadLatestDocumentMetadata and their configured blob backend.
func (s *Store) ReadLatestDocument(source string, maxBytes int64) (Document, bool, error) {
	if maxBytes <= 0 {
		return Document{}, false, fmt.Errorf("postgres: max bytes must be positive")
	}
	doc, ok, err := s.ReadLatestDocumentMetadata(source)
	if err != nil || !ok {
		return Document{}, ok, err
	}
	blob, ok, err := s.ReadBlobLimited(doc.BlobSHA256, maxBytes)
	if err != nil || !ok {
		return Document{}, ok, err
	}
	doc.Body = blob.Content
	return doc, true, nil
}

// maxDocumentListLimit keeps a list page bounded even when a caller supplies
// its own page size. Larger traversals advance the exclusive sequence cursor.
const maxDocumentListLimit = 500

func validateDocumentListRequest(after int64, limit int) error {
	if after < 0 {
		return fmt.Errorf("postgres: after sequence must not be negative")
	}
	if limit <= 0 || limit > maxDocumentListLimit {
		return fmt.Errorf("postgres: limit must be between 1 and %d", maxDocumentListLimit)
	}
	return nil
}

// ListLatestDocuments returns the current (highest-sequence) document.filed
// event for each source whose canonical sequence is strictly after after. It
// returns metadata and canonical provenance only; callers read the body through
// ReadLatestDocument so list pages never load unbounded document bytes.
func (s *Store) ListLatestDocuments(after int64, limit int) ([]Document, error) {
	if err := validateDocumentListRequest(after, limit); err != nil {
		return nil, err
	}

	rows, err := s.db.Query(
		`WITH current_documents AS (
			SELECT DISTINCT ON (content->>'source') sequence, id, content->>'source' AS source, blob_sha256, blob_media_type
			FROM events
			WHERE event_type = 'document.filed'
			  AND blob_sha256 IS NOT NULL
			  AND content->>'source' IS NOT NULL
			  AND content->>'source' <> ''
			ORDER BY content->>'source', sequence DESC
		)
		SELECT d.sequence, d.id, d.source, d.blob_sha256, COALESCE(d.blob_media_type, b.media_type)
		FROM current_documents d
		JOIN blobs b ON b.sha256 = d.blob_sha256
		WHERE d.sequence > $1
		ORDER BY d.sequence ASC
		LIMIT $2`,
		after, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var documents []Document
	for rows.Next() {
		var doc Document
		var digest []byte
		var mediaType sql.NullString
		if err := rows.Scan(&doc.Sequence, &doc.EventID, &doc.Source, &digest, &mediaType); err != nil {
			return nil, err
		}
		doc.BlobSHA256 = hex.EncodeToString(digest)
		doc.MediaType = mediaType.String
		documents = append(documents, doc)
	}
	return documents, rows.Err()
}

// ListEventsByTypeAndScope returns one bounded, ascending canonical page for
// explicit event types and an exact deployment scope. It is deliberately
// separate from Pull so scoped projectors never widen a generic replica pull.
func (s *Store) ListEventsByTypeAndScope(types []string, deploymentScope string, after int64, limit int) ([]PulledEvent, error) {
	if len(types) == 0 || deploymentScope == "" || after < 0 || limit <= 0 || limit > maxDocumentListLimit {
		return nil, fmt.Errorf("postgres: invalid scoped event query")
	}
	seen := make(map[string]struct{}, len(types))
	for _, eventType := range types {
		if eventType == "" {
			return nil, fmt.Errorf("postgres: invalid scoped event query")
		}
		if _, ok := seen[eventType]; ok {
			return nil, fmt.Errorf("postgres: invalid scoped event query")
		}
		seen[eventType] = struct{}{}
	}
	rows, err := s.db.Query(
		`SELECT sequence, id, occurred_at, event_type, actor, content, refs, blob_sha256
		 FROM events
		 WHERE event_type = ANY($1::text[]) AND content->>'deployment_scope' = $2 AND sequence > $3
		 ORDER BY sequence ASC LIMIT $4`, types, deploymentScope, after, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PulledEvent
	for rows.Next() {
		var row PulledEvent
		var id, eventType, actor string
		var occurredAt time.Time
		var content, refs, blobDigest []byte
		if err := rows.Scan(&row.Sequence, &id, &occurredAt, &eventType, &actor, &content, &refs, &blobDigest); err != nil {
			return nil, err
		}
		row.BlobSHA256 = hex.EncodeToString(blobDigest)
		row.Event = event.Event{ID: id, OccurredAt: occurredAt, EventType: eventType, Actor: actor, Content: json.RawMessage(content), Refs: json.RawMessage(refs)}
		out = append(out, row)
	}
	return out, rows.Err()
}

// Pull returns up to limit canonical events with sequence > after, ordered
// ascending by sequence. Sequence gaps (from rolled-back inserts, deletes,
// or concurrent writers) are allowed and never backfilled.
func (s *Store) Pull(after int64, limit int) ([]PulledEvent, error) {
	rows, err := s.db.Query(
		`SELECT sequence, id, occurred_at, event_type, actor, content, refs, blob_sha256
		 FROM events WHERE sequence > $1 ORDER BY sequence ASC LIMIT $2`,
		after, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PulledEvent
	for rows.Next() {
		var seq int64
		var id, eventType, actor string
		var occurredAt time.Time
		var content, refs, blobDigest []byte
		if err := rows.Scan(&seq, &id, &occurredAt, &eventType, &actor, &content, &refs, &blobDigest); err != nil {
			return nil, err
		}
		out = append(out, PulledEvent{
			Sequence:   seq,
			BlobSHA256: hex.EncodeToString(blobDigest),
			Event: event.Event{
				ID: id, OccurredAt: occurredAt, EventType: eventType, Actor: actor,
				Content: json.RawMessage(content), Refs: json.RawMessage(refs),
			},
		})
	}
	return out, rows.Err()
}

// jsonEqual reports whether a and b are the same JSON object modulo key
// order and insignificant whitespace, mirroring pkg/local's comparison so
// "same logical payload" means the same thing on both sides of a sync.
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
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing data after JSON value")
		}
		return nil, err
	}
	return v, nil
}
