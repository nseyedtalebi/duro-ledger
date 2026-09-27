package postgres

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/nseyedtalebi/duro-ledger/pkg/cas"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
)

// Locator is one cataloged observation of where a blob's bytes were seen
// physically, joined to the blob's canonical identity.
type Locator struct {
	SHA256         string     `json:"sha256"`
	Size           int64      `json:"size_bytes"`
	Locator        string     `json:"locator"`
	ObservedAt     time.Time  `json:"observed_at"`
	LastVerifiedAt *time.Time `json:"last_verified_at,omitempty"`
}

// decodeDigest accepts only the exact spelling Duro catalogs digests in --
// 64 lowercase hex characters -- so two textually different digests can never
// name the same blob. cas.ValidDigest is the single spelling rule.
func decodeDigest(digestText string) ([]byte, error) {
	if !cas.ValidDigest(digestText) {
		return nil, fmt.Errorf("postgres: SHA-256 digest must be %d lowercase hex characters, got %q", 2*32, digestText)
	}
	return hex.DecodeString(digestText)
}

// escapeLikeLiteral makes s match itself under LIKE ... ESCAPE '\': %, _ and
// backslash lose their pattern meaning. Callers append their own '%' after
// escaping, so a caller-supplied prefix can never widen the match.
func escapeLikeLiteral(s string) string {
	return strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(s)
}

// CatalogArtifact records metadata for a blob whose bytes already live in an
// external content-addressed store, plus the source locators they were
// observed at, in one transaction: either all of it lands or none does.
//
// digest/size are immutable content identity. A digest already cataloged with
// a different size is a conflict returned as an error, never an overwrite.
// verifiedAt is recorded when the caller has just streamed the bytes through
// their digest itself; the zero time leaves any existing verification alone.
//
// A repeated locator for the same digest is idempotent but refreshes its
// observation time -- "seen here again now" is the fact being recorded.
// fresh reports whether this call created the blobs row.
func (s *Store) CatalogArtifact(digestText string, size int64, locators []string, verifiedAt time.Time) (fresh bool, err error) {
	digest, err := decodeDigest(digestText)
	if err != nil {
		return false, &ValidationError{Err: err}
	}
	if size < 0 {
		return false, &ValidationError{Err: fmt.Errorf("postgres: blob size must not be negative")}
	}
	for _, locator := range locators {
		if err := event.ValidateAbsoluteURI(locator); err != nil {
			return false, &ValidationError{Err: fmt.Errorf("postgres: locator must be an absolute URI: %w", err)}
		}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	res, err := tx.Exec(
		`INSERT INTO blobs(id, sha256, size_bytes, content) VALUES ($1,$2,$3,NULL)
		 ON CONFLICT (sha256) DO NOTHING`,
		uuid.NewString(), digest, size,
	)
	if err != nil {
		return false, err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	var storedSize int64
	if err := tx.QueryRow(`SELECT size_bytes FROM blobs WHERE sha256 = $1`, digest).Scan(&storedSize); err != nil {
		return false, err
	}
	if storedSize != size {
		return false, &ValidationError{Err: fmt.Errorf("postgres: blob %s is cataloged with size %d, submitted size %d", digestText, storedSize, size)}
	}
	if !verifiedAt.IsZero() {
		if _, err := tx.Exec(`UPDATE blobs SET last_verified_at = $2 WHERE sha256 = $1`, digest, verifiedAt.UTC()); err != nil {
			return false, err
		}
	}

	for _, locator := range locators {
		if _, err := tx.Exec(
			`INSERT INTO blob_locator_observations(blob_sha256, locator, observed_at) VALUES ($1,$2,now())
			 ON CONFLICT (blob_sha256, locator) DO UPDATE SET observed_at = now()`,
			digest, locator,
		); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return inserted == 1, nil
}

// MarkBlobVerified records that exactly one cataloged blob's stored bytes were
// streamed through their digest and found intact. Callers must have completed
// that check successfully first: this writes the timestamp, it does not verify.
func (s *Store) MarkBlobVerified(digestText string, verifiedAt time.Time) error {
	digest, err := decodeDigest(digestText)
	if err != nil {
		return &ValidationError{Err: err}
	}
	res, err := s.db.Exec(`UPDATE blobs SET last_verified_at = $2 WHERE sha256 = $1`, digest, verifiedAt.UTC())
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("postgres: blob %s is not cataloged", digestText)
	}
	return nil
}

// ListLocators returns cataloged locator observations whose locator starts with
// the literal prefix. The prefix is matched literally and bound as a parameter:
// %, _ and backslash in it are ordinary characters, not wildcards. An empty
// prefix returns every observation.
func (s *Store) ListLocators(prefix string) ([]Locator, error) {
	rows, err := s.db.Query(
		`SELECT encode(o.blob_sha256,'hex'), b.size_bytes, o.locator, o.observed_at, b.last_verified_at
		 FROM blob_locator_observations o JOIN blobs b ON b.sha256 = o.blob_sha256
		 WHERE o.locator LIKE $1 ESCAPE '\'
		 ORDER BY o.locator, o.blob_sha256`,
		escapeLikeLiteral(prefix)+`%`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Locator, 0)
	for rows.Next() {
		var loc Locator
		var lastVerifiedAt sql.NullTime
		if err := rows.Scan(&loc.SHA256, &loc.Size, &loc.Locator, &loc.ObservedAt, &lastVerifiedAt); err != nil {
			return nil, err
		}
		if lastVerifiedAt.Valid {
			verified := lastVerifiedAt.Time
			loc.LastVerifiedAt = &verified
		}
		out = append(out, loc)
	}
	return out, rows.Err()
}
