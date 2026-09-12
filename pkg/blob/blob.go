// Package blob defines the small seam between Duro's canonical event
// authority (always PostgreSQL) and where canonical blob bytes physically
// live. PostgreSQL and the existing content-addressed filesystem (pkg/cas)
// are the two backends this change supports.
package blob

import "errors"

// ErrNotFound is returned by Store.Read when no blob is stored under digest.
var ErrNotFound = errors.New("blob: not found")

// Store durably persists blob bytes addressed by their SHA-256 digest and
// returns a bounded, verified read of the same bytes. It intentionally has
// no delete, listing, URI, or streaming methods, and no factory/registry:
// only what current callers need.
type Store interface {
	// Put durably stores content under digest, the lowercase 64-hex
	// SHA-256 digest the caller already computed and verified locally.
	// Put must reject content that does not hash to digest rather than
	// storing it anyway, and must leave any existing blob at digest
	// untouched (dedup, not overwrite).
	Put(digest string, content []byte) error
	// Read returns the bytes stored at digest, verified against digest
	// and bounded by maxBytes. It returns ErrNotFound if no blob is
	// stored at digest.
	Read(digest string, maxBytes int64) ([]byte, error)
	// Verify checks a stored blob's digest and exact size without returning
	// its bytes. Filesystem implementations stream this check.
	Verify(digest string, size int64) error
	// Kind identifies the backend, e.g. "filesystem" or "postgres".
	Kind() string
}
