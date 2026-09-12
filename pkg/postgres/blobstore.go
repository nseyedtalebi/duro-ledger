package postgres

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"

	"github.com/nseyedtalebi/duro-ledger/pkg/blob"
)

// BlobStore adapts Store's blobs table to blob.Store, so PostgreSQL itself
// can serve as a durable blob backend (alongside pkg/blob's filesystem
// backend) behind the same interface. It stores content inline in
// PostgreSQL, distinct from the metadata-only rows InsertWithBlobRef
// creates for bytes an external backend already holds.
type BlobStore struct {
	store *Store
}

var _ blob.Store = (*BlobStore)(nil)

// NewBlobStore adapts store as a blob.Store.
func NewBlobStore(store *Store) *BlobStore { return &BlobStore{store: store} }

// Put stores content under digest, deduplicated by sha256 like
// InsertWithBlob. It rejects content that does not hash to digest and
// leaves any existing blob at digest untouched.
func (b *BlobStore) Put(digest string, content []byte) error {
	sum := sha256.Sum256(content)
	if hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("postgres: blob digest mismatch: want %s, computed %x", digest, sum)
	}
	digestBytes, err := hex.DecodeString(digest)
	if err != nil || len(digestBytes) != sha256.Size {
		return fmt.Errorf("postgres: invalid SHA-256 digest %q", digest)
	}
	storedContent := content
	if storedContent == nil {
		storedContent = []byte{}
	}
	_, err = b.store.db.Exec(
		`INSERT INTO blobs(id, sha256, size_bytes, content) VALUES ($1,$2,$3,$4)
		 ON CONFLICT (sha256) DO NOTHING`,
		uuid.NewString(), digestBytes, len(storedContent), storedContent,
	)
	return err
}

// Read returns content stored at digest, verified and bounded by maxBytes.
func (b *BlobStore) Read(digest string, maxBytes int64) ([]byte, error) {
	stored, ok, err := b.store.ReadBlobLimited(digest, maxBytes)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s", blob.ErrNotFound, digest)
	}
	return stored.Content, nil
}

// Verify confirms a blob's persisted bytes and expected size. PostgreSQL is
// not the large-artifact path, so its verified read remains bounded in memory.
func (b *BlobStore) Verify(digest string, size int64) error {
	stored, ok, err := b.store.ReadBlob(digest)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %s", blob.ErrNotFound, digest)
	}
	if stored.Size != size {
		return fmt.Errorf("postgres: blob %s stored size %d, expected %d", digest, stored.Size, size)
	}
	return nil
}

// Kind identifies this backend as "postgres".
func (b *BlobStore) Kind() string { return "postgres" }
