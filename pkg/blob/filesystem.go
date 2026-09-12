package blob

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nseyedtalebi/duro-ledger/pkg/cas"
)

// FilesystemStore is a Store backed by pkg/cas, Duro's existing flat
// content-addressed filesystem layout. It adapts cas.Store rather than
// duplicating its sharding, hashing, temp-file, or verification logic.
type FilesystemStore struct {
	cas *cas.Store
}

// NewFilesystemStore opens a filesystem-backed Store rooted at root. root
// must already be an absolute path; that requirement is enforced at the
// CLI/service configuration boundary, not here.
func NewFilesystemStore(root string) (*FilesystemStore, error) {
	store, err := cas.Open(root)
	if err != nil {
		return nil, err
	}
	return &FilesystemStore{cas: store}, nil
}

// Put stores content at its sharded CAS path, rejecting it if it does not
// hash to digest.
func (f *FilesystemStore) Put(digest string, content []byte) error {
	_, _, err := f.cas.WriteExpected(bytes.NewReader(content), digest)
	return err
}

// PutFile streams a source file into the filesystem CAS and returns its
// verified content identity without loading the artifact into memory.
func (f *FilesystemStore) PutFile(path string) (digest string, size int64, err error) {
	digest, size, _, err = f.cas.WriteFile(path)
	return digest, size, err
}

// Verify streams a stored blob through its digest without loading it into
// memory, before an event is allowed to reference it canonically.
func (f *FilesystemStore) Verify(digest string, size int64) error {
	return f.cas.Verify(digest, size)
}

// Read returns content verified against digest and bounded by maxBytes.
func (f *FilesystemStore) Read(digest string, maxBytes int64) ([]byte, error) {
	content, err := f.cas.ReadVerified(digest, maxBytes)
	if errors.Is(err, cas.ErrNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, digest)
	}
	return content, err
}

// Kind identifies this backend as "filesystem".
func (f *FilesystemStore) Kind() string { return "filesystem" }
