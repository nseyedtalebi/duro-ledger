// Package blobconfig validates and opens one configured canonical blob
// backend without coupling the blob interface to PostgreSQL.
package blobconfig

import (
	"fmt"
	"path/filepath"

	"github.com/nseyedtalebi/duro-ledger/pkg/blob"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

const (
	PostgresKind   = "postgres"
	FilesystemKind = "filesystem"
)

// Validate checks an explicit canonical blob backend selection before a CLI
// or service reads protected connection configuration.
func Validate(kind, root string) error {
	switch kind {
	case PostgresKind:
		if root != "" {
			return fmt.Errorf("blob: --blob-root is only valid with --blob-store=%s", FilesystemKind)
		}
		return nil
	case FilesystemKind:
		if root == "" || !filepath.IsAbs(root) {
			return fmt.Errorf("blob: --blob-root must be an absolute path with --blob-store=%s", FilesystemKind)
		}
		return nil
	default:
		return fmt.Errorf("blob: unknown blob store %q", kind)
	}
}

// Open opens the validated canonical blob backend for canonical PostgreSQL
// store. Filesystem roots are created by the existing CAS implementation.
func Open(kind, root string, canonical *postgres.Store) (blob.Store, error) {
	if err := Validate(kind, root); err != nil {
		return nil, err
	}
	if canonical == nil {
		return nil, fmt.Errorf("blob: canonical PostgreSQL store is required")
	}
	if kind == PostgresKind {
		return postgres.NewBlobStore(canonical), nil
	}
	return blob.NewFilesystemStore(root)
}
