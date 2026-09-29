// Package cas implements a content-addressed blob store: files are written
// under a directory keyed by their SHA-256 digest, deduplicated on write,
// and made read-only once stored. Put and get both use bounded working
// memory per operation, independent of blob size.
package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// ErrInvalidDigest is returned when a caller-supplied digest is not a
// well-formed lowercase 64-hex SHA-256 digest.
var ErrInvalidDigest = errors.New("cas: invalid digest")

// ErrNotFound is returned when no blob is stored under a requested digest.
var ErrNotFound = errors.New("cas: blob not found")

// ErrCorrupt is returned when bytes on disk no longer match their recorded
// size or hash.
var ErrCorrupt = errors.New("cas: stored content failed verification")

// ErrSizeMismatch is returned by WriteFile when the source file's length
// changed between being stat'd and being fully read.
var ErrSizeMismatch = errors.New("cas: source size changed during read")

// ErrDestinationExists is returned by GetToFile when the destination path
// already exists; Get never overwrites an existing destination.
var ErrDestinationExists = errors.New("cas: destination already exists")

// ErrNotRegular is returned when a path this store expects to be immutable
// content-addressed authority (a stored digest path, or a Has/Get target)
// exists but is not a regular file -- e.g. a symlink, which could point at a
// mutable external target and silently break the content-addressing
// guarantee.
var ErrNotRegular = errors.New("cas: path exists but is not a regular file")

// ValidDigest reports whether digest is a well-formed lowercase 64-hex
// SHA-256 digest (no "sha256:" prefix; see ParseIdentity/FormatIdentity for
// the prefixed form artifacts are identified by at the CLI/event boundary).
func ValidDigest(digest string) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	for _, c := range digest {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// FormatIdentity renders a bare hex digest in the contract's artifact
// identity form: sha256:<64 lowercase hex characters>.
func FormatIdentity(digest string) string {
	return "sha256:" + digest
}

// ParseIdentity parses the contract's artifact identity form
// (sha256:<64 lowercase hex characters>) and returns the bare hex digest.
// Any other spelling, including a bare hex digest with no prefix, is
// rejected as malformed.
func ParseIdentity(identity string) (string, error) {
	const prefix = "sha256:"
	if !strings.HasPrefix(identity, prefix) {
		return "", fmt.Errorf("%w: %q must start with %q", ErrInvalidDigest, identity, prefix)
	}
	digest := strings.TrimPrefix(identity, prefix)
	if !ValidDigest(digest) {
		return "", fmt.Errorf("%w: %q", ErrInvalidDigest, identity)
	}
	return digest, nil
}

// Store is a content-addressed store rooted at a directory on disk.
type Store struct {
	root string
	// syncFile fsyncs an open file or directory handle. Overridable only by
	// withSyncFile, an unexported test seam for injecting deterministic
	// sync failures; production code always gets (*os.File).Sync.
	syncFile func(*os.File) error
	// afterStat runs between WriteFile's stat of a source and the read that
	// follows. Unexported test seam: it makes "the source changed during
	// ingestion" deterministic instead of a scheduling race. nil in
	// production.
	afterStat func()
}

// Open prepares (creating if needed) a content-addressed store rooted at
// dir, and returns a Store whose root is the canonical (symlink-resolved)
// absolute path, so later paths cannot shift with the process's working
// directory and name the real directories on disk rather than a link to
// them. The root plus its ancestors are made durable before anything is
// stored inside them.
//
// Durability requires traversal and read access to the store root and every
// ancestor up to the filesystem root: making a directory entry durable means
// opening that directory and fsyncing it, and Open does this for the whole
// chain. A store root under a directory the process may not open (for
// example a mode 0711 home directory in the path) cannot be made durable and
// Open fails rather than reporting a durability it did not establish. This
// applies to reads too: opening a store is a prerequisite of Get, so a get
// against a missing root creates and fsyncs it as a side effect.
func Open(dir string) (*Store, error) {
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{root: root, syncFile: (*os.File).Sync}
	for _, d := range []string{filepath.Join(root, "sha256"), filepath.Join(root, ".tmp")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	// filepath.Abs is purely lexical. If dir or any ancestor is a symlink, the
	// lexical path names the link, and fsyncing it would make the link's own
	// entry durable instead of the directories the blobs are actually written
	// into. Resolve once, after creation, and use the resolved root for every
	// path this Store derives.
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	s.root = canonical
	for _, d := range []string{filepath.Join(canonical, "sha256"), filepath.Join(canonical, ".tmp")} {
		// "" means sync every ancestor up to the filesystem root: the store
		// root's own directory entry has to survive a crash too.
		if err := s.syncChain(d, ""); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// withSyncFile returns a copy of s that fsyncs files and directories via fn
// instead of (*os.File).Sync. It exists only so tests can inject
// deterministic sync failures (and observe which handles were synced)
// without a real ops-level fault framework.
func (s *Store) withSyncFile(fn func(*os.File) error) *Store {
	c := *s
	c.syncFile = fn
	return &c
}

// withAfterStat returns a copy of s that runs fn after WriteFile stats a
// source and before it reads it. Test seam only.
func (s *Store) withAfterStat(fn func()) *Store {
	c := *s
	c.afterStat = fn
	return &c
}

// syncPath fsyncs the file or directory at path, so bytes written to it (for
// a file) or entries created in it (for a directory) are durable against a
// crash. Opening read-only is enough to fsync on the platforms Duro targets,
// and it never needs write permission on a published, read-only blob.
func (s *Store) syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return s.syncFile(f)
}

// syncChain fsyncs dir and then each ancestor, stopping after stopAt (which
// must be an ancestor of dir, or "" to continue to the filesystem root).
// Every level is synced unconditionally: a directory that already exists
// proves nothing, because the attempt that created it may have crashed
// before making the entry durable, and a concurrent creator may not have
// synced it yet.
func (s *Store) syncChain(dir, stopAt string) error {
	for {
		if err := s.syncPath(dir); err != nil {
			return err
		}
		if dir == stopAt {
			return nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}

// Path returns the on-disk path a blob with the given hex digest would
// occupy, whether or not it currently exists. Two levels of 2-hex-char
// sharding (65,536 leaf directories) keep any single directory small even
// at millions of stored blobs.
func (s *Store) Path(digest string) string {
	shard := digest
	if len(shard) < 4 {
		shard += strings.Repeat("0", 4-len(shard))
	}
	return filepath.Join(s.root, "sha256", shard[:2], shard[2:4], digest)
}

// Has reports whether a blob with the given digest is already stored. A
// malformed digest, or a path occupied by something other than a regular
// file, reports false rather than risking a traversal outside root or
// treating a symlink's mutable target as stored content.
func (s *Store) Has(digest string) bool {
	if !ValidDigest(digest) {
		return false
	}
	info, err := os.Lstat(s.Path(digest))
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}

// hashingCopy streams src to dst while hashing, using a fixed-size buffer so
// memory use is bounded regardless of src's length, and explicitly guards
// against wrapping the int64 byte counter rather than silently overflowing.
func hashingCopy(dst io.Writer, src io.Reader) (n int64, digest string, err error) {
	h := sha256.New()
	w := io.MultiWriter(dst, h)
	buf := make([]byte, 32*1024)
	for {
		nr, rerr := src.Read(buf)
		if nr > 0 {
			if n > math.MaxInt64-int64(nr) {
				return n, "", fmt.Errorf("cas: artifact exceeds maximum representable size (%d bytes)", int64(math.MaxInt64))
			}
			nw, werr := w.Write(buf[:nr])
			n += int64(nw)
			if werr != nil {
				return n, "", werr
			}
			if nw != nr {
				return n, "", io.ErrShortWrite
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			return n, "", rerr
		}
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// stageTemp streams src into a fresh temp file under ROOT/.tmp, hashing as
// it goes, and syncs the temp file before returning so its bytes are
// durable on disk before any later publish makes them visible under a
// digest path. The .tmp prefix makes a crash-abandoned file recognizable
// and distinct from a published blob; Store never treats a file under
// ROOT/.tmp as completed content.
func (s *Store) stageTemp(src io.Reader) (tmpPath, digest string, size int64, err error) {
	tmp, err := os.CreateTemp(filepath.Join(s.root, ".tmp"), "duro-put-*")
	if err != nil {
		return "", "", 0, err
	}
	tmpPath = tmp.Name()

	n, digest, copyErr := hashingCopy(tmp, src)
	if copyErr == nil {
		copyErr = s.syncFile(tmp)
	}
	closeErr := tmp.Close()
	if copyErr != nil {
		return tmpPath, "", 0, copyErr
	}
	if closeErr != nil {
		return tmpPath, "", 0, closeErr
	}
	return tmpPath, digest, n, nil
}

// publish makes a staged temp file visible at its digest path using a hard
// link rather than a rename: os.Link fails with EEXIST if the destination
// is already present, so two concurrent puts of the same fresh digest can
// never have one silently overwrite the other. The loser verifies the
// winner's bytes against digest/size instead of assuming they match.
// deduped reports whether a blob with digest was already durably stored
// (by this call or a previous one).
func (s *Store) publish(tmpPath, digest string, size int64) (deduped bool, err error) {
	final := s.Path(digest)
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	switch linkErr := os.Link(tmpPath, final); {
	case linkErr == nil:
		_ = os.Chmod(final, 0o444) // best-effort; dedup relies on presence, not perms
	case errors.Is(linkErr, fs.ErrExist):
		// Someone else's bytes are already at this digest path (published
		// concurrently, or by an earlier call). Verify them against
		// digest/size before treating them as this put's result -- the
		// existing artifact is left untouched either way -- and only then
		// establish durability below.
		if err := s.Verify(digest, size); err != nil {
			return false, err
		}
		deduped = true
	default:
		return false, linkErr
	}
	// Durability for every successful put, fresh or deduplicated: the blob
	// itself, then every directory entry leading to it up to the store root
	// (whose own entry Open made durable). This is never skipped just
	// because the paths already exist -- the attempt or the concurrent
	// creator that made them may not have synced them.
	if err := s.syncPath(final); err != nil {
		return false, err
	}
	if err := s.syncChain(dir, s.root); err != nil {
		return false, err
	}
	return deduped, nil
}

// Write streams src into the store, hashing as it goes, and returns the hex
// digest and byte size. deduped is true if a blob with that digest was
// already present, in which case the existing copy is kept and src's bytes
// are discarded after hashing and verification.
func (s *Store) Write(src io.Reader) (digest string, size int64, deduped bool, err error) {
	tmpPath, digest, size, err := s.stageTemp(src)
	defer os.Remove(tmpPath) // no-op once linked into place
	if err != nil {
		return "", 0, false, err
	}
	deduped, err = s.publish(tmpPath, digest, size)
	if err != nil {
		return "", 0, false, err
	}
	return digest, size, deduped, nil
}

// WriteFile streams a source file into the store. It stats path first for
// its expected length and compares that against the bytes actually read,
// so a source that changes size mid-ingestion (the caller is required to
// keep it stable) is rejected as ErrSizeMismatch rather than silently
// stored short or long.
func (s *Store) WriteFile(path string) (digest string, size int64, deduped bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", 0, false, err
	}
	if !info.Mode().IsRegular() {
		return "", 0, false, fmt.Errorf("cas: %s is not a regular file", path)
	}
	wantSize := info.Size()
	if s.afterStat != nil {
		s.afterStat()
	}

	tmpPath, digest, size, err := s.stageTemp(f)
	defer os.Remove(tmpPath) // no-op once linked into place
	if err != nil {
		return "", 0, false, err
	}
	if size != wantSize {
		return "", 0, false, fmt.Errorf("%w: %s was %d bytes at open, %d bytes read", ErrSizeMismatch, path, wantSize, size)
	}
	deduped, err = s.publish(tmpPath, digest, size)
	if err != nil {
		return "", 0, false, err
	}
	return digest, size, deduped, nil
}

// Verify streams a stored blob through SHA-256 without retaining its bytes
// in memory, checking both its recorded size and digest.
func (s *Store) Verify(digest string, size int64) error {
	if !ValidDigest(digest) {
		return fmt.Errorf("%w: %q", ErrInvalidDigest, digest)
	}
	if size < 0 {
		return fmt.Errorf("cas: size must not be negative")
	}
	path := s.Path(digest)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %s", ErrNotRegular, path)
	}
	if info.Size() != size {
		return fmt.Errorf("%w: %s stored size %d, expected %d", ErrCorrupt, digest, info.Size(), size)
	}
	f, err := s.get(digest)
	if err != nil {
		return err
	}
	defer f.Close()
	n, sum, err := hashingCopy(io.Discard, f)
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("%w: %s size changed while reading", ErrCorrupt, digest)
	}
	if sum != digest {
		return fmt.Errorf("%w: %s content does not match digest", ErrCorrupt, digest)
	}
	return nil
}

// get opens a stored blob for reading. It is unexported: it hands back
// unverified data (no digest/size check), so it exists only as a building
// block for Verify/GetToWriter/GetToFile, which apply that verification
// themselves; no external caller needs the raw handle.
func (s *Store) get(digest string) (*os.File, error) {
	if !ValidDigest(digest) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDigest, digest)
	}
	path := s.Path(digest)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s", ErrNotRegular, path)
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

// GetToWriter streams the blob stored at digest to w, verifying its bytes
// against digest as they are copied. w may already have received partial or
// corrupt bytes by the time an error is returned -- a stream has no staging
// area -- so callers MUST treat any non-nil error as a failed operation
// regardless of how much was written, and must not treat partial output as
// verified content.
func (s *Store) GetToWriter(digest string, w io.Writer) error {
	f, err := s.get(digest)
	if err != nil {
		return err
	}
	defer f.Close()
	n, sum, err := hashingCopy(w, f)
	if err != nil {
		return err
	}
	if sum != digest {
		return fmt.Errorf("%w: %s content does not match digest after writing %d bytes", ErrCorrupt, digest, n)
	}
	return nil
}

// GetToFile streams the blob stored at digest into a fresh temp file beside
// destPath, verifies it against digest, and only then publishes it to
// destPath. destPath is never silently overwritten: if it already exists,
// GetToFile returns ErrDestinationExists and leaves it untouched, and if a
// concurrent writer publishes destPath first, GetToFile detects the losing
// link and fails the same way rather than clobbering it.
func (s *Store) GetToFile(digest, destPath string) error {
	if _, err := os.Lstat(destPath); err == nil {
		return fmt.Errorf("%w: %s", ErrDestinationExists, destPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	src, err := s.get(digest)
	if err != nil {
		return err
	}
	defer src.Close()

	destDir := filepath.Dir(destPath)
	tmp, err := os.CreateTemp(destDir, "duro-get-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once linked into place

	n, sum, err := hashingCopy(tmp, src)
	if err == nil {
		err = s.syncFile(tmp)
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if sum != digest {
		return fmt.Errorf("%w: %s content does not match digest after reading %d bytes", ErrCorrupt, digest, n)
	}

	if err := os.Link(tmpPath, destPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: %s", ErrDestinationExists, destPath)
		}
		return err
	}
	// The destination's directory entry must survive a crash too, since
	// GetToFile's contract is "published only after successful verification".
	return s.syncPath(destDir)
}
