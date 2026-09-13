// Command duro is the minimal client for the SQLite-local /
// PostgreSQL-canonical event ledger: append queues generic events, file
// queues canonical document bodies, init applies administrator-owned schema,
// sync drains the queue, and pull refreshes the local replica. It has no
// subcommand framework beyond stdlib flag, and no lineage/CAS-era commands.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"mime"
	"os"
	"os/user"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/nseyedtalebi/duro-ledger/pkg/blob"
	"github.com/nseyedtalebi/duro-ledger/pkg/blobconfig"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/knowledgegraph"
	"github.com/nseyedtalebi/duro-ledger/pkg/local"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
	"github.com/nseyedtalebi/duro-ledger/pkg/sync"
)

// defaultPullBatch bounds how many canonical rows Pull fetches per round
// trip. It is an internal implementation detail, not a flag: the plan's
// command surface exposes no batch-size knob.
const defaultPullBatch = 500

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: duro init|append|file|sync|pull|read|list|kg")
	}
	switch args[0] {
	case "init":
		return runInit(args[1:])
	case "append":
		return runAppend(args[1:])
	case "file":
		return runFile(args[1:])
	case "sync":
		return runSync(args[1:])
	case "pull":
		return runPull(args[1:])
	case "read":
		return runRead(args[1:])
	case "list":
		return runList(args[1:])
	case "kg":
		return runKG(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

type initResult struct {
	Initialized bool `json:"initialized"`
}

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dsn := fs.String("postgres", "", "PostgreSQL administrator/provisioning DSN")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dsn == "" {
		return fmt.Errorf("--postgres is required")
	}
	if err := postgres.Initialize(*dsn); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(initResult{Initialized: true})
}

type appendResult struct {
	EventID string `json:"event_id"`
	Fresh   bool   `json:"fresh"`
}

func runAppend(args []string) error {
	fs := flag.NewFlagSet("append", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	localPath := fs.String("local", "", "local SQLite queue path")
	eventType := fs.String("type", "", "event type")
	actor := fs.String("actor", "", "actor claim; canonical storage binds new IDs to the authenticated PostgreSQL role")
	content := fs.String("content", "", "event content, a JSON object")
	refs := fs.String("refs", "", "event refs, a JSON object")
	resourceURI := fs.String("resource-uri", "", "optional absolute logical resource URI")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *localPath == "" || *eventType == "" || *content == "" {
		return fmt.Errorf("--local, --type, and --content are required")
	}

	var refsJSON json.RawMessage
	if *refs != "" {
		refsJSON = json.RawMessage(*refs)
	}
	ev, err := event.NewWithResourceURI("", *eventType, actorName(*actor), time.Time{}, json.RawMessage(*content), refsJSON, *resourceURI)
	if err != nil {
		return err
	}

	s, err := local.Open(*localPath)
	if err != nil {
		return err
	}
	defer s.Close()

	fresh, err := s.Enqueue(ev)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(appendResult{EventID: ev.ID, Fresh: fresh})
}

// runFile records a source-addressed event with exact body bytes in the blob
// store. document.filed remains the default; callers can name a more precise
// type such as dataset.filed without changing Duro's generic blob path.
func runFile(args []string) error {
	fs := flag.NewFlagSet("file", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	localPath := fs.String("local", "", "local SQLite queue path")
	bodyPath := fs.String("body", "", "document body file")
	source := fs.String("source", "", "source reference")
	resourceURI := fs.String("resource-uri", "", "optional absolute logical resource URI")
	eventType := fs.String("type", "document.filed", "event type")
	mediaType := fs.String("media-type", "", "body media type")
	blobStore := fs.String("blob-store", blobconfig.PostgresKind, "canonical blob store: postgres or filesystem")
	blobRoot := fs.String("blob-root", "", "absolute filesystem blob store root")
	actor := fs.String("actor", "", "actor claim; canonical storage binds new IDs to the authenticated PostgreSQL role")
	id := fs.String("id", "", "optional client-generated event UUID for retry")
	occurredAt := fs.String("occurred-at", "", "optional RFC3339 occurrence time for retry")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *localPath == "" || *bodyPath == "" || *source == "" {
		return fmt.Errorf("--local, --body, and --source are required")
	}
	if err := blobconfig.Validate(*blobStore, *blobRoot); err != nil {
		return err
	}
	content, err := json.Marshal(map[string]string{"source": *source})
	if err != nil {
		return err
	}
	if *mediaType == "" {
		*mediaType = mime.TypeByExtension(filepath.Ext(*bodyPath))
		if *mediaType == "" {
			*mediaType = "application/octet-stream"
		}
	}
	when := time.Time{}
	if *occurredAt != "" {
		when, err = time.Parse(time.RFC3339Nano, *occurredAt)
		if err != nil {
			return fmt.Errorf("--occurred-at: %w", err)
		}
	}
	ev, err := event.NewWithResourceURI(*id, *eventType, actorName(*actor), when, content, nil, *resourceURI)
	if err != nil {
		return err
	}

	s, err := local.Open(*localPath)
	if err != nil {
		return err
	}
	defer s.Close()
	var fresh bool
	if *blobStore == blobconfig.FilesystemKind {
		backend, err := blob.NewFilesystemStore(*blobRoot)
		if err != nil {
			return err
		}
		digest, size, err := backend.PutFile(*bodyPath)
		if err != nil {
			return err
		}
		fresh, err = s.EnqueueWithBlobRef(ev, digest, size, *mediaType)
	} else {
		body, err := os.ReadFile(*bodyPath)
		if err != nil {
			return err
		}
		fresh, err = s.EnqueueWithBlob(ev, body, *mediaType)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(appendResult{EventID: ev.ID, Fresh: fresh})
}

// actorName uses an explicit actor when supplied, then DURO_ACTOR, and
// otherwise falls back to the local OS username for offline queue records.
func actorName(explicit string) string {
	if explicit == "" {
		explicit = os.Getenv("DURO_ACTOR")
	}
	if explicit != "" {
		return explicit
	}
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "unknown"
}

type syncResult struct {
	Accepted       int `json:"accepted"`
	AlreadyPresent int `json:"already_present"`
	Conflict       int `json:"conflict"`
	Rejected       int `json:"rejected"`
}

func runSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	localPath := fs.String("local", "", "local SQLite queue path")
	dsn := fs.String("postgres", "", "PostgreSQL canonical store DSN")
	blobStore := fs.String("blob-store", blobconfig.PostgresKind, "canonical blob store: postgres or filesystem")
	blobRoot := fs.String("blob-root", "", "absolute filesystem blob store root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *localPath == "" || *dsn == "" {
		return fmt.Errorf("--local and --postgres are required")
	}
	if err := blobconfig.Validate(*blobStore, *blobRoot); err != nil {
		return err
	}

	loc, err := local.Open(*localPath)
	if err != nil {
		return err
	}
	defer loc.Close()
	rem, err := postgres.Open(*dsn)
	if err != nil {
		return err
	}
	defer rem.Close()

	var res sync.PushResult
	if *blobStore == blobconfig.PostgresKind {
		res, err = sync.Push(loc, rem, 0)
	} else {
		backend, openErr := blobconfig.Open(*blobStore, *blobRoot, rem)
		if openErr != nil {
			return openErr
		}
		res, err = sync.PushWithBlobStore(loc, rem, backend, 0)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(syncResult{
		Accepted:       res.Accepted,
		AlreadyPresent: res.AlreadyPresent,
		Conflict:       res.Conflict,
		Rejected:       res.Rejected,
	})
}

type pullResult struct {
	Applied int   `json:"applied"`
	Cursor  int64 `json:"cursor"`
}

func runPull(args []string) error {
	fs := flag.NewFlagSet("pull", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	localPath := fs.String("local", "", "local SQLite queue path")
	dsn := fs.String("postgres", "", "PostgreSQL canonical store DSN")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *localPath == "" || *dsn == "" {
		return fmt.Errorf("--local and --postgres are required")
	}

	loc, err := local.Open(*localPath)
	if err != nil {
		return err
	}
	defer loc.Close()
	rem, err := postgres.Open(*dsn)
	if err != nil {
		return err
	}
	defer rem.Close()

	n, err := sync.Pull(rem, loc, defaultPullBatch)
	if err != nil {
		return err
	}
	cursor, err := loc.Cursor()
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(pullResult{Applied: n, Cursor: cursor})
}

type readResult struct {
	Found       bool   `json:"found"`
	Sequence    int64  `json:"sequence,omitempty"`
	EventID     string `json:"event_id,omitempty"`
	Source      string `json:"source,omitempty"`
	ResourceURI string `json:"resource_uri,omitempty"`
	BlobSHA256  string `json:"blob_sha256,omitempty"`
	MediaType   string `json:"media_type,omitempty"`
	Body        string `json:"body,omitempty"`
}

func runRead(args []string) error {
	fs := flag.NewFlagSet("read", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dsn := fs.String("postgres", "", "PostgreSQL canonical store DSN")
	source := fs.String("source", "", "opaque source reference")
	maxBytes := fs.Int64("max-bytes", 1<<20, "maximum document body bytes")
	blobStore := fs.String("blob-store", blobconfig.PostgresKind, "canonical blob store: postgres or filesystem")
	blobRoot := fs.String("blob-root", "", "absolute filesystem blob store root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dsn == "" || *source == "" {
		return fmt.Errorf("--postgres and --source are required")
	}
	if err := blobconfig.Validate(*blobStore, *blobRoot); err != nil {
		return err
	}
	store, err := postgres.Open(*dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	backend, err := blobconfig.Open(*blobStore, *blobRoot, store)
	if err != nil {
		return err
	}
	doc, ok, err := store.ReadLatestDocumentMetadata(*source)
	if err != nil {
		return err
	}
	if !ok {
		return json.NewEncoder(os.Stdout).Encode(readResult{Found: false})
	}
	doc.Body, err = backend.Read(doc.BlobSHA256, *maxBytes)
	if err != nil {
		return err
	}
	if !utf8.Valid(doc.Body) {
		return fmt.Errorf("document %s is not UTF-8 text", doc.EventID)
	}
	return json.NewEncoder(os.Stdout).Encode(readResult{
		Found: true, Sequence: doc.Sequence, EventID: doc.EventID, Source: doc.Source, ResourceURI: doc.ResourceURI,
		BlobSHA256: doc.BlobSHA256, MediaType: doc.MediaType, Body: string(doc.Body),
	})
}

func runList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dsn := fs.String("postgres", "", "PostgreSQL canonical store DSN")
	after := fs.Int64("after", 0, "exclusive canonical sequence cursor")
	limit := fs.Int("limit", 100, "maximum current documents to return")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dsn == "" {
		return fmt.Errorf("--postgres is required")
	}
	store, err := postgres.Open(*dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	docs, err := store.ListLatestDocuments(*after, *limit)
	if err != nil {
		return err
	}

	result := struct {
		Documents []readResult `json:"documents"`
		Cursor    int64        `json:"cursor"`
	}{Cursor: *after}
	for _, doc := range docs {
		result.Documents = append(result.Documents, readResult{
			Found: true, Sequence: doc.Sequence, EventID: doc.EventID, Source: doc.Source, ResourceURI: doc.ResourceURI,
			BlobSHA256: doc.BlobSHA256, MediaType: doc.MediaType,
		})
		result.Cursor = doc.Sequence
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func runKG(args []string) error {
	fs := flag.NewFlagSet("kg", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dsn := fs.String("postgres", "", "PostgreSQL canonical store DSN")
	subject := fs.String("subject", "", "subject filter")
	predicate := fs.String("predicate", "", "predicate filter")
	object := fs.String("object", "", "object filter")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dsn == "" {
		return fmt.Errorf("--postgres is required")
	}

	store, err := postgres.Open(*dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	projection, err := knowledgegraph.Project(context.Background(), store)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(projection.Query(*subject, *predicate, *object))
}
