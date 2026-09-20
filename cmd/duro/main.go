// Command duro is the minimal client for the SQLite-local /
// PostgreSQL-canonical event ledger: append queues generic events, file
// queues canonical document bodies, init applies administrator-owned schema,
// sync drains the queue, and pull refreshes the local replica. It has no
// subcommand framework beyond stdlib flag, and no lineage/CAS-era commands.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
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
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: duro init|append|file|sync|pull|read|list|events|blob|kg")
	}
	if args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		_, err := fmt.Fprintln(os.Stdout, "Usage: duro init|append|file|sync|pull|read|list|events|blob|kg")
		return err
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
	case "events":
		return runEvents(args[1:])
	case "blob":
		return runBlob(args[1:])
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

func postgresDSN(value, path string) (string, error) {
	if value != "" && path != "" {
		return "", fmt.Errorf("--postgres and --postgres-file are mutually exclusive")
	}
	if path != "" {
		bytes, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read --postgres-file: %w", err)
		}
		value = strings.TrimSpace(string(bytes))
	}
	if value == "" {
		return "", fmt.Errorf("--postgres or --postgres-file is required")
	}
	return value, nil
}

func openProtectedPostgres(path string) (*postgres.Store, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("read --postgres-file: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("invalid --postgres-file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("invalid --postgres-file")
	}
	contents, err := io.ReadAll(file)
	if err != nil || len(strings.TrimSpace(string(contents))) == 0 {
		return nil, fmt.Errorf("read --postgres-file failed")
	}
	store, err := postgres.Open(strings.TrimSpace(string(contents)))
	if err != nil {
		return nil, fmt.Errorf("open canonical PostgreSQL store failed")
	}
	return store, nil
}

func runSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	localPath := fs.String("local", "", "local SQLite queue path")
	dsn := fs.String("postgres", "", "PostgreSQL canonical store DSN")
	dsnFile := fs.String("postgres-file", "", "protected file containing the PostgreSQL canonical store DSN")
	blobStore := fs.String("blob-store", blobconfig.PostgresKind, "canonical blob store: postgres or filesystem")
	blobRoot := fs.String("blob-root", "", "absolute filesystem blob store root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *localPath == "" {
		return fmt.Errorf("--local is required")
	}
	var postgresSet, postgresFileSet bool
	fs.Visit(func(flag *flag.Flag) {
		postgresSet = postgresSet || flag.Name == "postgres"
		postgresFileSet = postgresFileSet || flag.Name == "postgres-file"
	})
	if postgresSet && postgresFileSet {
		return fmt.Errorf("--postgres and --postgres-file are mutually exclusive")
	}
	dsnValue, err := postgresDSN(*dsn, *dsnFile)
	if err != nil {
		return err
	}
	if err := blobconfig.Validate(*blobStore, *blobRoot); err != nil {
		return err
	}

	loc, err := local.Open(*localPath)
	if err != nil {
		return err
	}
	defer loc.Close()
	rem, err := postgres.Open(dsnValue)
	if err != nil {
		return fmt.Errorf("open canonical PostgreSQL store failed")
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

type eventsResult struct {
	Events []eventMetadata `json:"events"`
	Cursor int64           `json:"cursor"`
}

type eventMetadata struct {
	Sequence      int64           `json:"sequence"`
	EventID       string          `json:"event_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	EventType     string          `json:"event_type"`
	Actor         string          `json:"actor"`
	Content       json.RawMessage `json:"content"`
	Refs          json.RawMessage `json:"refs"`
	ResourceURI   string          `json:"resource_uri"`
	BlobSHA256    string          `json:"blob_sha256"`
	BlobMediaType string          `json:"blob_media_type"`
	BlobSize      int64           `json:"blob_size"`
}

func runEvents(args []string) error {
	fs := flag.NewFlagSet("events", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	postgresFile := fs.String("postgres-file", "", "protected PostgreSQL canonical store DSN file")
	eventType := fs.String("type", "", "exact canonical event type")
	resourcePrefix := fs.String("resource-prefix", "", "literal logical resource URI prefix")
	after := fs.Int64("after", 0, "exclusive canonical sequence cursor")
	limit := fs.Int("limit", 500, "maximum events")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	if *postgresFile == "" || *eventType == "" || *resourcePrefix == "" {
		return fmt.Errorf("--postgres-file, --type, and --resource-prefix are required")
	}
	store, err := openProtectedPostgres(*postgresFile)
	if err != nil {
		return err
	}
	defer store.Close()
	rows, err := store.ListEventsByTypeAndResourcePrefix(*eventType, *resourcePrefix, *after, *limit)
	if err != nil {
		return fmt.Errorf("list canonical events failed")
	}
	result := eventsResult{Cursor: *after}
	for _, row := range rows {
		result.Events = append(result.Events, eventMetadata{
			Sequence: row.Sequence, EventID: row.Event.ID, OccurredAt: row.Event.OccurredAt, EventType: row.Event.EventType,
			Actor: row.Event.Actor, Content: row.Event.Content, Refs: row.Event.Refs, ResourceURI: row.Event.ResourceURI,
			BlobSHA256: row.BlobSHA256, BlobMediaType: row.BlobMediaType, BlobSize: row.BlobSize,
		})
		result.Cursor = row.Sequence
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func runBlob(args []string) error {
	fs := flag.NewFlagSet("blob", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	postgresFile := fs.String("postgres-file", "", "protected PostgreSQL canonical store DSN file")
	digest := fs.String("sha256", "", "canonical blob SHA-256")
	maxBytes := fs.Int64("max-bytes", 0, "maximum bytes to read")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	if *postgresFile == "" || *digest == "" || *maxBytes <= 0 {
		return fmt.Errorf("--postgres-file, --sha256, and positive --max-bytes are required")
	}
	store, err := openProtectedPostgres(*postgresFile)
	if err != nil {
		return err
	}
	defer store.Close()
	body, ok, err := store.ReadBlobLimited(*digest, *maxBytes)
	if err != nil {
		return fmt.Errorf("read canonical blob failed")
	}
	if !ok {
		return fmt.Errorf("canonical blob not found")
	}
	_, err = os.Stdout.Write(body.Content)
	return err
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
