package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

const testAdvisoryLockKey = 918273645

func runCLI(t *testing.T, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("go", append([]string{"run", "."}, args...)...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("duro %v failed: %v\n%s", args, err, out)
	}
	return out
}

// runCLIErr is runCLI for cases that are expected to fail: it returns the
// error and combined output instead of failing the test.
func runCLIErr(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.Command("go", append([]string{"run", "."}, args...)...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	return out, err
}

func TestRootHelpListsCommands(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"help"}} {
		out := string(runCLI(t, args...))
		for _, want := range []string{"Usage: duro", "init", "append", "file", "sync", "pull", "read", "list", "kg"} {
			if !strings.Contains(out, want) {
				t.Fatalf("duro %v output %q missing %q", args, out, want)
			}
		}
	}
}

func TestAppendEnqueuesLocalEvent(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")

	out := runCLI(t, "append", "--local", localPath, "--type", "test.type", "--content", `{"a":1}`)
	var got appendResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad json: %v\n%s", err, out)
	}
	if got.EventID == "" || !got.Fresh {
		t.Fatalf("bad result: %+v", got)
	}

	db, err := sql.Open("sqlite", localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM local_events WHERE id = ?`, got.EventID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("want 1 local_events row, got %d", count)
	}
}

func TestFileStagesDocumentBody(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")
	bodyPath := filepath.Join(dir, "note.md")
	if err := os.WriteFile(bodyPath, []byte("canonical body"), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runCLI(t, "file", "--local", localPath, "--body", bodyPath, "--source", "notes/example", "--resource-uri", "cura://palace/wing/room/example", "--media-type", "text/plain")
	var got appendResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad json: %v\\n%s", err, out)
	}
	if got.EventID == "" || !got.Fresh {
		t.Fatalf("bad result: %+v", got)
	}

	db, err := sql.Open("sqlite", localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var eventType, content, resourceURI, mediaType string
	var size int64
	var body []byte
	if err := db.QueryRow(`SELECT e.event_type, e.content, e.resource_uri, b.media_type, b.size_bytes, b.content
		FROM local_events e JOIN local_blobs b ON b.event_id = e.id WHERE e.id = ?`, got.EventID).Scan(&eventType, &content, &resourceURI, &mediaType, &size, &body); err != nil {
		t.Fatal(err)
	}
	if eventType != "document.filed" || content != `{"source":"notes/example"}` {
		t.Fatalf("event = type %q content %q", eventType, content)
	}
	if resourceURI != "cura://palace/wing/room/example" {
		t.Fatalf("resource URI = %q", resourceURI)
	}
	if mediaType != "text/plain" || size != int64(len("canonical body")) || string(body) != "canonical body" {
		t.Fatalf("blob = media_type %q size %d content %q", mediaType, size, body)
	}
}

func TestFileStreamsFilesystemBlobWithoutSQLiteCopy(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")
	root := filepath.Join(dir, "blobs")
	bodyPath := filepath.Join(dir, "run-output.bin")
	body := make([]byte, 2<<20)
	for i := range body {
		body[i] = byte(i)
	}
	if err := os.WriteFile(bodyPath, body, 0o600); err != nil {
		t.Fatal(err)
	}

	out := runCLI(t, "file", "--local", localPath, "--body", bodyPath, "--source", "runs/example/output", "--media-type", "application/octet-stream", "--blob-store", "filesystem", "--blob-root", root)
	var got appendResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad json: %v\\n%s", err, out)
	}

	db, err := sql.Open("sqlite", localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var digest string
	var size int64
	if err := db.QueryRow(`SELECT sha256, size_bytes FROM local_blob_refs WHERE event_id = ?`, got.EventID).Scan(&digest, &size); err != nil {
		t.Fatal(err)
	}
	if size != int64(len(body)) {
		t.Fatalf("local blob reference size = %d, want %d", size, len(body))
	}
	var stagedCopies int
	if err := db.QueryRow(`SELECT count(*) FROM local_blobs WHERE event_id = ?`, got.EventID).Scan(&stagedCopies); err != nil {
		t.Fatal(err)
	}
	if stagedCopies != 0 {
		t.Fatalf("want no SQLite blob copy, got %d", stagedCopies)
	}
	if info, err := os.Stat(filepath.Join(root, "sha256", digest[:2], digest[2:4], digest)); err != nil || info.Size() != size {
		t.Fatalf("filesystem blob = info:%v err:%v, want size %d", info, err, size)
	}
}

func TestFileUsesProvidedEventID(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")
	bodyPath := filepath.Join(dir, "note.md")
	id := "6d68e2af-8e91-4fda-a0dd-959f90bc5af8"
	if err := os.WriteFile(bodyPath, []byte("canonical body"), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runCLI(t, "file", "--id", id, "--local", localPath, "--body", bodyPath, "--source", "notes/example")
	var got appendResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad json: %v\n%s", err, out)
	}
	if got.EventID != id || !got.Fresh {
		t.Fatalf("result = %+v, want provided fresh id %q", got, id)
	}
}

func TestFileUsesProvidedOccurrenceTime(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")
	bodyPath := filepath.Join(dir, "note.md")
	occurredAt := "2026-08-22T19:30:00Z"
	if err := os.WriteFile(bodyPath, []byte("canonical body"), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runCLI(t, "file", "--occurred-at", occurredAt, "--local", localPath, "--body", bodyPath, "--source", "notes/example")
	var got appendResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad json: %v\n%s", err, out)
	}

	db, err := sql.Open("sqlite", localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var stored string
	if err := db.QueryRow(`SELECT occurred_at FROM local_events WHERE id = ?`, got.EventID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != occurredAt {
		t.Fatalf("occurred_at = %q, want %q", stored, occurredAt)
	}
}

func TestFileUsesExplicitEventType(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")
	bodyPath := filepath.Join(dir, "dataset.bin")
	if err := os.WriteFile(bodyPath, []byte("dataset bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runCLI(t, "file", "--local", localPath, "--body", bodyPath, "--source", "datasets/example/v1", "--type", "dataset.filed")
	var got appendResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad json: %v\\n%s", err, out)
	}
	db, err := sql.Open("sqlite", localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var eventType string
	if err := db.QueryRow(`SELECT event_type FROM local_events WHERE id = ?`, got.EventID).Scan(&eventType); err != nil {
		t.Fatal(err)
	}
	if eventType != "dataset.filed" {
		t.Fatalf("event_type = %q, want dataset.filed", eventType)
	}
}

func TestAppendUsesExplicitActor(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")

	out := runCLI(t, "append", "--actor", "duro_ingest", "--local", localPath, "--type", "test.type", "--content", `{"a":1}`)
	var got appendResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad json: %v\\n%s", err, out)
	}

	db, err := sql.Open("sqlite", localPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var actor string
	if err := db.QueryRow(`SELECT actor FROM local_events WHERE id = ?`, got.EventID).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if actor != "duro_ingest" {
		t.Fatalf("actor = %q, want explicit actor", actor)
	}
}

func TestAppendRequiresContentType(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")

	if _, err := runCLIErr(t, "append", "--local", localPath, "--content", `{"a":1}`); err == nil {
		t.Fatal("expected failure without --type")
	}
}

func TestAppendRejectsNonObjectContent(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")

	out, err := runCLIErr(t, "append", "--local", localPath, "--type", "test.type", "--content", `"just a string"`)
	if err == nil {
		t.Fatalf("expected failure for non-object content, got output %s", out)
	}
}

func TestReadRequiresSource(t *testing.T) {
	out, err := runCLIErr(t, "read", "--postgres", "postgresql://example")
	if err == nil || !strings.Contains(string(out), "--postgres and --source are required") {
		t.Fatalf("read without source = err %v output %q", err, out)
	}
}

func TestReadReturnsLatestCanonicalDocument(t *testing.T) {
	dsn := testPostgresDSN(t)
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")
	source := "example/test/diary"

	for _, body := range []string{"older", "newer"} {
		bodyPath := filepath.Join(dir, body+".txt")
		if err := os.WriteFile(bodyPath, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		runCLI(t, "file", "--local", localPath, "--body", bodyPath, "--source", source, "--media-type", "text/plain")
		runCLI(t, "sync", "--local", localPath, "--postgres", dsn)
	}

	out := runCLI(t, "read", "--postgres", dsn, "--source", source)
	var got readResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad read JSON: %v\n%s", err, out)
	}
	if !got.Found || got.Body != "newer" || got.Source != source || got.Sequence <= 0 || got.EventID == "" || got.BlobSHA256 == "" {
		t.Fatalf("read result = %+v, want newest document with provenance", got)
	}
	if _, err := runCLIErr(t, "read", "--postgres", dsn, "--source", source, "--max-bytes", "4"); err == nil {
		t.Fatal("expected bounded read to reject larger body")
	}
}

func TestListRequiresPostgres(t *testing.T) {
	out, err := runCLIErr(t, "list")
	if err == nil || !strings.Contains(string(out), "--postgres is required") {
		t.Fatalf("list without postgres = err %v output %q", err, out)
	}
}

func TestListReturnsCurrentCanonicalDocumentMetadata(t *testing.T) {
	dsn := testPostgresDSN(t)
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")
	for _, doc := range []struct {
		name, source, body, resourceURI string
	}{
		{"alpha-old", "example/drawers/alpha", "alpha-old", ""},
		{"beta", "example/drawers/beta", "beta", "cura://palace/wing/room/beta"},
		{"alpha-new", "example/drawers/alpha", "alpha-new", ""},
	} {
		bodyPath := filepath.Join(dir, doc.name+".txt")
		if err := os.WriteFile(bodyPath, []byte(doc.body), 0o600); err != nil {
			t.Fatal(err)
		}
		args := []string{"file", "--local", localPath, "--body", bodyPath, "--source", doc.source, "--media-type", "text/plain"}
		if doc.resourceURI != "" {
			args = append(args, "--resource-uri", doc.resourceURI)
		}
		runCLI(t, args...)
		runCLI(t, "sync", "--local", localPath, "--postgres", dsn)
	}

	out := runCLI(t, "list", "--postgres", dsn, "--after", "0", "--limit", "10")
	var got struct {
		Documents []readResult `json:"documents"`
		Cursor    int64        `json:"cursor"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("bad list JSON: %v\n%s", err, out)
	}
	if len(got.Documents) != 2 || got.Documents[0].Source != "example/drawers/beta" || got.Documents[1].Source != "example/drawers/alpha" {
		t.Fatalf("list result = %+v, want current beta then alpha", got)
	}
	for _, doc := range got.Documents {
		if !doc.Found || doc.EventID == "" || doc.BlobSHA256 == "" || doc.MediaType != "text/plain" || doc.Body != "" {
			t.Fatalf("listed document = %+v, want metadata with canonical provenance only", doc)
		}
	}
	if got.Documents[0].ResourceURI != "cura://palace/wing/room/beta" || got.Documents[1].ResourceURI != "" {
		t.Fatalf("listed resource URIs = %#v", got.Documents)
	}
	if got.Cursor != got.Documents[1].Sequence {
		t.Fatalf("cursor = %d, want final sequence %d", got.Cursor, got.Documents[1].Sequence)
	}
}

func TestInitAppliesCanonicalSchema(t *testing.T) {
	dsn := os.Getenv("DURO_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("DURO_POSTGRES_TEST_DSN not set; skipping PostgreSQL integration test")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(`SELECT pg_advisory_lock($1)`, testAdvisoryLockKey); err != nil {
		t.Fatal(err)
	}
	defer raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)

	runCLI(t, "init", "--postgres", dsn)
	var events sql.NullString
	if err := raw.QueryRow(`SELECT to_regclass('public.events')`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events.String != "events" {
		t.Fatalf("events table missing after init: %q", events.String)
	}
}

// testPostgresDSN skips the test clearly when DURO_POSTGRES_TEST_DSN is
// unset, and otherwise returns it after truncating the canonical tables.
func testPostgresDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DURO_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("DURO_POSTGRES_TEST_DSN not set; skipping PostgreSQL integration test")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	raw.SetMaxOpenConns(1)
	if _, err := raw.Exec(`SELECT pg_advisory_lock($1)`, testAdvisoryLockKey); err != nil {
		raw.Close()
		t.Fatalf("acquire test lock: %v", err)
	}
	if err := postgres.Initialize(dsn); err != nil {
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
		t.Fatalf("postgres.Initialize: %v", err)
	}
	if _, err := raw.Exec(`TRUNCATE events, blobs RESTART IDENTITY`); err != nil {
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = raw.Exec(`SELECT pg_advisory_unlock($1)`, testAdvisoryLockKey)
		raw.Close()
	})
	return dsn
}

// TestAppendSyncPullRoundTrip is the append -> sync -> pull smoke test: an
// offline append survives to a sync push, and a second, fresh local
// replica can pull the canonical result back.
func TestAppendSyncPullRoundTrip(t *testing.T) {
	dsn := testPostgresDSN(t)
	dir := t.TempDir()
	pushPath := filepath.Join(dir, "push.sqlite")
	pullPath := filepath.Join(dir, "pull.sqlite")

	appendOut := runCLI(t, "append", "--local", pushPath, "--type", "test.type", "--content", `{"a":1}`)
	var appended appendResult
	if err := json.Unmarshal(appendOut, &appended); err != nil {
		t.Fatalf("bad json: %v\n%s", err, appendOut)
	}

	syncOut := runCLI(t, "sync", "--local", pushPath, "--postgres", dsn)
	var synced syncResult
	if err := json.Unmarshal(syncOut, &synced); err != nil {
		t.Fatalf("bad json: %v\n%s", err, syncOut)
	}
	if synced.Accepted != 1 {
		t.Fatalf("want 1 accepted, got %+v", synced)
	}

	pullOut := runCLI(t, "pull", "--local", pullPath, "--postgres", dsn)
	var pulled pullResult
	if err := json.Unmarshal(pullOut, &pulled); err != nil {
		t.Fatalf("bad json: %v\n%s", err, pullOut)
	}
	if pulled.Applied != 1 {
		t.Fatalf("want 1 applied, got %+v", pulled)
	}
	if pulled.Cursor <= 0 {
		t.Fatalf("want positive cursor, got %d", pulled.Cursor)
	}

	db, err := sql.Open("sqlite", pullPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var content string
	if err := db.QueryRow(`SELECT content FROM local_events WHERE id = ?`, appended.EventID).Scan(&content); err != nil {
		t.Fatalf("select pulled event: %v", err)
	}
	// PostgreSQL's JSONB round-trips {"a":1} as {"a": 1} (a space after the
	// colon); the pulled local row reflects canonical's serialization.
	if content != `{"a": 1}` {
		t.Fatalf(`want content {"a": 1}, got %s`, content)
	}
}

// TestSyncReportsConflict exercises the conflict branch of sync's counts:
// a local row retried with different content than what's already
// canonical is reported, not silently dropped or overwritten.
func TestSyncReportsConflict(t *testing.T) {
	dsn := testPostgresDSN(t)
	dir := t.TempDir()
	localPath := filepath.Join(dir, "local.sqlite")

	appendOut := runCLI(t, "append", "--local", localPath, "--type", "test.type", "--content", `{"a":1}`)
	var appended appendResult
	if err := json.Unmarshal(appendOut, &appended); err != nil {
		t.Fatalf("bad json: %v\n%s", err, appendOut)
	}
	if _, err := runCLIErr(t, "sync", "--local", localPath, "--postgres", dsn); err != nil {
		t.Fatalf("first sync failed: %v", err)
	}

	// Overwrite the local row's content directly (bypassing append's
	// dup/conflict check) to simulate a second node having queued a
	// different payload under the same id before this one saw canonical.
	db, err := sql.Open("sqlite", localPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE local_events SET content = ?, synced_at = NULL WHERE id = ?`, `{"a":2}`, appended.EventID); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()

	syncOut := runCLI(t, "sync", "--local", localPath, "--postgres", dsn)
	var synced syncResult
	if err := json.Unmarshal(syncOut, &synced); err != nil {
		t.Fatalf("bad json: %v\n%s", err, syncOut)
	}
	if synced.Conflict != 1 {
		t.Fatalf("want 1 conflict, got %+v", synced)
	}
}
