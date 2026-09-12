package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/hermesmemory"
	"github.com/nseyedtalebi/duro-ledger/pkg/local"
	durosync "github.com/nseyedtalebi/duro-ledger/pkg/sync"
)

func TestConfigRejectsUnsafeOrIncompleteStartup(t *testing.T) {
	base := validStartupArgs(t, "postgresql://writer@db.example/duro?sslmode=verify-full")
	cases := []struct {
		name string
		args []string
	}{
		{name: "missing token file", args: replaceStartupArg(base, "--token-file", filepath.Join(t.TempDir(), "missing"))},
		{name: "missing dsn file", args: replaceStartupArg(base, "--postgres-file", filepath.Join(t.TempDir(), "missing"))},
		{name: "empty bind", args: replaceStartupArg(base, "--listen", "")},
		{name: "empty queue", args: replaceStartupArg(base, "--queue", "")},
		{name: "empty writer principal", args: replaceStartupArg(base, "--writer-principal", "")},
		{name: "empty writer surface", args: replaceStartupArg(base, "--writer-surface", "")},
		{name: "zero max bytes", args: replaceStartupArg(base, "--max-request-bytes", "0")},
		{name: "negative max bytes", args: replaceStartupArg(base, "--max-request-bytes", "-1")},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			called := false
			err := run(test.args, func(*http.Server) error {
				called = true
				return nil
			})
			if err == nil {
				t.Fatal("invalid startup configuration was accepted")
			}
			if called {
				t.Fatal("server started before configuration validation")
			}
		})
	}

	for _, test := range []struct {
		name string
		dsn  string
	}{
		{name: "url userinfo password", dsn: "postgresql://writer:***@db.example/duro?sslmode=verify-full"},
		{name: "url query password", dsn: "postgresql://writer@db.example/duro?sslmode=verify-full&password=secret"},
		{name: "key value password", dsn: "host=db.example user=writer password=secret sslmode=verify-full"},
		{name: "url query passfile", dsn: "postgresql://writer@db.example/duro?sslmode=verify-full&passfile=%2Ftmp%2Fpgpass"},
		{name: "url query service", dsn: "postgresql://writer@db.example/duro?sslmode=verify-full&service=writer"},
		{name: "url query servicefile", dsn: "postgresql://writer@db.example/duro?sslmode=verify-full&servicefile=%2Ftmp%2Fpg_service.conf"},
		{name: "url query sslpassword", dsn: "postgresql://writer@db.example/duro?sslmode=verify-full&sslpassword=secret"},
		{name: "key value passfile", dsn: "host=db.example user=writer passfile=/tmp/pgpass sslmode=verify-full"},
		{name: "key value service", dsn: "host=db.example user=writer service=writer sslmode=verify-full"},
		{name: "key value servicefile", dsn: "host=db.example user=writer servicefile=/tmp/pg_service.conf sslmode=verify-full"},
		{name: "key value sslpassword", dsn: "host=db.example user=writer sslpassword=secret sslmode=verify-full"},
		{name: "non verify full tls", dsn: "postgresql://writer@db.example/duro?sslmode=require"},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := validStartupArgs(t, test.dsn)
			if _, err := loadConfig(args); err == nil {
				t.Fatal("unsafe DSN was accepted")
			} else if strings.Contains(err.Error(), test.dsn) {
				t.Fatalf("startup error leaked DSN: %q", err)
			}
		})
	}
}

func TestConfigRejectsPostgresEnvironmentBeforeReadingFilesOrServing(t *testing.T) {
	for _, variable := range []string{
		"PGPASSWORD",
		"PGPASSFILE",
		"PGSERVICE",
		"PGSERVICEFILE",
		"PGHOST",
		"PGUSER",
		"PGEXTRA",
	} {
		t.Run(variable, func(t *testing.T) {
			const value = "environment-secret"
			t.Setenv(variable, value)
			args := validStartupArgs(t, "postgresql://writer@db.example/duro?sslmode=verify-full")
			args = replaceStartupArg(args, "--token-file", filepath.Join(t.TempDir(), "missing"))
			called := false
			err := run(args, func(*http.Server) error {
				called = true
				return nil
			})
			if err == nil {
				t.Fatal("PostgreSQL environment configuration was accepted")
			}
			if err.Error() != "PostgreSQL environment configuration is not permitted" {
				t.Fatalf("startup error = %q", err)
			}
			if strings.Contains(err.Error(), value) {
				t.Fatalf("startup error leaked environment value: %q", err)
			}
			if called {
				t.Fatal("server started with PostgreSQL environment configuration")
			}
		})
	}
}

func TestConfigRejectsNonTailscaleListenersBeforeStarting(t *testing.T) {
	for _, listen := range []string{
		"0.0.0.0:0",
		"127.0.0.1:8765",
		"localhost:8765",
		"host.example:8765",
		"203.0.113.1:8765",
		"100.64.0.1:0",
	} {
		t.Run(listen, func(t *testing.T) {
			called := false
			err := run(replaceStartupArg(validStartupArgs(t, "postgresql://writer@db.example/duro?sslmode=verify-full"), "--listen", listen), func(*http.Server) error {
				called = true
				return nil
			})
			if err == nil {
				t.Fatal("non-Tailscale listener was accepted")
			}
			if called {
				t.Fatal("server started before listener validation")
			}
		})
	}
}

func TestConfiguredServerUsesFiniteDeadlinesAndClosesSlowPreAuthClients(t *testing.T) {
	var server *http.Server
	if err := run(validStartupArgs(t, "postgresql://writer@db.example/duro?sslmode=verify-full"), func(configured *http.Server) error {
		server = configured
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if server == nil {
		t.Fatal("server was not configured")
	}
	for _, deadline := range []time.Duration{server.ReadHeaderTimeout, server.ReadTimeout, server.WriteTimeout, server.IdleTimeout} {
		if deadline <= 0 {
			t.Fatal("server has a non-finite deadline")
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(served)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		<-served
	})

	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := io.WriteString(connection, "POST /mcp HTTP/1.1\r\nHost: localhost\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(server.ReadHeaderTimeout + time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Read(make([]byte, 1)); err == nil {
		t.Fatal("slow pre-auth client remained open")
	} else if networkErr, ok := err.(net.Error); ok && networkErr.Timeout() {
		t.Fatalf("slow pre-auth client outlived server deadline: %v", err)
	}
}

func TestConfigAcceptsTailscaleListener(t *testing.T) {
	config, err := loadConfig(validStartupArgs(t, "postgresql://writer@db.example/duro?sslmode=verify-full"))
	if err != nil {
		t.Fatal(err)
	}
	if config.listen != "100.64.0.1:8765" {
		t.Fatalf("listen = %q", config.listen)
	}
}

func TestReadProtectedFileRejectsUnsafeFiles(t *testing.T) {
	dir := t.TempDir()
	safe := filepath.Join(dir, "safe")
	if err := os.WriteFile(safe, []byte("\nsecret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("world readable", func(t *testing.T) {
		path := filepath.Join(dir, "world-readable")
		if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := readProtectedFile(path); err == nil {
			t.Fatal("world-readable protected file was accepted")
		}
	})

	t.Run("symlink", func(t *testing.T) {
		path := filepath.Join(dir, "link")
		if err := os.Symlink(safe, path); err != nil {
			t.Fatal(err)
		}
		if _, err := readProtectedFile(path); err == nil {
			t.Fatal("symlink protected file was accepted")
		}
	})

	t.Run("regular owner-only file", func(t *testing.T) {
		contents, err := readProtectedFile(safe)
		if err != nil {
			t.Fatal(err)
		}
		if contents != "secret" {
			t.Fatalf("contents = %q, want trimmed secret", contents)
		}
	})
}

func TestConfigRejectsEmptySecretFiles(t *testing.T) {
	for _, fileFlag := range []string{"--token-file", "--postgres-file"} {
		t.Run(fileFlag, func(t *testing.T) {
			args := validStartupArgs(t, "postgresql://writer@db.example/duro?sslmode=verify-full")
			for i := range args {
				if args[i] == fileFlag {
					if err := os.WriteFile(args[i+1], []byte(" \n\t"), 0o600); err != nil {
						t.Fatal(err)
					}
					break
				}
			}
			if _, err := loadConfig(args); err == nil {
				t.Fatal("empty secret file was accepted")
			}
		})
	}
}

func TestConfigReadsTrimmedSafeDSNs(t *testing.T) {
	for _, dsn := range []string{
		"postgresql://writer@db.example/duro?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fduro-ca.pem&connect_timeout=5",
		"host=db.example port=5432 dbname=duro user=writer sslmode=verify-full sslrootcert=/etc/ssl/duro-ca.pem",
	} {
		config, err := loadConfig(validStartupArgs(t, dsn))
		if err != nil {
			t.Fatalf("safe DSN %q: %v", dsn, err)
		}
		if config.token != "test-token" || config.dsn != dsn {
			t.Fatalf("trimmed config = %#v", config)
		}
	}
}

func TestHealthIsUnauthenticatedAndDoesNotOpenDatastores(t *testing.T) {
	server := httptest.NewServer(newServer(config{
		token:           "test-token",
		maxRequestBytes: 1024,
		queuePath:       filepath.Join(t.TempDir(), "queue.sqlite"),
		dsn:             "postgresql://writer@db.example/duro?sslmode=verify-full",
		push: func(*local.Store, string) (durosync.PushResult, error) {
			panic("health must not open a datastore")
		},
	}))
	defer server.Close()

	response, err := http.Get(server.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/json" || string(body) != `{"ok":true}` {
		t.Fatalf("health = status %d content-type %q body %q", response.StatusCode, response.Header.Get("Content-Type"), body)
	}
}

func TestAuthenticatedMCPWorksThroughConfiguredServer(t *testing.T) {
	server := httptest.NewServer(newServer(config{token: "test-token", maxRequestBytes: 1024}))
	defer server.Close()

	response := mcpRequest(t, server.URL, "test-token", map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "test", "version": "1"},
		},
	})
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("initialize status = %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func validStartupArgs(t *testing.T, dsn string) []string {
	t.Helper()
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	dsnFile := filepath.Join(dir, "postgres")
	if err := os.WriteFile(tokenFile, []byte("\n test-token \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dsnFile, []byte("\n "+dsn+" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return []string{
		"--listen", "100.64.0.1:8765",
		"--token-file", tokenFile,
		"--postgres-file", dsnFile,
		"--queue", filepath.Join(dir, "queue.sqlite"),
		"--writer-principal", "duro_mcp_writer",
		"--writer-surface", "claude-desktop",
		"--max-request-bytes", "2097152",
	}
}

func replaceStartupArg(args []string, name, value string) []string {
	copy := append([]string(nil), args...)
	for i := range copy {
		if copy[i] == name {
			copy[i+1] = value
			return copy
		}
	}
	panic("missing startup argument " + name)
}

func TestRejectsUnauthorized(t *testing.T) {
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 1024}))
	defer server.Close()

	for _, authorization := range []string{"", "Bearer wrong-token"} {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/mcp", strings.NewReader("{"))
		if err != nil {
			t.Fatal(err)
		}
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}

		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Errorf("Authorization %q: status = %d, want %d", authorization, response.StatusCode, http.StatusUnauthorized)
		}
	}
}

func TestRejectsAuthorizedForeignOriginBeforeMCP(t *testing.T) {
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 1024}))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/mcp", strings.NewReader("{"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Origin", "https://foreign.example")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusForbidden)
	}
}

func TestBlankMemoryScopeListsOnlyFileDocument(t *testing.T) {
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 1024, memoryScope: ""}))
	defer server.Close()

	response := mcpRequest(t, server.URL, "test-token", map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "test", "version": "1"},
		},
	})
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("initialize status = %d, want %d", response.StatusCode, http.StatusOK)
	}

	response = mcpRequest(t, server.URL, "test-token", map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/list",
	})
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("tools/list status = %d, want %d", response.StatusCode, http.StatusOK)
	}

	var payload struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Result.Tools) != 1 || payload.Result.Tools[0].Name != "file_document" {
		t.Fatalf("tools = %#v, want exactly file_document", payload.Result.Tools)
	}
}

func TestRejectsMalformedRequest(t *testing.T) {
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 1024}))
	defer server.Close()

	response := mcpRequest(t, server.URL, "test-token", "x")
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

func TestRejectsOversizedRequest(t *testing.T) {
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 32}))
	defer server.Close()

	response := mcpRequest(t, server.URL, "test-token", "{"+strings.Repeat(" ", 31)+"}")
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

func TestLimitBodyRejectsOversizedRequest(t *testing.T) {
	called := false
	handler := limitBody(32, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(strings.Repeat("x", 33)))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
	if called {
		t.Fatal("next handler was called for an oversized request")
	}
}

func TestFileDocumentStagesExactDocumentAndPushes(t *testing.T) {
	queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
	occurredAt := time.Date(2026, time.September, 5, 12, 34, 56, 123456789, time.FixedZone("-04", -4*60*60))
	const eventID = "123e4567-e89b-12d3-a456-426614174000"
	var calls int
	var got event.Event
	var gotBlob local.Blob

	server := httptest.NewServer(newHandler(config{
		token:           "test-token",
		maxRequestBytes: 2 << 20,
		queuePath:       queuePath,
		dsn:             "postgres://not-used-in-test",
		writerPrincipal: "writer",
		writerSurface:   "mcp",
		push: func(store *local.Store, _ string) (durosync.PushResult, error) {
			calls++
			pending, err := store.Pending()
			if err != nil {
				return durosync.PushResult{}, err
			}
			if len(pending) != 1 {
				return durosync.PushResult{}, fmt.Errorf("pending rows = %d, want 1", len(pending))
			}
			got = pending[0].Event
			blob, ok, err := store.PendingBlob(got.ID)
			if err != nil {
				return durosync.PushResult{}, err
			}
			if !ok {
				return durosync.PushResult{}, fmt.Errorf("staged blob is missing")
			}
			gotBlob = blob
			if err := store.MarkAccepted(got.ID, 41); err != nil {
				return durosync.PushResult{}, err
			}
			return durosync.PushResult{Accepted: 1}, nil
		},
	}))
	defer server.Close()

	result := callFileDocument(t, server.URL, "test-token", map[string]any{
		"source":      "https://example.test/document",
		"body":        "exact body\n",
		"media_type":  "text/plain; charset=utf-8",
		"event_id":    eventID,
		"occurred_at": occurredAt.Format(time.RFC3339Nano),
	})
	if result.IsError {
		t.Fatalf("file_document error: %s", result.Text)
	}
	var output fileDocumentOutput
	if err := json.Unmarshal([]byte(result.Text), &output); err != nil {
		t.Fatalf("decode tool output: %v", err)
	}
	if output.EventID != eventID || output.Sequence != 41 || !output.Fresh || output.Accepted != 1 || output.AlreadyPresent != 0 || output.Conflict != 0 || output.Rejected != 0 {
		t.Fatalf("output = %+v", output)
	}
	if calls != 1 {
		t.Fatalf("push calls = %d, want 1", calls)
	}
	if got.ID != eventID || got.EventType != "document.filed" || got.Actor != "writer" || !got.OccurredAt.Equal(occurredAt) {
		t.Fatalf("event = %+v", got)
	}
	var content map[string]string
	if err := json.Unmarshal(got.Content, &content); err != nil {
		t.Fatal(err)
	}
	if len(content) != 1 || content["source"] != "https://example.test/document" {
		t.Fatalf("content = %s", got.Content)
	}
	var refs struct {
		MCP struct {
			WriterPrincipal string `json:"writer_principal"`
			WriterSurface   string `json:"writer_surface"`
		} `json:"mcp"`
	}
	if err := json.Unmarshal(got.Refs, &refs); err != nil {
		t.Fatal(err)
	}
	if refs.MCP.WriterPrincipal != "writer" || refs.MCP.WriterSurface != "mcp" {
		t.Fatalf("refs = %s", got.Refs)
	}
	if string(gotBlob.Content) != "exact body\n" || gotBlob.MediaType != "text/plain; charset=utf-8" {
		t.Fatalf("blob = %+v", gotBlob)
	}
}

func TestFileDocumentStagingFailureDoesNotPush(t *testing.T) {
	var calls int
	server := httptest.NewServer(newHandler(config{
		token:           "test-token",
		maxRequestBytes: 2 << 20,
		queuePath:       filepath.Join(t.TempDir(), "missing", "queue.sqlite"),
		writerPrincipal: "writer",
		writerSurface:   "mcp",
		push: func(*local.Store, string) (durosync.PushResult, error) {
			calls++
			return durosync.PushResult{}, nil
		},
	}))
	defer server.Close()

	result := callFileDocument(t, server.URL, "test-token", validFileDocumentArgs())
	if !result.IsError {
		t.Fatal("staging failure reported success")
	}
	if calls != 0 {
		t.Fatalf("push calls = %d, want 0", calls)
	}
}

func TestFileDocumentPushFailuresAreToolErrorsAndRetainQueue(t *testing.T) {
	cases := []struct {
		name    string
		outcome durosync.PushResult
		err     error
	}{
		{name: "conflict", outcome: durosync.PushResult{Conflict: 1}},
		{name: "rejected", outcome: durosync.PushResult{Rejected: 1}},
		{name: "transport", err: fmt.Errorf("canonical unavailable")},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
			server := httptest.NewServer(newHandler(config{
				token:           "test-token",
				maxRequestBytes: 2 << 20,
				queuePath:       queuePath,
				writerPrincipal: "writer",
				writerSurface:   "mcp",
				push: func(*local.Store, string) (durosync.PushResult, error) {
					return test.outcome, test.err
				},
			}))
			defer server.Close()

			if result := callFileDocument(t, server.URL, "test-token", validFileDocumentArgs()); !result.IsError {
				t.Fatal("push failure reported success")
			}
			store, err := local.Open(queuePath)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			pending, err := store.Pending()
			if err != nil {
				t.Fatal(err)
			}
			if len(pending) != 1 || pending[0].Event.ID != validFileDocumentArgs()["event_id"] {
				t.Fatalf("pending = %#v, want original queued event", pending)
			}
		})
	}
}

func TestFileDocumentBackendErrorsAreRedacted(t *testing.T) {
	const secret = "backend-secret-9f1e"
	var logs bytes.Buffer
	originalLogOutput := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(originalLogOutput) })

	server := httptest.NewServer(newHandler(config{
		token:           "test-token",
		maxRequestBytes: 2 << 20,
		queuePath:       filepath.Join(t.TempDir(), "queue.sqlite"),
		writerPrincipal: "writer",
		writerSurface:   "mcp",
		push: func(*local.Store, string) (durosync.PushResult, error) {
			return durosync.PushResult{}, fmt.Errorf("canonical transport failed: %s", secret)
		},
	}))
	defer server.Close()

	result := callFileDocument(t, server.URL, "test-token", validFileDocumentArgs())
	if !result.IsError {
		t.Fatal("backend failure reported success")
	}
	if result.Text != "file_document: document unavailable" {
		t.Fatalf("client error = %q", result.Text)
	}
	if strings.Contains(result.Text, secret) {
		t.Fatalf("client error leaked backend detail: %q", result.Text)
	}
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("server log leaked backend detail: %q", logs.String())
	}
}

func TestFileDocumentMissingRecordedSequenceIsToolErrorAndRetainsQueue(t *testing.T) {
	queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
	server := httptest.NewServer(newHandler(config{
		token:           "test-token",
		maxRequestBytes: 2 << 20,
		queuePath:       queuePath,
		writerPrincipal: "writer",
		writerSurface:   "mcp",
		push: func(*local.Store, string) (durosync.PushResult, error) {
			return durosync.PushResult{Accepted: 1}, nil
		},
	}))
	defer server.Close()

	if result := callFileDocument(t, server.URL, "test-token", validFileDocumentArgs()); !result.IsError {
		t.Fatal("successful push without recorded sequence reported success")
	}
	store, err := local.Open(queuePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	pending, err := store.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Event.ID != validFileDocumentArgs()["event_id"] {
		t.Fatalf("pending = %#v, want original queued event", pending)
	}
}

func TestFileDocumentExactRetryIsNotDuplicated(t *testing.T) {
	queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
	var calls int
	server := httptest.NewServer(newHandler(config{
		token:           "test-token",
		maxRequestBytes: 2 << 20,
		queuePath:       queuePath,
		writerPrincipal: "writer",
		writerSurface:   "mcp",
		push: func(store *local.Store, _ string) (durosync.PushResult, error) {
			calls++
			pending, err := store.Pending()
			if err != nil {
				return durosync.PushResult{}, err
			}
			if len(pending) == 0 {
				return durosync.PushResult{AlreadyPresent: 1}, nil
			}
			for _, row := range pending {
				if err := store.MarkAccepted(row.Event.ID, 41); err != nil {
					return durosync.PushResult{}, err
				}
			}
			return durosync.PushResult{Accepted: len(pending)}, nil
		},
	}))
	defer server.Close()

	first := callFileDocument(t, server.URL, "test-token", validFileDocumentArgs())
	second := callFileDocument(t, server.URL, "test-token", validFileDocumentArgs())
	if first.IsError || second.IsError {
		t.Fatalf("retry results: first=%+v second=%+v", first, second)
	}
	var firstOutput, secondOutput fileDocumentOutput
	if err := json.Unmarshal([]byte(first.Text), &firstOutput); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(second.Text), &secondOutput); err != nil {
		t.Fatal(err)
	}
	if !firstOutput.Fresh || secondOutput.Fresh || firstOutput.EventID != secondOutput.EventID || firstOutput.Sequence != 41 || secondOutput.Sequence != firstOutput.Sequence || firstOutput.Accepted != 1 || secondOutput.Accepted != 0 || secondOutput.AlreadyPresent != 1 {
		t.Fatalf("retry outputs: first=%+v second=%+v", firstOutput, secondOutput)
	}
	if calls != 2 {
		t.Fatalf("push calls = %d, want 2", calls)
	}
	store, err := local.Open(queuePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	pending, err := store.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending rows = %d, want 0", len(pending))
	}
}

func TestFileDocumentChangedBodyConflictsAndPreservesOriginal(t *testing.T) {
	queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
	var calls int
	var canonicalBody string
	server := httptest.NewServer(newHandler(config{
		token:           "test-token",
		maxRequestBytes: 2 << 20,
		queuePath:       queuePath,
		writerPrincipal: "writer",
		writerSurface:   "mcp",
		push: func(store *local.Store, _ string) (durosync.PushResult, error) {
			calls++
			pending, err := store.Pending()
			if err != nil {
				return durosync.PushResult{}, err
			}
			for _, row := range pending {
				blob, ok, err := store.PendingBlob(row.Event.ID)
				if err != nil {
					return durosync.PushResult{}, err
				}
				if !ok {
					return durosync.PushResult{}, fmt.Errorf("staged blob is missing")
				}
				canonicalBody = string(blob.Content)
				if err := store.MarkAccepted(row.Event.ID, 41); err != nil {
					return durosync.PushResult{}, err
				}
			}
			return durosync.PushResult{Accepted: len(pending)}, nil
		},
	}))
	defer server.Close()

	if result := callFileDocument(t, server.URL, "test-token", validFileDocumentArgs()); result.IsError {
		t.Fatalf("first filing failed: %s", result.Text)
	}
	changed := validFileDocumentArgs()
	changed["body"] = "changed body"
	if result := callFileDocument(t, server.URL, "test-token", changed); !result.IsError {
		t.Fatal("changed body under the same event ID reported success")
	}
	if canonicalBody != "document body" {
		t.Fatalf("canonical body = %q, want original", canonicalBody)
	}
	if calls != 1 {
		t.Fatalf("push calls = %d, want 1", calls)
	}
	store, err := local.Open(queuePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, ok, err := store.PendingBlob(validFileDocumentArgs()["event_id"].(string)); err != nil || ok {
		t.Fatalf("accepted blob must be reclaimed: ok=%v err=%v", ok, err)
	}
}

func TestFileDocumentRejectsInvalidInputBeforeStaging(t *testing.T) {
	queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
	store, err := local.Open(queuePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var calls int
	server := httptest.NewServer(newHandler(config{
		token:           "test-token",
		maxRequestBytes: 3 << 20,
		queuePath:       queuePath,
		writerPrincipal: "writer",
		writerSurface:   "mcp",
		push: func(*local.Store, string) (durosync.PushResult, error) {
			calls++
			return durosync.PushResult{}, nil
		},
	}))
	defer server.Close()

	cases := []map[string]any{
		{"source": "", "body": "document body", "media_type": "text/plain", "event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z"},
		{"source": "source", "body": "", "media_type": "text/plain", "event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z"},
		{"source": strings.Repeat("s", 4097), "body": "document body", "media_type": "text/plain", "event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z"},
		{"source": "source", "body": strings.Repeat("b", 1<<20+1), "media_type": "text/plain", "event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z"},
		{"source": "source", "body": "document body", "media_type": strings.Repeat("m", 257), "event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z"},
		{"source": "source", "body": "document body", "media_type": "text/plain", "event_id": "not-a-uuid", "occurred_at": "2026-09-05T12:34:56.123456789Z"},
		{"source": "source", "body": "document body", "media_type": "text/plain", "event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "not-a-time"},
		{"source": "source", "body": "document body", "media_type": "text/plain", "event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "actor": "forged"},
	}
	for _, args := range cases {
		if result := callFileDocument(t, server.URL, "test-token", args); !result.IsError {
			t.Fatalf("invalid args reported success: %#v", args)
		}
	}
	if calls != 0 {
		t.Fatalf("push calls = %d, want 0", calls)
	}
	pending, err := store.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("staged %d invalid requests", len(pending))
	}
}

func TestParseFileDocumentRequestRejectsRawMalformedUTF8(t *testing.T) {
	arguments := append([]byte(`{"source":"source","body":"`), 0xff)
	arguments = append(arguments, []byte(`","media_type":"text/plain","event_id":"123e4567-e89b-12d3-a456-426614174000","occurred_at":"2026-09-05T12:34:56.123456789Z"}`)...)

	if _, _, err := parseFileDocumentRequest(arguments); err == nil {
		t.Fatal("raw malformed UTF-8 arguments were accepted")
	}
}

func TestFileDocumentRejectsRawMalformedUTF8OverMCPHTTP(t *testing.T) {
	var calls int
	server := httptest.NewServer(newHandler(config{
		token:           "test-token",
		maxRequestBytes: 2 << 20,
		queuePath:       filepath.Join(t.TempDir(), "queue.sqlite"),
		writerPrincipal: "writer",
		writerSurface:   "mcp",
		push: func(*local.Store, string) (durosync.PushResult, error) {
			calls++
			return durosync.PushResult{Accepted: 1}, nil
		},
	}))
	defer server.Close()

	payload := append([]byte(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"file_document","arguments":{"source":"source","body":"`), 0xff)
	payload = append(payload, []byte(`","media_type":"text/plain","event_id":"123e4567-e89b-12d3-a456-426614174000","occurred_at":"2026-09-05T12:34:56.123456789Z"}}}`)...)
	response := mcpRequest(t, server.URL, "test-token", string(payload))
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("tools/call status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var result struct {
		Result struct {
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if !result.Result.IsError {
		t.Fatal("raw malformed UTF-8 arguments reported success over MCP HTTP")
	}
	if calls != 0 {
		t.Fatalf("push calls = %d, want 0", calls)
	}
}

func TestFileDocumentSchemaHasOnlyDocumentInputs(t *testing.T) {
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 1024}))
	defer server.Close()
	response := mcpRequest(t, server.URL, "test-token", map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "tools/list",
	})
	defer response.Body.Close()
	var payload struct {
		Result struct {
			Tools []struct {
				InputSchema struct {
					Properties map[string]json.RawMessage `json:"properties"`
					Required   []string                   `json:"required"`
					Additional bool                       `json:"additionalProperties"`
				} `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Result.Tools) != 1 {
		t.Fatalf("tools = %#v", payload.Result.Tools)
	}
	schema := payload.Result.Tools[0].InputSchema
	if schema.Additional || len(schema.Properties) != 5 || len(schema.Required) != 5 {
		t.Fatalf("schema = %#v", schema)
	}
	required := make(map[string]bool, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = true
	}
	for _, name := range []string{"source", "body", "media_type", "event_id", "occurred_at"} {
		if _, ok := schema.Properties[name]; !ok {
			t.Fatalf("schema missing %q: %#v", name, schema.Properties)
		}
		if !required[name] {
			t.Fatalf("schema does not require %q: %#v", name, schema.Required)
		}
	}
}

func TestConfigTreatsBlankMemoryScopeAsDisabled(t *testing.T) {
	args := append(validStartupArgs(t, "postgresql://writer@db.example/duro?sslmode=verify-full"), "--memory-scope", " \t")
	configured, err := loadConfig(args)
	if err != nil || configured.memoryScope != "" {
		t.Fatalf("blank memory scope config = %#v err=%v", configured, err)
	}
}

func TestMemoryScopeListsExactlyMemoryTools(t *testing.T) {
	args := append(validStartupArgs(t, "postgresql://writer@db.example/duro?sslmode=verify-full"), "--memory-scope", "scope-a")
	configured, err := loadConfig(args)
	if err != nil || configured.memoryScope != "scope-a" {
		t.Fatalf("memory scope config = %#v err=%v", configured, err)
	}
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 1024, memoryScope: "scope-a"}))
	defer server.Close()
	response := mcpRequest(t, server.URL, "test-token", map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
	defer response.Body.Close()
	var payload struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, len(payload.Result.Tools))
	for _, tool := range payload.Result.Tools {
		got[tool.Name] = true
	}
	for _, name := range []string{"record_hermes_memory_turn", "redact_hermes_memory_turn", "recall_hermes_memory"} {
		if !got[name] {
			t.Fatalf("tools = %#v, missing %q", payload.Result.Tools, name)
		}
	}
	if got["file_document"] {
		t.Fatalf("tools = %#v, must not expose file_document in memory mode", payload.Result.Tools)
	}
	if len(got) != 3 {
		t.Fatalf("tools = %#v, want exactly three memory tools", payload.Result.Tools)
	}
}

func TestMemoryWritesStageCanonicalEventsWithoutBlobs(t *testing.T) {
	queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
	var got []event.Event
	server := httptest.NewServer(newHandler(config{
		token: "test-token", maxRequestBytes: 2 << 20, queuePath: queuePath, dsn: "postgres://not-used", writerPrincipal: "writer", writerSurface: "hermes", memoryScope: "scope-a",
		push: func(store *local.Store, _ string) (durosync.PushResult, error) {
			pending, err := store.Pending()
			if err != nil {
				return durosync.PushResult{}, err
			}
			for _, row := range pending {
				if _, ok, err := store.PendingBlob(row.Event.ID); err != nil || ok {
					return durosync.PushResult{}, fmt.Errorf("memory event blob: ok=%v err=%v", ok, err)
				}
				got = append(got, row.Event)
				if err := store.MarkAccepted(row.Event.ID, int64(40+len(got))); err != nil {
					return durosync.PushResult{}, err
				}
			}
			return durosync.PushResult{Accepted: len(pending)}, nil
		},
	}))
	defer server.Close()

	if result := callMCPTool(t, server.URL, "test-token", "record_hermes_memory_turn", validMemoryTurnArgs()); result.IsError {
		t.Fatalf("turn: %s", result.Text)
	}
	userOnly := validMemoryTurnArgs()
	userOnly["event_id"] = "323e4567-e89b-12d3-a456-426614174000"
	userOnly["user_text"] = "only user"
	userOnly["assistant_text"] = ""
	if result := callMCPTool(t, server.URL, "test-token", "record_hermes_memory_turn", userOnly); result.IsError {
		t.Fatalf("user-only turn: %s", result.Text)
	}
	assistantOnly := validMemoryTurnArgs()
	assistantOnly["event_id"] = "423e4567-e89b-12d3-a456-426614174000"
	assistantOnly["user_text"] = ""
	assistantOnly["assistant_text"] = "only assistant"
	if result := callMCPTool(t, server.URL, "test-token", "record_hermes_memory_turn", assistantOnly); result.IsError {
		t.Fatalf("assistant-only turn: %s", result.Text)
	}
	if result := callMCPTool(t, server.URL, "test-token", "redact_hermes_memory_turn", validMemoryRedactionArgs()); result.IsError {
		t.Fatalf("redaction: %s", result.Text)
	}
	if len(got) != 4 {
		t.Fatalf("events = %d, want 4", len(got))
	}
	for i, want := range []struct{ typ, content string }{
		{"hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"conversation-1","user_text":"remember alpha","assistant_text":"alpha answer"}`},
		{"hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"conversation-1","user_text":"only user","assistant_text":""}`},
		{"hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"conversation-1","user_text":"","assistant_text":"only assistant"}`},
		{"hermes.memory.redacted", `{"deployment_scope":"scope-a","target_event_id":"123e4567-e89b-12d3-a456-426614174000"}`},
	} {
		if got[i].EventType != want.typ || string(got[i].Content) != want.content {
			t.Fatalf("events = %#v", got)
		}
	}
	var content map[string]string
	if err := json.Unmarshal(got[0].Content, &content); err != nil {
		t.Fatal(err)
	}
	if len(content) != 4 || content["deployment_scope"] != "scope-a" || content["conversation_id"] != "conversation-1" || content["user_text"] != "remember alpha" || content["assistant_text"] != "alpha answer" {
		t.Fatalf("turn content = %s", got[0].Content)
	}
	if got[0].Actor != "writer" || !strings.Contains(string(got[0].Refs), `"writer_surface":"hermes"`) {
		t.Fatalf("provenance = event=%+v refs=%s", got[0], got[0].Refs)
	}
}

func TestMemoryWritesRejectInvalidAndForgedScopeBeforeStaging(t *testing.T) {
	queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
	var pushes int
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 3 << 20, queuePath: queuePath, writerPrincipal: "writer", writerSurface: "hermes", memoryScope: "scope-a", push: func(*local.Store, string) (durosync.PushResult, error) { pushes++; return durosync.PushResult{}, nil }}))
	defer server.Close()
	cases := []struct {
		tool string
		args map[string]any
	}{
		{"record_hermes_memory_turn", map[string]any{"event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "conversation_id": "c", "user_text": "", "assistant_text": ""}},
		{"record_hermes_memory_turn", map[string]any{"event_id": "123E4567-E89B-12D3-A456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "conversation_id": "c", "user_text": "x", "assistant_text": "y"}},
		{"record_hermes_memory_turn", map[string]any{"event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "not-a-time", "conversation_id": "c", "user_text": "x", "assistant_text": "y"}},
		{"record_hermes_memory_turn", map[string]any{"event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "conversation_id": strings.Repeat("c", 4097), "user_text": "x", "assistant_text": "y"}},
		{"record_hermes_memory_turn", map[string]any{"event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "conversation_id": "c", "user_text": strings.Repeat("x", 16<<10+1), "assistant_text": "y"}},
		{"record_hermes_memory_turn", map[string]any{"event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "conversation_id": "c", "user_text": "x", "assistant_text": "y", "deployment_scope": "forged"}},
		{"redact_hermes_memory_turn", map[string]any{"event_id": "223e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "target_event_id": "not-a-uuid"}},
	}
	for _, test := range cases {
		if result := callMCPTool(t, server.URL, "test-token", test.tool, test.args); !result.IsError {
			t.Fatalf("invalid request succeeded: %s %#v", test.tool, test.args)
		}
	}
	if pushes != 0 {
		t.Fatalf("pushes = %d, want 0", pushes)
	}
	store, err := local.Open(queuePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	pending, err := store.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("invalid input staged %#v", pending)
	}
}

func TestMemoryWriteBackendErrorsAreRedacted(t *testing.T) {
	const secret = "memory-write-secret-9f1e"
	var logs bytes.Buffer
	original := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(original) })
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 2 << 20, queuePath: filepath.Join(t.TempDir(), "queue.sqlite"), writerPrincipal: "writer", writerSurface: "hermes", memoryScope: "scope-a", push: func(*local.Store, string) (durosync.PushResult, error) {
		return durosync.PushResult{}, fmt.Errorf("canonical failure: %s", secret)
	}}))
	defer server.Close()
	result := callMCPTool(t, server.URL, "test-token", "record_hermes_memory_turn", validMemoryTurnArgs())
	if !result.IsError || strings.Contains(result.Text, secret) || strings.Contains(logs.String(), secret) {
		t.Fatalf("memory backend error/log leaked: result=%q logs=%q", result.Text, logs.String())
	}
}

func TestMemoryRecallUsesConfiguredScopeOverStatelessHTTP(t *testing.T) {
	var gotScope, gotQuery string
	var gotLimit int
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 1024, memoryScope: "scope-a", recall: func(_ string, scope, query string, limit int) (hermesmemory.RecallResponse, error) {
		gotScope, gotQuery, gotLimit = scope, query, limit
		return hermesmemory.RecallResponse{Results: []hermesmemory.Result{{EventID: "123e4567-e89b-12d3-a456-426614174000", Sequence: 9, ConversationID: "c", Excerpt: "alpha"}}}, nil
	}}))
	defer server.Close()
	result := callMCPTool(t, server.URL, "test-token", "recall_hermes_memory", map[string]any{"query": "alpha", "limit": 1})
	if result.IsError {
		t.Fatalf("recall: %s", result.Text)
	}
	if gotScope != "scope-a" || gotQuery != "alpha" || gotLimit != 1 || strings.Contains(result.Text, "scope-a") {
		t.Fatalf("recall scope/query/result = %q/%q/%d/%s", gotScope, gotQuery, gotLimit, result.Text)
	}
	if result := callMCPTool(t, server.URL, "test-token", "recall_hermes_memory", map[string]any{"query": "alpha", "limit": 1, "deployment_scope": "scope-b"}); !result.IsError {
		t.Fatal("forged recall scope succeeded")
	}
}

func TestMemoryRecallBackendErrorsAreRedacted(t *testing.T) {
	const secret = "recall-secret-9f1e"
	var logs bytes.Buffer
	original := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(original) })
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 1024, memoryScope: "scope-a", recall: func(string, string, string, int) (hermesmemory.RecallResponse, error) {
		return hermesmemory.RecallResponse{}, fmt.Errorf("canonical failure: %s", secret)
	}}))
	defer server.Close()
	result := callMCPTool(t, server.URL, "test-token", "recall_hermes_memory", map[string]any{"query": "alpha", "limit": 1})
	if !result.IsError || strings.Contains(result.Text, secret) || strings.Contains(logs.String(), secret) {
		t.Fatalf("recall backend error/log leaked: result=%q logs=%q", result.Text, logs.String())
	}
}

func TestMemoryRecallRejectsInvalidInput(t *testing.T) {
	var calls int
	server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 4096, memoryScope: "scope-a", recall: func(string, string, string, int) (hermesmemory.RecallResponse, error) {
		calls++
		return hermesmemory.RecallResponse{}, nil
	}}))
	defer server.Close()
	for _, args := range []map[string]any{{"query": "", "limit": 1}, {"query": strings.Repeat("q", 1025), "limit": 1}, {"query": "q", "limit": 0}, {"query": "q", "limit": 11}} {
		if result := callMCPTool(t, server.URL, "test-token", "recall_hermes_memory", args); !result.IsError {
			t.Fatalf("invalid recall succeeded: %#v", args)
		}
	}
	if calls != 0 {
		t.Fatalf("recall calls = %d", calls)
	}
}

func TestMemoryWritesRetryAndConflict(t *testing.T) {
	for _, test := range []struct {
		tool    string
		args    map[string]any
		changed string
	}{
		{"record_hermes_memory_turn", validMemoryTurnArgs(), "user_text"},
		{"redact_hermes_memory_turn", validMemoryRedactionArgs(), "target_event_id"},
	} {
		t.Run(test.tool, func(t *testing.T) {
			queuePath := filepath.Join(t.TempDir(), "queue.sqlite")
			server := httptest.NewServer(newHandler(config{token: "test-token", maxRequestBytes: 2 << 20, queuePath: queuePath, writerPrincipal: "writer", writerSurface: "hermes", memoryScope: "scope-a", push: func(store *local.Store, _ string) (durosync.PushResult, error) {
				pending, err := store.Pending()
				if err != nil {
					return durosync.PushResult{}, err
				}
				for _, row := range pending {
					if err := store.MarkAccepted(row.Event.ID, 41); err != nil {
						return durosync.PushResult{}, err
					}
				}
				if len(pending) == 0 {
					return durosync.PushResult{AlreadyPresent: 1}, nil
				}
				return durosync.PushResult{Accepted: len(pending)}, nil
			}}))
			defer server.Close()
			first := callMCPTool(t, server.URL, "test-token", test.tool, test.args)
			second := callMCPTool(t, server.URL, "test-token", test.tool, test.args)
			if first.IsError || second.IsError || !strings.Contains(first.Text, `"sequence":41`) || !strings.Contains(second.Text, `"sequence":41`) {
				t.Fatalf("retry = first=%+v second=%+v", first, second)
			}
			changed := make(map[string]any, len(test.args))
			for k, v := range test.args {
				changed[k] = v
			}
			changed[test.changed] = "333e4567-e89b-12d3-a456-426614174000"
			if test.changed == "user_text" {
				changed[test.changed] = "changed"
			}
			if result := callMCPTool(t, server.URL, "test-token", test.tool, changed); !result.IsError {
				t.Fatal("changed payload succeeded")
			}
		})
	}
}

func validMemoryTurnArgs() map[string]any {
	return map[string]any{"event_id": "123e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "conversation_id": "conversation-1", "user_text": "remember alpha", "assistant_text": "alpha answer"}
}
func validMemoryRedactionArgs() map[string]any {
	return map[string]any{"event_id": "223e4567-e89b-12d3-a456-426614174000", "occurred_at": "2026-09-05T12:34:56.123456789Z", "target_event_id": "123e4567-e89b-12d3-a456-426614174000"}
}
func callMCPTool(t *testing.T, serverURL, token, name string, arguments map[string]any) toolResult {
	t.Helper()
	response := mcpRequest(t, serverURL, token, map[string]any{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": map[string]any{"name": name, "arguments": arguments}})
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("tools/call status = %d", response.StatusCode)
	}
	var payload struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Result.Content) != 1 {
		t.Fatalf("tool content = %#v", payload.Result.Content)
	}
	return toolResult{Text: payload.Result.Content[0].Text, IsError: payload.Result.IsError}
}

type fileDocumentOutput struct {
	EventID        string `json:"event_id"`
	Sequence       int64  `json:"sequence"`
	Fresh          bool   `json:"fresh"`
	Accepted       int    `json:"accepted"`
	AlreadyPresent int    `json:"already_present"`
	Conflict       int    `json:"conflict"`
	Rejected       int    `json:"rejected"`
}

type toolResult struct {
	Text    string
	IsError bool
}

func validFileDocumentArgs() map[string]any {
	return map[string]any{
		"source":      "source",
		"body":        "document body",
		"media_type":  "text/plain",
		"event_id":    "123e4567-e89b-12d3-a456-426614174000",
		"occurred_at": "2026-09-05T12:34:56.123456789Z",
	}
}

func callFileDocument(t *testing.T, serverURL, token string, arguments map[string]any) toolResult {
	t.Helper()
	response := mcpRequest(t, serverURL, token, map[string]any{
		"jsonrpc": "2.0",
		"id":      3,
		"method":  "tools/call",
		"params":  map[string]any{"name": "file_document", "arguments": arguments},
	})
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("tools/call status = %d", response.StatusCode)
	}
	var payload struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Result.Content) != 1 {
		t.Fatalf("tool content = %#v", payload.Result.Content)
	}
	return toolResult{Text: payload.Result.Content[0].Text, IsError: payload.Result.IsError}
}

func mcpRequest(t *testing.T, serverURL, token string, payload any) *http.Response {
	t.Helper()

	var body io.Reader
	switch payload := payload.(type) {
	case string:
		body = strings.NewReader(payload)
	default:
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(http.MethodPost, serverURL+"/mcp", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
