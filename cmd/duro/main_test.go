package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nseyedtalebi/duro-ledger/internal/pgtest"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

// duroBin is the real compiled binary: the CLI contract is tested by running
// it, not by calling run() in-process.
var duroBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "duro-bin-*")
	if err != nil {
		panic(err)
	}
	duroBin = filepath.Join(dir, "duro")
	out, err := exec.Command("go", "build", "-o", duroBin, ".").CombinedOutput()
	if err != nil {
		os.RemoveAll(dir)
		panic("building duro: " + err.Error() + "\n" + string(out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	stdout, stderr string
	code           int
}

// duro runs the binary with a deliberately minimal environment, so
// environment-fallback behavior is exactly what each test sets.
func duro(t *testing.T, env []string, args ...string) result {
	t.Helper()
	cmd := exec.Command(duroBin, args...)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}, env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	code := 0
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("running duro %v: %v", args, err)
		}
		code = exitErr.ExitCode()
	}
	return result{stdout.String(), stderr.String(), code}
}

// TestHelpExitsZeroWithoutIO checks every help surface: no database, no
// store root, no environment, exit 0.
func TestHelpExitsZeroWithoutIO(t *testing.T) {
	cases := [][]string{
		{},
		{"help"},
		{"--help"},
		{"-h"},
		{"init", "--help"},
		{"append", "--help"},
		{"artifact"},
		{"artifact", "--help"},
		{"artifact", "put", "--help"},
		{"artifact", "get", "--help"},
		{"help", "init"},
		{"help", "append"},
		{"help", "artifact"},
		{"help", "artifact", "put"},
		{"help", "artifact", "get"},
	}
	for _, args := range cases {
		t.Run(strings.Join(append([]string{"duro"}, args...), " "), func(t *testing.T) {
			got := duro(t, nil, args...)
			if got.code != 0 {
				t.Errorf("exit code = %d (stderr: %s)", got.code, got.stderr)
			}
			if len(got.stdout)+len(got.stderr) < 40 {
				t.Errorf("help output too short: stdout %q stderr %q", got.stdout, got.stderr)
			}
			if !strings.Contains(got.stdout+got.stderr, "duro") {
				t.Errorf("help output does not mention the command: %q%q", got.stdout, got.stderr)
			}
		})
	}
}

func TestBadArgumentsRejected(t *testing.T) {
	cases := map[string][]string{
		"unknown command":             {"frobnicate"},
		"unknown artifact subcommand": {"artifact", "frobnicate"},
		"init positional":             {"init", "--postgres", "postgres://x/y", "extra"},
		"append positional":           {"append", "--postgres", "postgres://x/y", "--type", "t", "extra"},
		"put positional":              {"artifact", "put", "--postgres", "postgres://x/y", "--root", "/nonexistent/duro-store", "--file", "f", "extra"},
		"get positional":              {"artifact", "get", "--root", "/nonexistent/duro-store", "--sha256", "sha256:" + strings.Repeat("ab", 32), "extra"},
		"init missing dsn":            {"init"},
		"append missing dsn":          {"append", "--type", "t"},
		"append missing type":         {"append", "--postgres", "postgres://x/y"},
		"append blank type":           {"append", "--postgres", "postgres://x/y", "--type", "  \t "},
		"append explicit empty json":  {"append", "--postgres", "postgres://x/y", "--type", "t", "--content", ""},
		"append null content":         {"append", "--postgres", "postgres://x/y", "--type", "t", "--content", "null"},
		"append array refs":           {"append", "--postgres", "postgres://x/y", "--type", "t", "--refs", "[]"},
		"append unknown flag":         {"append", "--nope"},
		"put missing file":            {"artifact", "put", "--postgres", "postgres://x/y", "--root", "/nonexistent/duro-store"},
		"put missing root":            {"artifact", "put", "--postgres", "postgres://x/y", "--file", "f"},
		"get missing digest":          {"artifact", "get", "--root", "/nonexistent/duro-store"},
		"get malformed digest":        {"artifact", "get", "--root", "/nonexistent/duro-store", "--sha256", "deadbeef"},
		"get bare hex digest":         {"artifact", "get", "--root", "/nonexistent/duro-store", "--sha256", strings.Repeat("ab", 32)},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			got := duro(t, nil, args...)
			if got.code == 0 {
				t.Errorf("exit code = 0, want nonzero (stdout: %q)", got.stdout)
			}
			if got.stderr == "" {
				t.Error("nothing written to stderr")
			}
			if got.stdout != "" {
				t.Errorf("wrote to stdout on failure: %q", got.stdout)
			}
		})
	}
}

// malformedDigestTouchesNothing: rejecting --sha256 must not create the
// store root as a side effect.
func TestMalformedDigestCreatesNoStore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	got := duro(t, nil, "artifact", "get", "--root", root, "--sha256", "sha256:nope")
	if got.code == 0 {
		t.Fatal("malformed digest accepted")
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("store root was created for a rejected request: %v", err)
	}
}

func initializedDB(t *testing.T) string {
	t.Helper()
	dsn := pgtest.NewDatabase(t)
	got := duro(t, nil, "init", "--postgres", dsn)
	if got.code != 0 {
		t.Fatalf("duro init: exit %d, stderr %s", got.code, got.stderr)
	}
	var payload map[string]bool
	if err := json.Unmarshal([]byte(got.stdout), &payload); err != nil || !payload["initialized"] {
		t.Fatalf("init stdout = %q (%v)", got.stdout, err)
	}
	return dsn
}

// TestEndToEnd exercises the real CLI paths against a real database and a
// real store: init (twice), append, artifact put (twice), artifact get to a
// file and to stdout, plus a committed readback.
func TestEndToEnd(t *testing.T) {
	dsn := initializedDB(t)
	if got := duro(t, nil, "init", "--postgres", dsn); got.code != 0 {
		t.Fatalf("second init: exit %d, stderr %s", got.code, got.stderr)
	}

	got := duro(t, nil, "append", "--postgres", dsn, "--type", "document.tagged", "--content", `{"tag":"reviewed"}`)
	if got.code != 0 {
		t.Fatalf("append: exit %d, stderr %s", got.code, got.stderr)
	}
	var first eventJSON
	if err := json.Unmarshal([]byte(got.stdout), &first); err != nil {
		t.Fatalf("append stdout %q: %v", got.stdout, err)
	}
	if first.ID == "" || first.Actor == "" || first.ReceivedAt == "" || first.EventType != "document.tagged" {
		t.Errorf("append output missing database-assigned fields: %+v", first)
	}
	if string(first.Refs) != "{}" {
		t.Errorf("refs = %s, want {}", first.Refs)
	}
	// Repeating the same input creates a distinct event.
	repeat := duro(t, nil, "append", "--postgres", dsn, "--type", "document.tagged", "--content", `{"tag":"reviewed"}`)
	var second eventJSON
	if err := json.Unmarshal([]byte(repeat.stdout), &second); err != nil {
		t.Fatalf("repeat append stdout %q: %v", repeat.stdout, err)
	}
	if second.ID == first.ID {
		t.Error("repeated append reused an id")
	}

	// artifact put, twice: the second reuses the bytes and appends a second
	// observation event.
	root := filepath.Join(t.TempDir(), "store")
	data := bytes.Repeat([]byte("cli artifact "), 1000)
	src := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	identity := "sha256:" + hex.EncodeToString(sum[:])

	var receipts []struct {
		Digest string    `json:"digest"`
		Size   int64     `json:"size_bytes"`
		Event  eventJSON `json:"event"`
	}
	for i := range 2 {
		put := duro(t, nil, "artifact", "put", "--postgres", dsn, "--root", root, "--file", src)
		if put.code != 0 {
			t.Fatalf("put %d: exit %d, stderr %s", i, put.code, put.stderr)
		}
		var receipt struct {
			Digest string    `json:"digest"`
			Size   int64     `json:"size_bytes"`
			Event  eventJSON `json:"event"`
		}
		if err := json.Unmarshal([]byte(put.stdout), &receipt); err != nil {
			t.Fatalf("put %d stdout %q: %v", i, put.stdout, err)
		}
		if receipt.Digest != identity || receipt.Size != int64(len(data)) {
			t.Errorf("put %d receipt = %s / %d", i, receipt.Digest, receipt.Size)
		}
		if receipt.Event.EventType != "artifact.observed" {
			t.Errorf("put %d event_type = %q", i, receipt.Event.EventType)
		}
		if !strings.Contains(string(receipt.Event.Content), src) {
			t.Errorf("put %d content %s missing source_path %s", i, receipt.Event.Content, src)
		}
		receipts = append(receipts, receipt)
	}
	if receipts[0].Event.ID == receipts[1].Event.ID {
		t.Error("duplicate-content put reused the observation event")
	}

	// get to a file, then refuse to overwrite it; then get to stdout.
	out := filepath.Join(t.TempDir(), "restored.bin")
	if g := duro(t, nil, "artifact", "get", "--root", root, "--sha256", identity, "--out", out); g.code != 0 {
		t.Fatalf("get --out: exit %d, stderr %s", g.code, g.stderr)
	}
	restored, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(restored, data) {
		t.Fatalf("restored bytes differ (%v)", err)
	}
	if g := duro(t, nil, "artifact", "get", "--root", root, "--sha256", identity, "--out", out); g.code == 0 {
		t.Error("get overwrote an existing destination")
	}
	if g := duro(t, nil, "artifact", "get", "--root", root, "--sha256", identity); g.code != 0 || g.stdout != string(data) {
		t.Errorf("get to stdout: exit %d, %d bytes", g.code, len(g.stdout))
	}
	// A missing artifact fails even though the store root exists.
	absent := sha256.Sum256([]byte("never stored"))
	if g := duro(t, nil, "artifact", "get", "--root", root, "--sha256", "sha256:"+hex.EncodeToString(absent[:])); g.code == 0 {
		t.Error("get of an absent artifact succeeded")
	}

	// Committed readback: 2 appends + 2 observation events.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var total, observed int
	if err := db.QueryRow(`SELECT count(*), count(*) FILTER (WHERE event_type = 'artifact.observed') FROM public.events`).Scan(&total, &observed); err != nil {
		t.Fatal(err)
	}
	if total != 4 || observed != 2 {
		t.Errorf("ledger holds %d events (%d observations), want 4 (2)", total, observed)
	}
}

func TestEnvFallbackAndFlagPrecedence(t *testing.T) {
	dsn := initializedDB(t)
	root := filepath.Join(t.TempDir(), "store")
	src := filepath.Join(t.TempDir(), "env.bin")
	if err := os.WriteFile(src, []byte("env fallback"), 0o644); err != nil {
		t.Fatal(err)
	}
	env := []string{"DURO_POSTGRES_DSN=" + dsn, "DURO_CAS_ROOT=" + root}

	if got := duro(t, env, "append", "--type", "env.fallback"); got.code != 0 {
		t.Errorf("append via env fallback: exit %d, stderr %s", got.code, got.stderr)
	}
	if got := duro(t, env, "artifact", "put", "--file", src); got.code != 0 {
		t.Errorf("put via env fallback: exit %d, stderr %s", got.code, got.stderr)
	}

	// Flags win over the environment: a broken env plus a good flag works,
	// and a good env plus a broken flag fails.
	broken := []string{"DURO_POSTGRES_DSN=postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=2", "DURO_CAS_ROOT=/nonexistent/duro-root"}
	if got := duro(t, broken, "append", "--postgres", dsn, "--type", "flag.wins"); got.code != 0 {
		t.Errorf("flag should override a broken env DSN: exit %d, stderr %s", got.code, got.stderr)
	}
	if got := duro(t, env, "append", "--postgres", "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=2", "--type", "flag.wins"); got.code == 0 {
		t.Error("a broken --postgres flag should not fall back to the environment")
	}
	if got := duro(t, broken, "artifact", "get", "--root", root, "--sha256", "sha256:"+strings.Repeat("ab", 32)); got.code == 0 {
		t.Error("get of an absent digest succeeded")
	} else if strings.Contains(got.stderr, "nonexistent") {
		t.Errorf("--root flag did not override DURO_CAS_ROOT: %s", got.stderr)
	}
}

// TestPartialOutcomeErrorSchema drives the real stored-artifact/failed-append
// boundary: a reader-only DSN can stage and publish bytes but cannot append,
// so the CLI must report the machine-readable partial outcome and keep the
// artifact.
func TestPartialOutcomeErrorSchema(t *testing.T) {
	adminDSN := initializedDB(t)
	reader, readerDSN := pgtest.NewRole(t, adminDSN)
	if err := postgres.ProvisionReader(adminDSN, reader); err != nil {
		t.Fatalf("ProvisionReader: %v", err)
	}
	root := filepath.Join(t.TempDir(), "store")
	data := []byte("stored without an event")
	src := filepath.Join(t.TempDir(), "orphan.bin")
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	identity := "sha256:" + hex.EncodeToString(sum[:])

	got := duro(t, nil, "artifact", "put", "--postgres", readerDSN, "--root", root, "--file", src)
	if got.code == 0 {
		t.Fatal("put with a reader-only DSN succeeded")
	}
	if got.stdout != "" {
		t.Errorf("wrote a success receipt to stdout: %q", got.stdout)
	}
	var reported struct {
		Digest         string `json:"digest"`
		Size           int64  `json:"size_bytes"`
		ArtifactStored bool   `json:"artifact_stored"`
		EventOutcome   string `json:"event_outcome"`
		Error          string `json:"error"`
	}
	if err := json.Unmarshal([]byte(got.stderr), &reported); err != nil {
		t.Fatalf("stderr is not the documented JSON error object: %q (%v)", got.stderr, err)
	}
	if reported.Digest != identity || reported.Size != int64(len(data)) || !reported.ArtifactStored {
		t.Errorf("partial outcome = %+v", reported)
	}
	if reported.EventOutcome != "not_committed" {
		t.Errorf("event_outcome = %q, want not_committed (the server refused the insert)", reported.EventOutcome)
	}
	if reported.Error == "" {
		t.Error("error field is empty")
	}
	// The artifact is retained: a later get succeeds.
	out := filepath.Join(t.TempDir(), "retained.bin")
	if g := duro(t, nil, "artifact", "get", "--root", root, "--sha256", identity, "--out", out); g.code != 0 {
		t.Fatalf("retained artifact unreadable: exit %d, stderr %s", g.code, g.stderr)
	}
	if restored, err := os.ReadFile(out); err != nil || !bytes.Equal(restored, data) {
		t.Errorf("retained artifact differs (%v)", err)
	}
}

// TestInitProvisionsRoles covers init's optional role flags end to end,
// including its refusal to "secure" a role it cannot restrict.
func TestInitProvisionsRoles(t *testing.T) {
	adminDSN := initializedDB(t)
	writer, writerDSN := pgtest.NewRole(t, adminDSN)
	reader, readerDSN := pgtest.NewRole(t, adminDSN)
	if got := duro(t, nil, "init", "--postgres", adminDSN, "--writer", writer, "--reader", reader); got.code != 0 {
		t.Fatalf("init with roles: exit %d, stderr %s", got.code, got.stderr)
	}
	appended := duro(t, nil, "append", "--postgres", writerDSN, "--type", "writer.cli.append")
	if appended.code != 0 {
		t.Fatalf("writer append: exit %d, stderr %s", appended.code, appended.stderr)
	}
	var se eventJSON
	if err := json.Unmarshal([]byte(appended.stdout), &se); err != nil {
		t.Fatal(err)
	}
	if se.Actor != writer {
		t.Errorf("actor = %q, want %q", se.Actor, writer)
	}
	if got := duro(t, nil, "append", "--postgres", readerDSN, "--type", "reader.cli.append"); got.code == 0 {
		t.Error("reader was allowed to append through the CLI")
	}
	// The administrator/owner role cannot be provisioned as a writer.
	var owner string
	db, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(`SELECT session_user`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if got := duro(t, nil, "init", "--postgres", adminDSN, "--writer", owner); got.code == 0 {
		t.Error("init provisioned the owner role as a writer")
	}
}
