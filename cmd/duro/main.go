// Command duro is the minimal client for Duro's PostgreSQL event ledger and
// filesystem artifact store: init provisions the schema, append records a
// generic event, and artifact put/get store and retrieve content-addressed
// files. There is no local queue, no sync/pull, and no subcommand
// framework beyond stdlib flag.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nseyedtalebi/duro-ledger/pkg/artifact"
	"github.com/nseyedtalebi/duro-ledger/pkg/cas"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

// errReported marks an error whose machine-readable form has already been
// printed to stderr by the caller; main must still exit nonzero for it but
// must not print it again.
var errReported = errors.New("duro: already reported")

const rootUsage = `Duro: a PostgreSQL event ledger and filesystem artifact store.

Usage:
  duro init      --postgres DSN [--writer ROLE] [--reader ROLE]
  duro append    --postgres DSN --type EVENT_TYPE [--content JSON] [--refs JSON]
  duro artifact put  --postgres DSN --root DIR --file PATH
  duro artifact get  --root DIR --sha256 sha256:HEX [--out PATH]
  duro help | --help | -h

Run "duro <command> --help", "duro artifact <subcommand> --help", or
"duro help <command>" for details on a specific command. See README.md for
the full contract, output schemas, error schemas, and operator setup (role
provisioning SQL, compose.yaml, PostgreSQL version requirements).

DSN flags fall back to DURO_POSTGRES_DSN; --root flags fall back to
DURO_CAS_ROOT.`

func main() {
	if err := run(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		if !errors.Is(err, errReported) {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Println(rootUsage)
		return nil
	}
	// "duro help <command...>" routes to that command's own --help output
	// instead of always printing the root usage, so e.g. "duro help init"
	// shows init's flags rather than the unrelated root summary.
	if args[0] == "help" && len(args) > 1 {
		return run(append(append([]string{}, args[1:]...), "--help"))
	}
	switch args[0] {
	case "help", "--help", "-h":
		fmt.Println(rootUsage)
		return nil
	case "init":
		return runInit(args[1:])
	case "append":
		return runAppend(args[1:])
	case "artifact":
		return runArtifact(args[1:])
	default:
		return fmt.Errorf("unknown command %q; run \"duro help\"", args[0])
	}
}

func envFallback(flagVal, envVar string) string {
	if flagVal != "" {
		return flagVal
	}
	return os.Getenv(envVar)
}

func postgresDSN(flagVal string) string { return envFallback(flagVal, "DURO_POSTGRES_DSN") }
func casRoot(flagVal string) string     { return envFallback(flagVal, "DURO_CAS_ROOT") }

// --- init ---

const initUsage = `Usage: duro init --postgres DSN [--writer ROLE] [--reader ROLE]

Applies Duro's canonical schema (a single "events" table) using an
administrator/provisioning connection. Idempotent: run again against an
already-initialized, contract-compliant database, it does nothing and
succeeds. Run against a database whose public.events table does not match
the current contract shape (for example, an older Duro schema), it returns
an error and makes no changes -- it never drops or alters existing data.

Flags:
  --postgres DSN   Administrator/provisioning PostgreSQL DSN (required).
                    Falls back to DURO_POSTGRES_DSN.
  --writer ROLE    Optional: grant an existing login ROLE the minimum
                    privilege to append events (column-level INSERT on
                    event_type/content/refs only; it cannot set id,
                    received_at, or actor).
  --reader ROLE    Optional: grant an existing login ROLE SELECT on the
                    event table and nothing else.

Output (stdout, on success): {"initialized":true}

Errors: connection failure, insufficient administrator privilege, or an
incompatible existing public.events table, printed to stderr with exit
code 1.

Example:
  duro init --postgres "postgres://admin@localhost:5432/duro" \
    --writer duro_writer --reader duro_reader`

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, initUsage) }
	dsn := fs.String("postgres", "", "administrator/provisioning PostgreSQL DSN")
	writer := fs.String("writer", "", "existing login role to grant writer (append-only) privilege")
	reader := fs.String("reader", "", "existing login role to grant reader (select-only) privilege")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return fmt.Errorf("init: unexpected argument(s): %v", fs.Args())
	}
	resolvedDSN := postgresDSN(*dsn)
	if resolvedDSN == "" {
		fs.Usage()
		return fmt.Errorf("init: --postgres is required (or set DURO_POSTGRES_DSN)")
	}
	if err := postgres.Initialize(resolvedDSN); err != nil {
		return err
	}
	if *writer != "" {
		if err := postgres.ProvisionWriter(resolvedDSN, *writer); err != nil {
			return fmt.Errorf("init: provisioning writer %q: %w", *writer, err)
		}
	}
	if *reader != "" {
		if err := postgres.ProvisionReader(resolvedDSN, *reader); err != nil {
			return fmt.Errorf("init: provisioning reader %q: %w", *reader, err)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]bool{"initialized": true})
}

// --- append ---

const appendUsage = `Usage: duro append --postgres DSN --type EVENT_TYPE [--content JSON] [--refs JSON]

Appends one event to the canonical ledger in a single database transaction.
PostgreSQL assigns id (UUIDv7), received_at, and actor (the authenticated
login role); this command cannot override them. Success is only reported
after the transaction commits.

Flags:
  --postgres DSN     PostgreSQL DSN (required). Falls back to
                      DURO_POSTGRES_DSN. An ordinary writer-role DSN is
                      sufficient; no administrator privilege is needed.
  --type EVENT_TYPE  Required. Nonblank, at most 255 UTF-8 bytes.
  --content JSON     Optional JSON object; defaults to {}. An explicit
                      JSON null is invalid. At most 1 MiB as UTF-8
                      jsonb::text.
  --refs JSON        Optional JSON object; defaults to {}. Same rules as
                      --content.

Output (stdout, on success): the complete stored event as JSON:
  {"id":"...","received_at":"...","event_type":"...","actor":"...",
   "content":{...},"refs":{...}}

Errors (stderr, exit code 1): validation failures (missing/oversized
--type, malformed --content/--refs) are reported as a plain message before
any database connection is opened. A confirmed transaction failure (e.g. a
value PostgreSQL itself rejects) and a lost connection with an uncertain
commit outcome are both reported as plain messages; the process exits
nonzero either way and performs no automatic retry. Repeating the same
--type/--content/--refs creates a new, distinct event -- retrying after any
failure is an explicit, separate invocation. If the failure was a lost
connection (uncertain/"unknown" outcome), the original append may have
actually committed despite never confirming; retrying anyway appends a
second event regardless, so an "unknown" outcome can leave two observation
events for what the caller intended as one call. Only retry if a duplicate
event is acceptable, or read the ledger first to check.

Example:
  duro append --postgres "postgres://writer@localhost:5432/duro" \
    --type document.tagged --content '{"tag":"reviewed"}'`

func runAppend(args []string) error {
	fs := flag.NewFlagSet("append", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, appendUsage) }
	dsn := fs.String("postgres", "", "PostgreSQL DSN")
	eventType := fs.String("type", "", "event type")
	content := fs.String("content", "", "event content, a JSON object (default {})")
	refs := fs.String("refs", "", "event refs, a JSON object (default {})")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return fmt.Errorf("append: unexpected argument(s): %v", fs.Args())
	}
	resolvedDSN := postgresDSN(*dsn)
	if resolvedDSN == "" || *eventType == "" {
		fs.Usage()
		return fmt.Errorf("append: --postgres (or DURO_POSTGRES_DSN) and --type are required")
	}

	var contentSet, refsSet bool
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "content":
			contentSet = true
		case "refs":
			refsSet = true
		}
	})
	n := event.New{
		EventType: *eventType,
		Content:   rawJSONFlag(*content, contentSet),
		Refs:      rawJSONFlag(*refs, refsSet),
	}
	if err := n.Validate(); err != nil {
		fs.Usage()
		return err
	}

	store, err := postgres.Open(resolvedDSN)
	if err != nil {
		return err
	}
	defer store.Close()

	se, err := store.Append(n)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(toEventJSON(se))
}

// rawJSONFlag returns val as the raw JSON a caller supplied for a --content
// or --refs flag, or nil if the flag was never set on the command line.
// event.New defaults a nil field to {}; an explicitly empty string ("--content
// ”") is preserved as-is so it fails JSON-object validation instead of being
// silently treated as omitted.
func rawJSONFlag(val string, set bool) json.RawMessage {
	if !set {
		return nil
	}
	return json.RawMessage(val)
}

// eventJSON is the wire shape of a stored event, shared by "append" and
// "artifact put" so the encoding lives in one place.
type eventJSON struct {
	ID         string          `json:"id"`
	ReceivedAt string          `json:"received_at"`
	EventType  string          `json:"event_type"`
	Actor      string          `json:"actor"`
	Content    json.RawMessage `json:"content"`
	Refs       json.RawMessage `json:"refs"`
}

func toEventJSON(se postgres.StoredEvent) eventJSON {
	return eventJSON{
		ID:         se.ID,
		ReceivedAt: se.ReceivedAt.Format(time.RFC3339Nano),
		EventType:  se.EventType,
		Actor:      se.Actor,
		Content:    se.Content,
		Refs:       se.Refs,
	}
}

// --- artifact ---

const artifactUsage = `Usage: duro artifact put|get --help

Subcommands:
  put   Store a file in the content-addressed filesystem store and record
         its observation as an event.
  get   Retrieve a stored file by its digest.

Run "duro artifact put --help" or "duro artifact get --help" for details.`

func runArtifact(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Println(artifactUsage)
		return nil
	}
	switch args[0] {
	case "put":
		return runArtifactPut(args[1:])
	case "get":
		return runArtifactGet(args[1:])
	default:
		return fmt.Errorf("unknown artifact subcommand %q; run \"duro artifact --help\"", args[0])
	}
}

const artifactPutUsage = `Usage: duro artifact put --postgres DSN --root DIR --file PATH

Streams --file into the content-addressed filesystem store rooted at
--root, then appends an artifact.observed event to the canonical ledger.
Both steps must succeed for a full success; see "Errors" below for what
happens when only the first does.

Identity is sha256:<64 lowercase hex characters>. Storage is content-
addressed and deduplicated: putting bytes that already exist verifies the
existing artifact and reuses it rather than storing a second copy, but
every successful put -- including a duplicate-content put -- appends its
own new artifact.observed event.

Flags:
  --postgres DSN   PostgreSQL DSN (required). Falls back to
                    DURO_POSTGRES_DSN.
  --root DIR       Content-addressed store root directory (required).
                    Falls back to DURO_CAS_ROOT. Created if missing.
  --file PATH      Source file to store (required). Must be a regular
                    file whose length does not change while it is being
                    read.

Output (stdout, on full success):
  {"digest":"sha256:...","size_bytes":N,
   "event":{"id":"...","received_at":"...","event_type":"artifact.observed",
            "actor":"...","content":{"digest":"sha256:...","size_bytes":N,
            "source_host":"...","source_path":"/abs/path"},"refs":{}}}

Errors:
  - If storing or verifying the bytes themselves fails (read/write error,
    length mismatch, or existing-artifact corruption at that digest) before
    anything is durably linked into the store, nothing is recorded: no
    artifact, no event. Reported as a plain message to stderr, exit code 1.
    In the narrow case where the bytes were already linked into place but a
    following directory-sync fails, the artifact MAY exist on disk even
    though this call reports a plain storage error and appended no event --
    content-addressing makes a later retry safe either way (see below).
  - If the bytes are stored (or verified as an existing duplicate) but the
    event append fails or its commit outcome is unknown, the artifact is
    retained on disk and a JSON error object is printed to stderr instead
    of a plain message, then the process exits 1:
      {"digest":"sha256:...","size_bytes":N,"artifact_stored":true,
       "event_outcome":"not_committed"|"unknown","error":"..."}
    Retrying "artifact put" with the same --file is always safe as far as
    the bytes go (they deduplicate against the digest already on disk) and
    appends a new observation event. If event_outcome was "unknown", the
    earlier append may have actually committed despite the lost
    acknowledgment; retrying still appends another event regardless, so an
    "unknown" outcome can leave two observation events on disk for what was
    intended as one put. Only retry if that duplicate is acceptable, or
    read the ledger first to check.

Side effects: creates files and up to two levels of sharding directories
under --root; no existing file under --root is ever overwritten.

Example:
  duro artifact put --postgres "postgres://writer@localhost:5432/duro" \
    --root /var/lib/duro/artifacts --file ./report.pdf`

func runArtifactPut(args []string) error {
	fs := flag.NewFlagSet("artifact put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, artifactPutUsage) }
	dsn := fs.String("postgres", "", "PostgreSQL DSN")
	root := fs.String("root", "", "content-addressed store root directory")
	file := fs.String("file", "", "source file to store")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return fmt.Errorf("artifact put: unexpected argument(s): %v", fs.Args())
	}
	resolvedDSN := postgresDSN(*dsn)
	resolvedRoot := casRoot(*root)
	if resolvedDSN == "" || resolvedRoot == "" || *file == "" {
		fs.Usage()
		return fmt.Errorf("artifact put: --postgres, --root (or their env fallbacks), and --file are required")
	}

	store, err := cas.Open(resolvedRoot)
	if err != nil {
		return err
	}
	ledger, err := postgres.Open(resolvedDSN)
	if err != nil {
		return err
	}
	defer ledger.Close()

	result, err := artifact.Put(store, ledger, *file)
	if err != nil {
		var pe *artifact.PutError
		if errors.As(err, &pe) {
			enc := json.NewEncoder(os.Stderr)
			_ = enc.Encode(struct {
				Digest         string `json:"digest"`
				Size           int64  `json:"size_bytes"`
				ArtifactStored bool   `json:"artifact_stored"`
				EventOutcome   string `json:"event_outcome"`
				Error          string `json:"error"`
			}{pe.Digest, pe.Size, pe.ArtifactStored, pe.EventOutcome, pe.Err.Error()})
			return errReported
		}
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Digest string    `json:"digest"`
		Size   int64     `json:"size_bytes"`
		Event  eventJSON `json:"event"`
	}{
		Digest: result.Digest,
		Size:   result.Size,
		Event:  toEventJSON(result.Event),
	})
}

const artifactGetUsage = `Usage: duro artifact get --root DIR --sha256 sha256:HEX [--out PATH]

Retrieves the artifact identified by --sha256 from the content-addressed
filesystem store rooted at --root, streaming and verifying its bytes as
they are read. Reads directly against the filesystem store; no PostgreSQL
connection is used or required.

Flags:
  --root DIR       Content-addressed store root directory (required).
                    Falls back to DURO_CAS_ROOT.
  --sha256 IDENT   Required. Artifact identity: sha256:<64 lowercase hex
                    characters>. Malformed identities are rejected.
  --out PATH       Optional destination file path. If omitted, bytes are
                    written to stdout. PATH must not already exist: an
                    existing destination is never overwritten.

Output:
  --out given, success: {"digest":"sha256:...","path":"..."} on stdout.
  --out omitted: the artifact's raw bytes on stdout. A terminal error
    (corruption, missing artifact, read failure, or interrupted transfer)
    is always reported with a nonzero exit even if some bytes were already
    written to stdout -- partial stdout output is never a valid artifact.

Errors (stderr, exit code 1): malformed digest, artifact not found,
verification failure (streamed bytes do not hash to the requested digest --
there is no separate size catalog to check against), or --out already
existing.

Example:
  duro artifact get --root /var/lib/duro/artifacts \
    --sha256 sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08 \
    --out ./report.pdf`

func runArtifactGet(args []string) error {
	fs := flag.NewFlagSet("artifact get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, artifactGetUsage) }
	root := fs.String("root", "", "content-addressed store root directory")
	digest := fs.String("sha256", "", "artifact identity, sha256:<hex>")
	out := fs.String("out", "", "destination file path; omit to write to stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return fmt.Errorf("artifact get: unexpected argument(s): %v", fs.Args())
	}
	resolvedRoot := casRoot(*root)
	if resolvedRoot == "" || *digest == "" {
		fs.Usage()
		return fmt.Errorf("artifact get: --root (or DURO_CAS_ROOT) and --sha256 are required")
	}
	// Validate the digest's shape before cas.Open, which creates directories
	// under --root as a side effect: a malformed --sha256 should fail
	// without ever touching the filesystem.
	if _, err := cas.ParseIdentity(*digest); err != nil {
		fs.Usage()
		return err
	}

	store, err := cas.Open(resolvedRoot)
	if err != nil {
		return err
	}

	if *out == "" {
		if err := artifact.GetToWriter(store, *digest, os.Stdout); err != nil {
			return err
		}
		return nil
	}
	if err := artifact.GetToFile(store, *digest, *out); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Digest string `json:"digest"`
		Path   string `json:"path"`
	}{*digest, *out})
}
