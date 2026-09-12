package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nseyedtalebi/duro-ledger/pkg/blobconfig"
	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/hermesmemory"
	"github.com/nseyedtalebi/duro-ledger/pkg/local"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
	durosync "github.com/nseyedtalebi/duro-ledger/pkg/sync"
)

type config struct {
	listen          string
	token           string
	maxRequestBytes int64
	queuePath       string
	dsn             string
	blobStore       string
	blobRoot        string
	writerPrincipal string
	writerSurface   string
	memoryScope     string
	push            pushFunc
	recall          memoryRecallFunc
}

type pushFunc func(*local.Store, string) (durosync.PushResult, error)
type memoryRecallFunc func(string, string, string, int) (hermesmemory.RecallResponse, error)

// ponytail: single-writer ceiling; replace this process-global lock with a
// per-queue/process-safe writer if throughput requires concurrent writers.
var queueMu sync.Mutex

const (
	maxSourceBytes    = 4 << 10
	maxBodyBytes      = 1 << 20
	maxMediaTypeBytes = 256
	backendToolError  = "file_document: document unavailable"
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 15 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second
)

const fileDocumentSchema = `{
	"type":"object",
	"additionalProperties":false,
	"required":["source","body","media_type","event_id","occurred_at"],
	"properties":{
		"source":{"type":"string","minLength":1,"maxLength":4096},
		"body":{"type":"string","minLength":1,"maxLength":1048576},
		"media_type":{"type":"string","minLength":1,"maxLength":256},
		"event_id":{"type":"string","format":"uuid"},
		"occurred_at":{"type":"string","format":"date-time"}
	}
}`

const memoryTurnSchema = `{"type":"object","additionalProperties":false,"required":["event_id","occurred_at","conversation_id","user_text","assistant_text"],"properties":{"event_id":{"type":"string","format":"uuid"},"occurred_at":{"type":"string","format":"date-time"},"conversation_id":{"type":"string","minLength":1,"maxLength":4096},"user_text":{"type":"string","maxLength":16384},"assistant_text":{"type":"string","maxLength":16384}}}`
const memoryRedactionSchema = `{"type":"object","additionalProperties":false,"required":["event_id","occurred_at","target_event_id"],"properties":{"event_id":{"type":"string","format":"uuid"},"occurred_at":{"type":"string","format":"date-time"},"target_event_id":{"type":"string","format":"uuid"}}}`
const memoryRecallSchema = `{"type":"object","additionalProperties":false,"required":["query","limit"],"properties":{"query":{"type":"string","minLength":1,"maxLength":1024},"limit":{"type":"integer","minimum":1,"maximum":10}}}`

type memoryTurnRequest struct {
	EventID        string `json:"event_id"`
	OccurredAt     string `json:"occurred_at"`
	ConversationID string `json:"conversation_id"`
	UserText       string `json:"user_text"`
	AssistantText  string `json:"assistant_text"`
}
type memoryRedactionRequest struct {
	EventID       string `json:"event_id"`
	OccurredAt    string `json:"occurred_at"`
	TargetEventID string `json:"target_event_id"`
}
type memoryRecallRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}
type memoryWriteResult struct {
	EventID        string `json:"event_id"`
	Sequence       int64  `json:"sequence"`
	Fresh          bool   `json:"fresh"`
	Accepted       int    `json:"accepted"`
	AlreadyPresent int    `json:"already_present"`
	Conflict       int    `json:"conflict"`
	Rejected       int    `json:"rejected"`
}

type fileDocumentRequest struct {
	Source     string `json:"source"`
	Body       string `json:"body"`
	MediaType  string `json:"media_type"`
	EventID    string `json:"event_id"`
	OccurredAt string `json:"occurred_at"`
}

type fileDocumentResult struct {
	EventID        string `json:"event_id"`
	Sequence       int64  `json:"sequence"`
	Fresh          bool   `json:"fresh"`
	Accepted       int    `json:"accepted"`
	AlreadyPresent int    `json:"already_present"`
	Conflict       int    `json:"conflict"`
	Rejected       int    `json:"rejected"`
}

func main() {
	if err := run(os.Args[1:], serveHTTP); err != nil {
		log.Fatal("duro-mcp: startup failed")
	}
}

func run(args []string, serve func(*http.Server) error) error {
	config, err := loadConfig(args)
	if err != nil {
		return err
	}
	return serve(newHTTPServer(config))
}

func serveHTTP(server *http.Server) error {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	return server.Serve(listener)
}

func loadConfig(args []string) (config, error) {
	var config config
	flags := flag.NewFlagSet("duro-mcp", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var tokenFile, postgresFile string
	flags.StringVar(&config.listen, "listen", "", "HTTP listen address")
	flags.StringVar(&tokenFile, "token-file", "", "bearer token file")
	flags.StringVar(&postgresFile, "postgres-file", "", "PostgreSQL DSN file")
	flags.StringVar(&config.queuePath, "queue", "", "SQLite queue path")
	flags.StringVar(&config.blobStore, "blob-store", blobconfig.PostgresKind, "canonical blob store: postgres or filesystem")
	flags.StringVar(&config.blobRoot, "blob-root", "", "absolute filesystem blob store root")
	flags.StringVar(&config.writerPrincipal, "writer-principal", "", "Duro writer principal")
	flags.StringVar(&config.writerSurface, "writer-surface", "", "Duro writer surface")
	flags.StringVar(&config.memoryScope, "memory-scope", "", "optional Hermes deployment memory scope")
	flags.Int64Var(&config.maxRequestBytes, "max-request-bytes", 2<<20, "maximum request body bytes")
	if err := flags.Parse(args); err != nil || len(flags.Args()) != 0 {
		return config, errors.New("invalid command configuration")
	}
	if err := rejectPostgresEnvironment(); err != nil {
		return config, err
	}
	config.listen = strings.TrimSpace(config.listen)
	config.queuePath = strings.TrimSpace(config.queuePath)
	config.blobStore = strings.TrimSpace(config.blobStore)
	config.blobRoot = strings.TrimSpace(config.blobRoot)
	config.writerPrincipal = strings.TrimSpace(config.writerPrincipal)
	config.writerSurface = strings.TrimSpace(config.writerSurface)
	config.memoryScope = strings.TrimSpace(config.memoryScope)
	if config.listen == "" || config.queuePath == "" || config.writerPrincipal == "" || config.writerSurface == "" || config.maxRequestBytes <= 0 {
		return config, errors.New("incomplete command configuration")
	}
	if err := blobconfig.Validate(config.blobStore, config.blobRoot); err != nil {
		return config, errors.New("invalid blob store configuration")
	}
	if err := validateListen(config.listen); err != nil {
		return config, err
	}
	if strings.TrimSpace(tokenFile) == "" || strings.TrimSpace(postgresFile) == "" {
		return config, errors.New("missing protected configuration file")
	}
	var err error
	if config.token, err = readProtectedFile(tokenFile); err != nil || config.token == "" {
		return config, errors.New("invalid token file")
	}
	if config.dsn, err = readProtectedFile(postgresFile); err != nil || config.dsn == "" {
		return config, errors.New("invalid PostgreSQL DSN file")
	}
	if err := validateDSN(config.dsn); err != nil {
		return config, err
	}
	return config, nil
}

func rejectPostgresEnvironment() error {
	for _, environment := range os.Environ() {
		name, _, _ := strings.Cut(environment, "=")
		if strings.HasPrefix(name, "PG") {
			return errors.New("PostgreSQL environment configuration is not permitted")
		}
	}
	return nil
}

func validateListen(listen string) error {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return errors.New("invalid listen address")
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return errors.New("invalid listen address")
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !netip.MustParsePrefix("100.64.0.0/10").Contains(address) && !netip.MustParsePrefix("fd7a:115c:a1e0::/48").Contains(address) {
		return errors.New("invalid listen address")
	}
	return nil
}

func readProtectedFile(path string) (string, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", errors.New("protected file unavailable")
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&^0o600 != 0 {
		return "", errors.New("protected file unavailable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return "", errors.New("protected file unavailable")
	}
	contents, err := io.ReadAll(file)
	if err != nil {
		return "", errors.New("protected file unavailable")
	}
	return strings.TrimSpace(string(contents)), nil
}

func validateDSN(dsn string) error {
	if strings.HasPrefix(strings.ToLower(dsn), "postgres://") || strings.HasPrefix(strings.ToLower(dsn), "postgresql://") {
		connection, err := url.Parse(dsn)
		if err != nil || !strings.EqualFold(connection.Scheme, "postgres") && !strings.EqualFold(connection.Scheme, "postgresql") {
			return errors.New("invalid PostgreSQL DSN")
		}
		if connection.User != nil {
			if _, hasPassword := connection.User.Password(); hasPassword {
				return errors.New("PostgreSQL DSN must not contain an inline password")
			}
		}
		query := connection.Query()
		for key := range query {
			if err := validateDSNOption(key); err != nil {
				return err
			}
		}
		if modes := query["sslmode"]; len(modes) != 1 || modes[0] != "verify-full" {
			return errors.New("PostgreSQL DSN must require sslmode=verify-full")
		}
		return nil
	}

	options, err := parseDSNOptions(dsn)
	if err != nil {
		return errors.New("invalid PostgreSQL DSN")
	}
	for key := range options {
		if err := validateDSNOption(key); err != nil {
			return err
		}
	}
	if options["sslmode"] != "verify-full" {
		return errors.New("PostgreSQL DSN must require sslmode=verify-full")
	}
	return nil
}

func validateDSNOption(key string) error {
	switch strings.ToLower(key) {
	case "password":
		return errors.New("PostgreSQL DSN must not contain an inline password")
	case "passfile", "service", "servicefile", "sslpassword":
		return errors.New("PostgreSQL DSN must not use credential indirection")
	}
	return nil
}

func parseDSNOptions(dsn string) (map[string]string, error) {
	options := make(map[string]string)
	for dsn != "" {
		dsn = strings.TrimLeft(dsn, " \t\n\r")
		if dsn == "" {
			break
		}
		keyEnd := strings.IndexAny(dsn, "= \t\n\r")
		if keyEnd <= 0 {
			return nil, errors.New("missing option key")
		}
		key := strings.ToLower(dsn[:keyEnd])
		dsn = strings.TrimLeft(dsn[keyEnd:], " \t\n\r")
		if !strings.HasPrefix(dsn, "=") {
			return nil, errors.New("missing option equals")
		}
		dsn = strings.TrimLeft(dsn[1:], " \t\n\r")
		var value string
		if strings.HasPrefix(dsn, "'") {
			var valueBuilder strings.Builder
			closed := false
			for i := 1; i < len(dsn); i++ {
				if dsn[i] == '\\' && i+1 < len(dsn) {
					i++
					valueBuilder.WriteByte(dsn[i])
					continue
				}
				if dsn[i] == '\'' {
					dsn = dsn[i+1:]
					closed = true
					break
				}
				valueBuilder.WriteByte(dsn[i])
			}
			if !closed {
				return nil, errors.New("unterminated quoted value")
			}
			value = valueBuilder.String()
		} else {
			valueEnd := strings.IndexAny(dsn, " \t\n\r")
			if valueEnd < 0 {
				value, dsn = dsn, ""
			} else {
				value, dsn = dsn[:valueEnd], dsn[valueEnd:]
			}
		}
		if _, exists := options[key]; exists {
			return nil, errors.New("duplicate option")
		}
		options[key] = value
	}
	return options, nil
}

func newHTTPServer(config config) *http.Server {
	return &http.Server{
		Addr:              config.listen,
		Handler:           newServer(config),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
}

func newServer(config config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"ok":true}`)
	})
	mux.Handle("/mcp", newHandler(config))
	return mux
}

func newHandler(config config) http.Handler {
	server := mcp.NewServer(&mcp.Implementation{Name: "duro-mcp", Version: "0"}, nil)
	if config.memoryScope == "" {
		server.AddTool(&mcp.Tool{
			Name:        "file_document",
			Description: "File a text document into Duro.",
			InputSchema: json.RawMessage(fileDocumentSchema),
		}, func(_ context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return fileDocument(config, request.Params.Arguments)
		})
	} else {
		server.AddTool(&mcp.Tool{Name: "record_hermes_memory_turn", Description: "Record a scoped Hermes memory turn.", InputSchema: json.RawMessage(memoryTurnSchema)}, func(_ context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return recordHermesMemoryTurn(config, request.Params.Arguments)
		})
		server.AddTool(&mcp.Tool{Name: "redact_hermes_memory_turn", Description: "Redact a scoped Hermes memory turn.", InputSchema: json.RawMessage(memoryRedactionSchema)}, func(_ context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return redactHermesMemoryTurn(config, request.Params.Arguments)
		})
		server.AddTool(&mcp.Tool{Name: "recall_hermes_memory", Description: "Recall scoped Hermes memory.", InputSchema: json.RawMessage(memoryRecallSchema)}, func(_ context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return recallHermesMemory(config, request.Params.Arguments)
		})
	}

	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{
		Stateless:           true,
		JSONResponse:        true,
		MaxRequestBodyBytes: config.maxRequestBytes,
	})
	return http.NewCrossOriginProtection().Handler(bearerAuth(config.token, limitBody(config.maxRequestBytes, handler)))
}

func fileDocument(config config, arguments json.RawMessage) (*mcp.CallToolResult, error) {
	request, occurredAt, err := parseFileDocumentRequest(arguments)
	if err != nil {
		return toolError(err), nil
	}
	content, err := json.Marshal(map[string]string{"source": request.Source})
	if err != nil {
		return toolError(err), nil
	}
	refs, err := json.Marshal(map[string]map[string]string{"mcp": {
		"writer_principal": config.writerPrincipal,
		"writer_surface":   config.writerSurface,
	}})
	if err != nil {
		return toolError(err), nil
	}
	ev, err := event.New(request.EventID, "document.filed", config.writerPrincipal, occurredAt, content, refs)
	if err != nil {
		return toolError(err), nil
	}

	queueMu.Lock()
	defer queueMu.Unlock()

	store, err := local.Open(config.queuePath)
	if err != nil {
		return backendError(config, "open queue", err), nil
	}
	defer store.Close()
	fresh, err := store.EnqueueWithBlob(ev, []byte(request.Body), request.MediaType)
	if err != nil {
		return backendError(config, "stage document", err), nil
	}

	push := config.push
	if push == nil {
		push = func(store *local.Store, dsn string) (durosync.PushResult, error) {
			return pushDocuments(store, dsn, config.blobStore, config.blobRoot)
		}
	}
	outcome, err := push(store, config.dsn)
	if err != nil {
		return backendError(config, "push document", err), nil
	}
	if outcome.Conflict != 0 || outcome.Rejected != 0 {
		return backendError(config, "canonical push result", fmt.Errorf("conflict=%d rejected=%d", outcome.Conflict, outcome.Rejected)), nil
	}
	sequence, ok, err := store.RemoteSequence(ev.ID)
	if err != nil {
		return backendError(config, "read canonical sequence", err), nil
	}
	if !ok {
		return backendError(config, "read canonical sequence", errors.New("push did not record sequence")), nil
	}
	result, err := json.Marshal(fileDocumentResult{
		EventID:        ev.ID,
		Sequence:       sequence,
		Fresh:          fresh,
		Accepted:       outcome.Accepted,
		AlreadyPresent: outcome.AlreadyPresent,
		Conflict:       outcome.Conflict,
		Rejected:       outcome.Rejected,
	})
	if err != nil {
		return toolError(err), nil
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(result)}}}, nil
}

func recordHermesMemoryTurn(config config, arguments json.RawMessage) (*mcp.CallToolResult, error) {
	var request memoryTurnRequest
	occurredAt, err := parseMemoryArguments("record_hermes_memory_turn", arguments, &request)
	if err != nil || request.ConversationID == "" || len(request.ConversationID) > 4<<10 || (request.UserText == "" && request.AssistantText == "") || len(request.UserText) > 16<<10 || len(request.AssistantText) > 16<<10 || !utf8.ValidString(request.ConversationID) || !utf8.ValidString(request.UserText) || !utf8.ValidString(request.AssistantText) {
		if err == nil {
			err = errors.New("invalid memory turn")
		}
		return toolError(fmt.Errorf("record_hermes_memory_turn: invalid arguments: %w", err)), nil
	}
	content, _ := json.Marshal(struct {
		Scope          string `json:"deployment_scope"`
		ConversationID string `json:"conversation_id"`
		UserText       string `json:"user_text"`
		AssistantText  string `json:"assistant_text"`
	}{config.memoryScope, request.ConversationID, request.UserText, request.AssistantText})
	return stageMemoryEvent(config, request.EventID, occurredAt, "hermes.memory.turn", content)
}

func redactHermesMemoryTurn(config config, arguments json.RawMessage) (*mcp.CallToolResult, error) {
	var request memoryRedactionRequest
	occurredAt, err := parseMemoryArguments("redact_hermes_memory_turn", arguments, &request)
	if err != nil || !canonicalUUID(request.TargetEventID) {
		if err == nil {
			err = errors.New("invalid target_event_id")
		}
		return toolError(fmt.Errorf("redact_hermes_memory_turn: invalid arguments: %w", err)), nil
	}
	content, _ := json.Marshal(struct {
		Scope         string `json:"deployment_scope"`
		TargetEventID string `json:"target_event_id"`
	}{config.memoryScope, request.TargetEventID})
	return stageMemoryEvent(config, request.EventID, occurredAt, "hermes.memory.redacted", content)
}

func stageMemoryEvent(config config, id string, occurredAt time.Time, eventType string, content json.RawMessage) (*mcp.CallToolResult, error) {
	refs, err := json.Marshal(map[string]map[string]string{"mcp": {"writer_principal": config.writerPrincipal, "writer_surface": config.writerSurface}})
	if err != nil {
		return toolError(err), nil
	}
	ev, err := event.New(id, eventType, config.writerPrincipal, occurredAt, content, refs)
	if err != nil {
		return toolError(err), nil
	}
	queueMu.Lock()
	defer queueMu.Unlock()
	store, err := local.Open(config.queuePath)
	if err != nil {
		return memoryBackendError("open queue", err), nil
	}
	defer store.Close()
	fresh, err := store.Enqueue(ev)
	if err != nil {
		return memoryBackendError("stage event", err), nil
	}
	push := config.push
	if push == nil {
		push = func(store *local.Store, dsn string) (durosync.PushResult, error) {
			return pushDocuments(store, dsn, config.blobStore, config.blobRoot)
		}
	}
	outcome, err := push(store, config.dsn)
	if err != nil {
		return memoryBackendError("push event", err), nil
	}
	if outcome.Conflict != 0 || outcome.Rejected != 0 {
		return memoryBackendError("canonical push result", errors.New("canonical failure")), nil
	}
	sequence, ok, err := store.RemoteSequence(ev.ID)
	if err != nil || !ok {
		return memoryBackendError("read canonical sequence", err), nil
	}
	result, err := json.Marshal(memoryWriteResult{EventID: ev.ID, Sequence: sequence, Fresh: fresh, Accepted: outcome.Accepted, AlreadyPresent: outcome.AlreadyPresent, Conflict: outcome.Conflict, Rejected: outcome.Rejected})
	if err != nil {
		return toolError(err), nil
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(result)}}}, nil
}

func recallHermesMemory(config config, arguments json.RawMessage) (*mcp.CallToolResult, error) {
	var request memoryRecallRequest
	if err := decodeMemoryArguments(arguments, &request); err != nil || !utf8.ValidString(request.Query) || len(request.Query) == 0 || len(request.Query) > 1024 || request.Limit < 1 || request.Limit > 10 {
		return toolError(errors.New("recall_hermes_memory: invalid arguments")), nil
	}
	recall := config.recall
	if recall == nil {
		recall = recallMemory
	}
	response, err := recall(config.dsn, config.memoryScope, request.Query, request.Limit)
	if err != nil {
		return memoryBackendError("recall", err), nil
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return toolError(err), nil
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}}}, nil
}

func recallMemory(dsn, scope, query string, limit int) (hermesmemory.RecallResponse, error) {
	store, err := postgres.Open(dsn)
	if err != nil {
		return hermesmemory.RecallResponse{}, err
	}
	defer store.Close()
	projection, err := hermesmemory.New(scope)
	if err != nil {
		return hermesmemory.RecallResponse{}, err
	}
	if err := projection.Rebuild(store); err != nil {
		return hermesmemory.RecallResponse{}, err
	}
	return projection.Recall(query, limit)
}

func parseMemoryArguments(tool string, arguments json.RawMessage, destination any) (time.Time, error) {
	if err := decodeMemoryArguments(arguments, destination); err != nil {
		return time.Time{}, err
	}
	var id, occurredAt string
	switch value := destination.(type) {
	case *memoryTurnRequest:
		id, occurredAt = value.EventID, value.OccurredAt
	case *memoryRedactionRequest:
		id, occurredAt = value.EventID, value.OccurredAt
	default:
		return time.Time{}, errors.New("invalid arguments")
	}
	if !canonicalUUID(id) {
		return time.Time{}, errors.New("event_id must be a canonical lowercase UUID")
	}
	parsed, err := time.Parse(time.RFC3339Nano, occurredAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("occurred_at must be RFC3339Nano: %w", err)
	}
	return parsed, nil
}

func decodeMemoryArguments(arguments json.RawMessage, destination any) error {
	if !utf8.Valid(arguments) {
		return errors.New("must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(arguments))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func canonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value
}

func memoryBackendError(operation string, _ error) *mcp.CallToolResult {
	log.Printf("duro-mcp: hermes memory %s failed", operation)
	return toolError(errors.New("hermes memory unavailable"))
}

func parseFileDocumentRequest(arguments json.RawMessage) (fileDocumentRequest, time.Time, error) {
	var request fileDocumentRequest
	if !utf8.Valid(arguments) {
		return fileDocumentRequest{}, time.Time{}, errors.New("file_document: invalid arguments: must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(arguments))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return fileDocumentRequest{}, time.Time{}, fmt.Errorf("file_document: invalid arguments: %w", err)
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return fileDocumentRequest{}, time.Time{}, fmt.Errorf("file_document: invalid arguments: %w", err)
	}
	if request.Source == "" || len(request.Source) > maxSourceBytes {
		return fileDocumentRequest{}, time.Time{}, fmt.Errorf("file_document: source must be between 1 and %d bytes", maxSourceBytes)
	}
	if request.Body == "" || len(request.Body) > maxBodyBytes || !utf8.ValidString(request.Body) {
		return fileDocumentRequest{}, time.Time{}, fmt.Errorf("file_document: body must be valid UTF-8 and between 1 and %d bytes", maxBodyBytes)
	}
	if request.MediaType == "" || len(request.MediaType) > maxMediaTypeBytes {
		return fileDocumentRequest{}, time.Time{}, fmt.Errorf("file_document: media_type must be between 1 and %d bytes", maxMediaTypeBytes)
	}
	id, err := uuid.Parse(request.EventID)
	if err != nil || id.String() != request.EventID {
		return fileDocumentRequest{}, time.Time{}, fmt.Errorf("file_document: event_id must be a canonical lowercase UUID")
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, request.OccurredAt)
	if err != nil {
		return fileDocumentRequest{}, time.Time{}, fmt.Errorf("file_document: occurred_at must be RFC3339Nano: %w", err)
	}
	return request, occurredAt, nil
}

func pushDocuments(store *local.Store, dsn, blobStore, blobRoot string) (durosync.PushResult, error) {
	if blobStore == "" {
		blobStore = blobconfig.PostgresKind
	}
	canonical, err := postgres.Open(dsn)
	if err != nil {
		return durosync.PushResult{}, err
	}
	defer canonical.Close()
	if blobStore == blobconfig.PostgresKind {
		return durosync.Push(store, canonical, 0)
	}
	backend, err := blobconfig.Open(blobStore, blobRoot, canonical)
	if err != nil {
		return durosync.PushResult{}, err
	}
	return durosync.PushWithBlobStore(store, canonical, backend, 0)
}

func toolError(err error) *mcp.CallToolResult {
	result := &mcp.CallToolResult{}
	result.SetError(err)
	return result
}

func backendError(_ config, operation string, _ error) *mcp.CallToolResult {
	log.Printf("duro-mcp: file_document %s failed", operation)
	return toolError(errors.New(backendToolError))
}

func bearerAuth(token string, next http.Handler) http.Handler {
	expected := []byte("Bearer " + token)
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), expected) != 1 {
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(response, request)
	})
}

func limitBody(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(response, request.Body, maxBytes))
		if err != nil {
			var maxBytesError *http.MaxBytesError
			if errors.As(err, &maxBytesError) {
				http.Error(response, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		request.Body.Close()
		request.Body = io.NopCloser(bytes.NewReader(body))
		next.ServeHTTP(response, request)
	})
}
