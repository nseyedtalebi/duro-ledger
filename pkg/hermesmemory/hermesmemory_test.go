package hermesmemory

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

type memorySource struct {
	rows     []postgres.PulledEvent
	types    []string
	scope    string
	maxLimit int
	queries  []memoryQuery
}

type memoryQuery struct {
	after int64
	limit int
}

func (s *memorySource) ListEventsByTypeAndScope(types []string, scope string, after int64, limit int) ([]postgres.PulledEvent, error) {
	if s.maxLimit != 0 && limit > s.maxLimit {
		return nil, fmt.Errorf("page limit %d exceeds %d", limit, s.maxLimit)
	}
	s.types = append([]string(nil), types...)
	s.scope = scope
	s.queries = append(s.queries, memoryQuery{after: after, limit: limit})
	wanted := make(map[string]struct{}, len(types))
	for _, eventType := range types {
		wanted[eventType] = struct{}{}
	}
	var out []postgres.PulledEvent
	for _, row := range s.rows {
		var content map[string]string
		if _, ok := wanted[row.Event.EventType]; ok && json.Unmarshal(row.Event.Content, &content) == nil && content["deployment_scope"] == scope && row.Sequence > after {
			out = append(out, row)
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func memoryEvent(t *testing.T, sequence int64, id, eventType, content string) postgres.PulledEvent {
	t.Helper()
	ev, err := event.New(id, eventType, "writer", time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC), json.RawMessage(content), json.RawMessage(`{"mcp":{"writer_principal":"writer","writer_surface":"mcp"}}`))
	if err != nil {
		t.Fatal(err)
	}
	return postgres.PulledEvent{Sequence: sequence, Event: ev}
}

func TestProjectionRebuildsOnlyFixedScopeAndHidesRedactedTurns(t *testing.T) {
	source := &memorySource{rows: []postgres.PulledEvent{
		memoryEvent(t, 1, "11111111-1111-1111-1111-111111111111", "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c1","user_text":"alpha","assistant_text":"first"}`),
		memoryEvent(t, 2, "22222222-2222-2222-2222-222222222222", "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c2","user_text":"alpha alpha","assistant_text":"second"}`),
		memoryEvent(t, 3, "33333333-3333-3333-3333-333333333333", "hermes.memory.redacted", `{"deployment_scope":"scope-a","target_event_id":"11111111-1111-1111-1111-111111111111"}`),
		memoryEvent(t, 4, "44444444-4444-4444-4444-444444444444", "hermes.memory.turn", `{"deployment_scope":"scope-b","conversation_id":"other","user_text":"alpha alpha alpha","assistant_text":"other"}`),
	}}
	projection, err := New("scope-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Rebuild(source); err != nil {
		t.Fatal(err)
	}
	response, err := projection.Recall("alpha", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].EventID != "22222222-2222-2222-2222-222222222222" || response.Results[0].Sequence != 2 || response.Results[0].ConversationID != "c2" {
		t.Fatalf("results = %#v", response.Results)
	}
	if source.scope != "scope-a" || len(source.types) != 2 || source.types[0] != "hermes.memory.turn" || source.types[1] != "hermes.memory.redacted" {
		t.Fatalf("query = types=%q scope=%q", source.types, source.scope)
	}
}

func TestProjectionRanksLexicallyCapsExcerptsAndSignalsTruncation(t *testing.T) {
	rows := []postgres.PulledEvent{
		memoryEvent(t, 1, "11111111-1111-1111-1111-111111111111", "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c1","user_text":"alpha beta","assistant_text":"`+strings.Repeat("x", 600)+`"}`),
		memoryEvent(t, 2, "22222222-2222-2222-2222-222222222222", "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c2","user_text":"alpha alpha alpha","assistant_text":"plain"}`),
		memoryEvent(t, 3, "33333333-3333-3333-3333-333333333333", "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c3","user_text":"alpha beta","assistant_text":"newer"}`),
	}
	projection, err := New("scope-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Rebuild(&memorySource{rows: rows}); err != nil {
		t.Fatal(err)
	}
	response, err := projection.Recall("alpha beta", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 3 || response.Results[0].Sequence != 3 || response.Results[1].Sequence != 1 || response.Results[2].Sequence != 2 || len(response.Results[1].Excerpt) > 512 {
		t.Fatalf("ranked results = %#v", response.Results)
	}

	rows = make([]postgres.PulledEvent, 10000)
	for i := range rows {
		rows[i] = memoryEvent(t, int64(i+1), fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c","user_text":"alpha","assistant_text":"a"}`)
	}
	if err := projection.Rebuild(&memorySource{rows: rows}); err != nil {
		t.Fatal(err)
	}
	response, err = projection.Recall("alpha", 1)
	if err != nil || response.Truncated {
		t.Fatalf("bounded recall = %#v err=%v", response, err)
	}
}

func TestProjectionRecallWithTruncatedScopeWithholdsPossiblyRedactedTurns(t *testing.T) {
	rows := make([]postgres.PulledEvent, 0, 10001)
	for i := 1; i <= 10000; i++ {
		rows = append(rows, memoryEvent(t, int64(i), fmt.Sprintf("00000000-0000-4000-8000-%012d", i), "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c","user_text":"alpha","assistant_text":"a"}`))
	}
	target := "00000000-0000-4000-8000-000000010000"
	rows = append(rows, memoryEvent(t, 10001, "10000000-0000-4000-8000-000000000001", "hermes.memory.redacted", `{"deployment_scope":"scope-a","target_event_id":"`+target+`"}`))
	projection, err := New("scope-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Rebuild(&memorySource{rows: rows}); err != nil {
		t.Fatal(err)
	}
	response, err := projection.Recall("alpha", 1)
	if err != nil || !response.Truncated || len(response.Results) != 0 {
		t.Fatalf("truncated recall = %#v err=%v", response, err)
	}
}

func TestProjectionRebuildPagesAtStoreLimitAndProbesPastMaximum(t *testing.T) {
	rows := make([]postgres.PulledEvent, 10001)
	for i := range rows {
		rows[i] = memoryEvent(t, int64(i+1), fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1), "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c","user_text":"alpha","assistant_text":"a"}`)
	}
	source := &memorySource{rows: rows, maxLimit: 500}
	projection, err := New("scope-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Rebuild(source); err != nil {
		t.Fatal(err)
	}
	response, err := projection.Recall("alpha", 1)
	if err != nil || !response.Truncated || len(response.Results) != 0 {
		t.Fatalf("bounded recall = %#v err=%v", response, err)
	}
	if len(source.queries) != 21 || source.queries[20] != (memoryQuery{after: 10000, limit: 1}) {
		t.Fatalf("queries = %#v", source.queries)
	}
}

func TestProjectionRejectsMalformedMatchingEvents(t *testing.T) {
	source := &memorySource{rows: []postgres.PulledEvent{
		memoryEvent(t, 1, "11111111-1111-1111-1111-111111111111", "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"c1","user_text":"alpha"}`),
	}}
	projection, err := New("scope-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Rebuild(source); err == nil {
		t.Fatal("malformed matching event was accepted")
	}
}

func TestProjectionRejectsEmptyConversationID(t *testing.T) {
	source := &memorySource{rows: []postgres.PulledEvent{
		memoryEvent(t, 1, "11111111-1111-1111-1111-111111111111", "hermes.memory.turn", `{"deployment_scope":"scope-a","conversation_id":"","user_text":"alpha","assistant_text":"assistant"}`),
	}}
	projection, err := New("scope-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := projection.Rebuild(source); err == nil || err.Error() != "hermesmemory: malformed matching event" {
		t.Fatalf("Rebuild error = %v", err)
	}
	response, err := projection.Recall("alpha", 10)
	if err != nil || len(response.Results) != 0 {
		t.Fatalf("Recall response = %#v err=%v", response, err)
	}
}
