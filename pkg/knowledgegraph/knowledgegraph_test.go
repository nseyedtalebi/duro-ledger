package knowledgegraph

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/nseyedtalebi/duro-ledger/pkg/event"
	"github.com/nseyedtalebi/duro-ledger/pkg/postgres"
)

var eventTime = time.Unix(1, 0).UTC()

func mustEvent(t *testing.T, id, eventType, content string) event.Event {
	t.Helper()
	ev, err := event.New(id, eventType, "tester", eventTime, json.RawMessage(content), nil)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func TestApplyIsReplaySafeAndKeepsProvenance(t *testing.T) {
	fact := func(id string, sequence int64, content map[string]string) postgres.PulledEvent {
		body, err := json.Marshal(content)
		if err != nil {
			t.Fatal(err)
		}
		ev := mustEvent(t, id, "knowledge.fact", string(body))
		return postgres.PulledEvent{Sequence: sequence, Event: ev}
	}
	rows := []postgres.PulledEvent{
		{Sequence: 1, Event: mustEvent(t, "00000000-0000-0000-0000-000000000001", "other.type", `{"subject":"wrong"}`)},
		fact("00000000-0000-0000-0000-000000000002", 2, map[string]string{"subject": "Example", "predicate": "uses", "object": "Duro"}),
		fact("00000000-0000-0000-0000-000000000003", 3, map[string]string{"subject": "Example", "predicate": "uses", "object": "Duro"}),
		fact("00000000-0000-0000-0000-000000000004", 4, map[string]string{"subject": "Duro", "predicate": "projects", "object": "KG"}),
	}

	projection := New()
	if err := projection.Apply(rows); err != nil {
		t.Fatal(err)
	}
	if err := projection.Apply(rows); err != nil {
		t.Fatal(err)
	}

	want := []Triple{
		{Subject: "Example", Predicate: "uses", Object: "Duro", Sequence: 2, EventID: "00000000-0000-0000-0000-000000000002"},
		{Subject: "Duro", Predicate: "projects", Object: "KG", Sequence: 4, EventID: "00000000-0000-0000-0000-000000000004"},
	}
	if got := projection.Query("", "", ""); !reflect.DeepEqual(got, want) {
		t.Fatalf("Query() = %#v, want %#v", got, want)
	}
	if got := projection.Query("Example", "", ""); len(got) != 1 || got[0].EventID != "00000000-0000-0000-0000-000000000002" {
		t.Fatalf("subject query = %#v", got)
	}
}

func TestInvalidationRemovesTripleFromDefaultQuery(t *testing.T) {
	projection := New()
	rows := []postgres.PulledEvent{
		{Sequence: 1, Event: mustEvent(t, "00000000-0000-0000-0000-000000000006", "knowledge.fact", `{"subject":"Example","predicate":"uses","object":"Duro"}`)},
		{Sequence: 2, Event: mustEvent(t, "00000000-0000-0000-0000-000000000007", "knowledge.fact.invalidated", `{"subject":"Example","predicate":"uses","object":"Duro"}`)},
	}
	if err := projection.Apply(rows); err != nil {
		t.Fatal(err)
	}
	if got := projection.Query("", "", ""); len(got) != 0 {
		t.Fatalf("Query() = %#v, want invalidated fact hidden", got)
	}
}

func TestReassertionAfterInvalidationResetsProvenance(t *testing.T) {
	projection := New()
	rows := []postgres.PulledEvent{
		{Sequence: 1, Event: mustEvent(t, "00000000-0000-0000-0000-000000000008", "knowledge.fact", `{"subject":"Example","predicate":"uses","object":"Duro"}`)},
		{Sequence: 2, Event: mustEvent(t, "00000000-0000-0000-0000-000000000009", "knowledge.fact.invalidated", `{"subject":"Example","predicate":"uses","object":"Duro"}`)},
		{Sequence: 3, Event: mustEvent(t, "00000000-0000-0000-0000-00000000000a", "knowledge.fact", `{"subject":"Example","predicate":"uses","object":"Duro"}`)},
	}
	if err := projection.Apply(rows); err != nil {
		t.Fatal(err)
	}
	got := projection.Query("", "", "")
	if len(got) != 1 || got[0].Sequence != 3 || got[0].EventID != "00000000-0000-0000-0000-00000000000a" {
		t.Fatalf("Query() = %#v, want post-invalidation provenance", got)
	}
	if got := projection.Invalidated(); len(got) != 0 {
		t.Fatalf("Invalidated() = %#v, want empty after reassertion", got)
	}
}

func TestInvalidatedReturnsBothProvenances(t *testing.T) {
	projection := New()
	rows := []postgres.PulledEvent{
		{Sequence: 1, Event: mustEvent(t, "00000000-0000-0000-0000-00000000000b", "knowledge.fact", `{"subject":"Example","predicate":"uses","object":"Duro"}`)},
		{Sequence: 2, Event: mustEvent(t, "00000000-0000-0000-0000-00000000000c", "knowledge.fact.invalidated", `{"subject":"Example","predicate":"uses","object":"Duro"}`)},
	}
	if err := projection.Apply(rows); err != nil {
		t.Fatal(err)
	}
	got := projection.Invalidated()
	if len(got) != 1 || got[0].LastAssertion.Sequence != 1 || got[0].Invalidation.Sequence != 2 {
		t.Fatalf("Invalidated() = %#v, want both provenances", got)
	}
}

func TestInvalidationOfUnknownFactIsInert(t *testing.T) {
	projection := New()
	if err := projection.Apply([]postgres.PulledEvent{{
		Sequence: 1,
		Event:    mustEvent(t, "00000000-0000-0000-0000-00000000000d", "knowledge.fact.invalidated", `{"subject":"Example","predicate":"uses","object":"Duro"}`),
	}}); err != nil {
		t.Fatal(err)
	}
	if got := projection.Query("", "", ""); len(got) != 0 {
		t.Fatalf("Query() = %#v, want no visible fact", got)
	}
}

func TestMalformedInvalidationCanBeRetriedAfterFix(t *testing.T) {
	projection := New()
	bad := postgres.PulledEvent{
		Sequence: 1,
		Event:    mustEvent(t, "00000000-0000-0000-0000-00000000000e", "knowledge.fact.invalidated", `{"subject":"Example"}`),
	}
	if err := projection.Apply([]postgres.PulledEvent{bad}); err == nil {
		t.Fatal("expected malformed invalidation to fail")
	}
	bad.Event.Content = []byte(`{"subject":"Example","predicate":"uses","object":"Duro"}`)
	if err := projection.Apply([]postgres.PulledEvent{bad}); err != nil {
		t.Fatalf("retry after malformed event: %v", err)
	}
}

func TestApplyRejectsMalformedFact(t *testing.T) {
	projection := New()
	err := projection.Apply([]postgres.PulledEvent{{
		Sequence: 1,
		Event:    mustEvent(t, "00000000-0000-0000-0000-000000000005", "knowledge.fact", `{"subject":"Example"}`),
	}})
	if err == nil {
		t.Fatal("expected malformed fact to fail")
	}
}
