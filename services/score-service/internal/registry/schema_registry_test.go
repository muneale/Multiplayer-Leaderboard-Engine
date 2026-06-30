package registry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type TestEvent struct {
	EventID   string    `avro:"event_id"`
	Score     float64   `avro:"score"`
	Timestamp time.Time `avro:"timestamp"`
}

const TestSchema = `{
	"type": "record",
	"name": "TestEvent",
	"namespace": "com.test",
	"fields": [
		{"name": "event_id", "type": "string"},
		{"name": "score", "type": "double"},
		{
			"name": "timestamp",
			"type": {
				"type": "long",
				"logicalType": "timestamp-millis"
			}
		}
	]
}`

func TestSchemaRegistryClient(t *testing.T) {
	// Create mock Schema Registry server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/subjects/test-subject/versions" && r.Method == "POST" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int{"id": 42})
			return
		}
		if r.URL.Path == "/schemas/ids/42" && r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": TestSchema})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(server.URL)

	// 1. Test Register
	id, err := client.RegisterAvroSchema("test-subject", TestSchema)
	if err != nil {
		t.Fatalf("failed to register schema: %v", err)
	}
	if id != 42 {
		t.Errorf("expected schema ID 42, got %d", id)
	}

	// 2. Test Get
	parsedSchema, err := client.GetAvroSchema(42)
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}

	// 3. Test Encode
	now := time.Now().UTC().Truncate(time.Millisecond) // avro timestamp-millis truncates to milliseconds
	event := TestEvent{
		EventID:   "evt-123",
		Score:     99.5,
		Timestamp: now,
	}

	encoded, err := client.EncodeAvro(42, parsedSchema, &event)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// 4. Test Decode
	var decodedEvent TestEvent
	ok, err := client.DecodeAvro(encoded, &decodedEvent)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if !ok {
		t.Errorf("expected decode to recognize confluent format")
	}

	if decodedEvent.EventID != event.EventID {
		t.Errorf("expected EventID %s, got %s", event.EventID, decodedEvent.EventID)
	}
	if decodedEvent.Score != event.Score {
		t.Errorf("expected Score %f, got %f", event.Score, decodedEvent.Score)
	}
	if !decodedEvent.Timestamp.Equal(event.Timestamp) {
		t.Errorf("expected Timestamp %v, got %v", event.Timestamp, decodedEvent.Timestamp)
	}

	// 5. Test JSON Fallback check
	var dummy TestEvent
	ok, err = client.DecodeAvro([]byte(`{"event_id": "evt-json"}`), &dummy)
	if err != nil {
		t.Fatalf("expected no error for non-confluent format, got %v", err)
	}
	if ok {
		t.Errorf("expected DecodeAvro to return false for non-confluent format")
	}
}
