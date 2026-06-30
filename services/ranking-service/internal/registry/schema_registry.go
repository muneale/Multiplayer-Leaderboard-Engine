package registry

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
)

// SchemaRegistryClient is a lightweight client for Confluent Schema Registry
type SchemaRegistryClient struct {
	url        string
	httpClient *http.Client
	mu         sync.RWMutex
	idToSchema map[int]avro.Schema
	schemaToID map[string]int
}

func NewClient(url string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		url: url,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		idToSchema: make(map[int]avro.Schema),
		schemaToID: make(map[string]int),
	}
}

type registerResponse struct {
	ID int `json:"id"`
}

type schemaResponse struct {
	Schema string `json:"schema"`
}

// RegisterAvroSchema registers an Avro schema under a subject and returns the Schema ID
func (c *SchemaRegistryClient) RegisterAvroSchema(subject string, schemaStr string) (int, error) {
	c.mu.RLock()
	if id, exists := c.schemaToID[schemaStr]; exists {
		c.mu.RUnlock()
		return id, nil
	}
	c.mu.RUnlock()

	reqBody, err := json.Marshal(map[string]string{
		"schemaType": "AVRO",
		"schema":     schemaStr,
	})
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}

	resp, err := c.httpClient.Post(fmt.Sprintf("%s/subjects/%s/versions", c.url, subject), "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(reqBody))
	if err != nil {
		return 0, fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registry returned status %d: %s", resp.StatusCode, string(body))
	}

	var res registerResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}

	parsed, err := avro.Parse(schemaStr)
	if err != nil {
		return 0, fmt.Errorf("parse avro schema: %w", err)
	}

	c.mu.Lock()
	c.schemaToID[schemaStr] = res.ID
	c.idToSchema[res.ID] = parsed
	c.mu.Unlock()

	return res.ID, nil
}

// GetAvroSchema retrieves an Avro schema by Schema ID
func (c *SchemaRegistryClient) GetAvroSchema(id int) (avro.Schema, error) {
	c.mu.RLock()
	if schema, exists := c.idToSchema[id]; exists {
		c.mu.RUnlock()
		return schema, nil
	}
	c.mu.RUnlock()

	resp, err := c.httpClient.Get(fmt.Sprintf("%s/schemas/ids/%d", c.url, id))
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry returned status %d: %s", resp.StatusCode, string(body))
	}

	var res schemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	parsed, err := avro.Parse(res.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}

	c.mu.Lock()
	c.idToSchema[id] = parsed
	c.mu.Unlock()

	return parsed, nil
}

// EncodeAvro serializes data to Avro and prepends the Confluent wire format header
func (c *SchemaRegistryClient) EncodeAvro(schemaID int, schema avro.Schema, data interface{}) ([]byte, error) {
	serialized, err := avro.Marshal(schema, data)
	if err != nil {
		return nil, fmt.Errorf("avro marshal: %w", err)
	}

	buf := make([]byte, 5+len(serialized))
	buf[0] = 0x00 // Magic byte
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	copy(buf[5:], serialized)

	return buf, nil
}

// DecodeAvro parses the Confluent wire format header and deserializes the rest using the schema fetched from the registry.
// Returns (true, nil) if successfully decoded, (false, nil) if it is not in Confluent format, or an error.
func (c *SchemaRegistryClient) DecodeAvro(data []byte, target interface{}) (bool, error) {
	if len(data) < 5 || data[0] != 0x00 {
		return false, nil
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	schema, err := c.GetAvroSchema(schemaID)
	if err != nil {
		return false, fmt.Errorf("get avro schema: %w", err)
	}

	if err := avro.Unmarshal(schema, data[5:], target); err != nil {
		return false, fmt.Errorf("avro unmarshal: %w", err)
	}

	return true, nil
}
