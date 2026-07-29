package domain

import (
	"encoding/json"
	"testing"
)

func TestRawBlockEnvelopeValidate(t *testing.T) {
	envelope := RawBlockEnvelope{
		SchemaVersion: RawBlockSchemaVersion,
		ChainID:       8453,
		BlockHash:     "0xabc",
		ParentHash:    "0xdef",
		Block:         json.RawMessage(`{}`),
	}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("expected valid envelope: %v", err)
	}
}
