package cloudformation

import (
	"encoding/json"
	"fmt"
)

// Query protocol struct lists arrive as arrays; the stack store uses keyed values.
// Keep accepting the existing JSON map representation for direct callers.
func normalizeParameterArray(body []byte) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("decode request fields: %w", err)
	}

	raw := fields["Parameters"]
	if len(raw) == 0 || raw[0] != '[' {
		return body, nil
	}

	var entries []struct {
		ParameterKey   string
		ParameterValue string
	}
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("decode Parameters array: %w", err)
	}

	parameters := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.ParameterKey == "" {
			return nil, fmt.Errorf("ParameterKey is required")
		}

		parameters[entry.ParameterKey] = entry.ParameterValue
	}

	encoded, err := json.Marshal(parameters)
	if err != nil {
		return nil, fmt.Errorf("encode Parameters map: %w", err)
	}

	fields["Parameters"] = encoded

	return json.Marshal(fields)
}
