package cloudformation

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// The dispatcher retains parsed form values. Recover the exact strings before
// generic Query coercion can turn a CloudFormation parameter into a boolean/number.
func restoreQueryParameters(r *http.Request, body []byte) ([]byte, error) {
	parameters := make(map[string]string)

	for key, values := range r.Form {
		name, ok := strings.CutPrefix(key, "Parameters.member.")
		if !ok || !strings.HasSuffix(name, ".ParameterKey") {
			continue
		}

		index := strings.TrimSuffix(name, ".ParameterKey")
		if _, err := strconv.Atoi(index); err != nil || len(values) != 1 {
			return nil, fmt.Errorf("invalid parameter member %s", key)
		}

		parameters[values[0]] = r.Form.Get("Parameters.member." + index + ".ParameterValue")
	}

	if len(parameters) == 0 {
		return body, nil
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("decode Query request: %w", err)
	}

	encoded, err := json.Marshal(parameters)
	if err != nil {
		return nil, fmt.Errorf("encode Query parameters: %w", err)
	}

	fields["Parameters"] = encoded

	result, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("encode restored Query request: %w", err)
	}

	return result, nil
}
