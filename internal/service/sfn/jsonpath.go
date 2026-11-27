package sfn

import (
	"fmt"
	"strconv"
	"strings"
)

// resolveAnyJSONPath selects reference-path fields and array elements, including
// the ordered results of a Parallel state. Wildcards and expressions are not references.
func resolveAnyJSONPath(path string, data any) (any, error) {
	if path == "$" {
		return data, nil
	}

	if !strings.HasPrefix(path, "$") {
		return nil, fmt.Errorf("unsupported JSONPath %q: must start with $", path)
	}

	current := data

	for rest := path[1:]; rest != ""; {
		if rest[0] == '[' {
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return nil, fmt.Errorf("jsonPath %q: unclosed array index", path)
			}
			index, err := strconv.Atoi(rest[1:end])
			items, ok := current.([]any)
			if err != nil || !ok || index < 0 || index >= len(items) {
				return nil, fmt.Errorf("jsonPath %q: unavailable array index %q", path, rest[1:end])
			}
			current, rest = items[index], rest[end+1:]
			continue
		}
		if rest[0] != '.' {
			return nil, fmt.Errorf("unsupported reference JSONPath %q", path)
		}
		rest = rest[1:]
		end := strings.IndexAny(rest, ".[")
		if end < 0 {
			end = len(rest)
		}
		part := rest[:end]
		if part == "" {
			return nil, fmt.Errorf("jsonPath %q: empty field", path)
		}
		rest = rest[end:]
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("jsonPath %q: cannot access field %q on non-object", path, part)
		}

		val, exists := m[part]
		if !exists {
			return nil, fmt.Errorf("jsonPath %q: field %q not found", path, part)
		}

		current = val
	}

	return current, nil
}
