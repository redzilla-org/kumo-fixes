//go:build athena_native && cgo

package athena

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"unicode"

	polyglot "github.com/tobilg/polyglot/packages/go"
)

const translatorVersion = "0.12.0"

var nativeTranslator struct {
	once   sync.Once
	client *polyglot.Client
	err    error
}

// Load one explicitly packaged, version-matched FFI library for the process.
// No runtime downloads or per-query translator subprocesses are used.
func translator() (*polyglot.Client, error) {
	nativeTranslator.once.Do(func() {
		library := os.Getenv("POLYGLOT_SQL_FFI_PATH")
		if library == "" {
			nativeTranslator.err = fmt.Errorf("POLYGLOT_SQL_FFI_PATH is required for native Athena")
			return
		}
		client, err := polyglot.Open(library)
		if err != nil {
			nativeTranslator.err = fmt.Errorf("load Polyglot: %w", err)
			return
		}
		version, err := client.RuntimeVersion()
		if err != nil || version != translatorVersion {
			_ = client.Close()
			nativeTranslator.err = fmt.Errorf("Polyglot runtime must be %s (got %q, error %v)", translatorVersion, version, err)
			return
		}
		nativeTranslator.client = client
	})
	return nativeTranslator.client, nativeTranslator.err
}

// Isolate Athena-specific semantics before dialect generation. Unknown function
// names retain FILTER clauses and expand into our owned DuckDB compatibility macros.
var athenaFunctions = map[string]string{
	"max_by": "kumo_max_by", "regexp_extract": "kumo_regexp_extract",
	"regexp_replace": "kumo_regexp_replace", "to_iso8601": "kumo_to_iso8601", "fail": "kumo_fail",
}

func rewriteAthenaFunctions(sql string) (string, int, error) {
	var out strings.Builder
	filters := 0
	for i := 0; i < len(sql); {
		start := i
		if sql[i] == '\'' || sql[i] == '"' || sql[i] == '`' {
			quote := sql[i]
			i++
			closed := false
			for i < len(sql) {
				if sql[i] == quote {
					i++
					if i < len(sql) && sql[i] == quote {
						i++
						continue
					}
					closed = true
					break
				}
				i++
			}
			if !closed {
				return "", 0, fmt.Errorf("unterminated quoted SQL token")
			}
			out.WriteString(sql[start:i])
			continue
		}
		if strings.HasPrefix(sql[i:], "--") {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			out.WriteString(sql[start:i])
			continue
		}
		if strings.HasPrefix(sql[i:], "/*") {
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return "", 0, fmt.Errorf("unterminated SQL comment")
			}
			i += end + 4
			out.WriteString(sql[start:i])
			continue
		}
		if (sql[i] >= 'a' && sql[i] <= 'z') || (sql[i] >= 'A' && sql[i] <= 'Z') || sql[i] == '_' {
			for i < len(sql) && (unicode.IsLetter(rune(sql[i])) || unicode.IsDigit(rune(sql[i])) || sql[i] == '_') {
				i++
			}
			token := sql[start:i]
			next := i
			for next < len(sql) && unicode.IsSpace(rune(sql[next])) {
				next++
			}
			if next < len(sql) && sql[next] == '(' {
				if strings.EqualFold(token, "filter") {
					filters++
				}
				if replacement, ok := athenaFunctions[strings.ToLower(token)]; ok {
					token = replacement
				}
			}
			out.WriteString(token)
			continue
		}
		out.WriteByte(sql[i])
		i++
	}
	return out.String(), filters, nil
}

func translateSelect(query string) (string, error) {
	client, err := translator()
	if err != nil {
		return "", err
	}
	source, filters, err := rewriteAthenaFunctions(query)
	if err != nil {
		return "", err
	}
	statements, err := client.Transpile(source, "athena", "duckdb")
	if err != nil {
		return "", fmt.Errorf("translate Athena SQL: %w", err)
	}
	if len(statements) != 1 {
		return "", fmt.Errorf("exactly one SELECT statement is required")
	}
	translated := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(statements[0]), ";"))
	_, retained, err := rewriteAthenaFunctions(translated)
	if err != nil || filters != retained {
		return "", fmt.Errorf("translation did not preserve aggregate FILTER clauses")
	}
	return translated, nil
}
