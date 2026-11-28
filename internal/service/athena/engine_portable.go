//go:build !athena_native || !cgo

package athena

import (
	"context"
	"errors"
)

const engineDescription = "Athena execution unavailable in portable build"

// Portable binaries retain the control-plane API but never invent query results.
func executeQuery(_ context.Context, _ *QueryExecution) (*ResultSet, int64, error) {
	return nil, 0, errors.New("Athena execution requires the athena_native CGO build and packaged Polyglot library")
}
