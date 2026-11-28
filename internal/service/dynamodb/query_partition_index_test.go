package dynamodb

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestQueryTimeIndependentOfUnrelatedPartitions reproduces the full-table
// Query: a Query walked every row, so its latency grew with table size. A one-row
// partition must cost about the same in a table 200x larger, on the base table
// and on a GSI, after the table is reloaded from its append-only log.
func TestQueryTimeIndependentOfUnrelatedPartitions(t *testing.T) {
	small := queryLatency(t, 1_000)
	large := queryLatency(t, 200_000)

	for name, s := range small {
		t.Logf("%s: 1k rows %v, 200k rows %v", name, s, large[name])

		// A full walk is ~200x slower; an index lookup stays flat.
		if large[name] > 20*s {
			t.Fatalf("%s Query scales with table size: %v -> %v", name, s, large[name])
		}
	}
}

// queryLatency fills a persisted table with one row per partition, reloads it,
// and returns the fastest of repeated one-partition Queries per access path.
func queryLatency(t *testing.T, rows int) map[string]time.Duration {
	t.Helper()

	ctx := context.Background()
	dir := t.TempDir()
	s := NewMemoryStorage("http://localhost:4566", WithDataDir(dir))

	_, err := s.CreateTable(ctx, &CreateTableRequest{
		TableName: "parts",
		KeySchema: []KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "sk", KeyType: "RANGE"}},
		AttributeDefinitions: []AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"}, {AttributeName: "sk", AttributeType: "N"},
			{AttributeName: "g", AttributeType: "S"},
		},
		GlobalSecondaryIndexes: []GlobalSecondaryIndex{{IndexName: "byG", KeySchema: []KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := range rows {
		item := Item{"pk": {S: ptr(fmt.Sprintf("p%d", i))}, "sk": {N: ptr("1")}, "g": {S: ptr(fmt.Sprintf("g%d", i))}}
		if _, err := s.PutItem(ctx, "parts", item, false, ConditionInput{}); err != nil {
			t.Fatal(err)
		}
	}

	// Reload so the index must be rebuilt from the log, not from live writes.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = NewMemoryStorage("http://localhost:4566", WithDataDir(dir))
	defer s.Close()

	paths := map[string][2]string{"base": {"", "pk = :v AND sk >= :n"}, "gsi": {"byG", "g = :v"}}
	values := map[string]map[string]AttributeValue{
		"base": {":v": {S: ptr("p7")}, ":n": {N: ptr("0")}},
		"gsi":  {":v": {S: ptr("g7")}},
	}

	best := map[string]time.Duration{}

	// Batches of 200 Queries: Windows' coarse clock cannot time a single one.
	const batch = 200

	for name, p := range paths {
		for range 5 {
			start := time.Now()

			for range batch {
				got, _, _, err := s.Query(ctx, "parts", p[0], p[1], "", nil, values[name], 0, nil, true)
				if err != nil {
					t.Fatal(err)
				}

				if len(got) != 1 {
					t.Fatalf("%s: want 1 item, got %d", name, len(got))
				}
			}

			perQuery := time.Since(start) / batch
			if best[name] == 0 || perQuery < best[name] {
				best[name] = perQuery
			}
		}
	}

	return best
}
