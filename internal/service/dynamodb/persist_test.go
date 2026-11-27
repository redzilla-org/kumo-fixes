package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestItemCodecRoundTripEdgeValues pins the lossless contract of the compact
// encoding for the shapes JSON distinguishes: empty vs nil collections, nil
// pointers inside documents, false BOOL/NULL and binary values.
func TestItemCodecRoundTripEdgeValues(t *testing.T) {
	f := false
	item := Item{
		"pk":     {S: ptr("k")},
		"n":      {N: ptr("-1.5e3")},
		"emptyL": {L: []*AttributeValue{}},
		"emptyM": {M: map[string]*AttributeValue{}},
		"emptyS": {S: ptr("")},
		"ss":     {SS: []string{"a", "b"}},
		"ns":     {NS: []string{"1"}},
		"b":      {B: []byte{0, 1, 2}},
		"bs":     {BS: [][]byte{{9}, {}}},
		"bool":   {BOOL: &f},
		"null":   {NULL: ptr(true)},
		"zero":   {},
		"doc": {M: map[string]*AttributeValue{
			"nilptr": nil,
			"list":   {L: []*AttributeValue{{S: ptr("x")}, nil, {M: map[string]*AttributeValue{"deep": {N: ptr("7")}}}}},
		}},
	}

	got, err := decodeItem(encodeItem(item))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, item) {
		t.Fatalf("round trip changed item:\n got=%#v\nwant=%#v", got, item)
	}
}

// TestPersistenceRoundTripAndLegacyMigration covers the v2 per-table logs
// (puts, overwrites, deletes, GSI query after restart) and the one-time import
// of a v1 dynamodb.json.
func TestPersistenceRoundTripAndLegacyMigration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	store := NewMemoryStorage("http://localhost:4566", WithDataDir(dir))
	createProfileTable(t, store)

	for i := 0; i < 50; i++ {
		if _, err := store.PutItem(ctx, "profile", wideItem(i), false, ConditionInput{}); err != nil {
			t.Fatal(err)
		}
	}

	// Overwrite and delete must replay in order after restart.
	if _, err := store.UpdateItem(ctx, "profile", Item{"pk": wideItem(1)["pk"], "sk": {S: ptr("ROW")}},
		"SET StringField001 = :v", nil, map[string]AttributeValue{":v": {S: ptr("updated")}}, "", ConditionInput{}); err != nil {
		t.Fatal(err)
	}

	if _, err := store.DeleteItem(ctx, "profile", Item{"pk": wideItem(2)["pk"], "sk": {S: ptr("ROW")}}, false, ConditionInput{}); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	close(store.stopTTL)

	reloaded := NewMemoryStorage("http://localhost:4566", WithDataDir(dir))
	assertProfileState(t, reloaded, 49)

	// Export the reloaded state in the v1 shape, then import it fresh.
	legacy, err := reloaded.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	close(reloaded.stopTTL)

	legacyDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(legacyDir, "dynamodb.json"), legacy, 0o600); err != nil {
		t.Fatal(err)
	}

	migrated := NewMemoryStorage("http://localhost:4566", WithDataDir(legacyDir))
	t.Cleanup(func() { close(migrated.stopTTL) })
	assertProfileState(t, migrated, 49)

	if _, err := os.Stat(filepath.Join(legacyDir, "dynamodb.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy snapshot should be removed after migration: %v", err)
	}

	var meta persistedMeta

	raw, err := os.ReadFile(filepath.Join(legacyDir, persistDirName, metaName+".json"))
	if err != nil {
		t.Fatal(err)
	}

	if err := json.Unmarshal(raw, &meta); err != nil || meta.Version != persistVersion {
		t.Fatalf("meta not written as v%d: %v %+v", persistVersion, err, meta.Version)
	}
}

// assertProfileState checks row count, the overwrite, the delete and a GSI
// query served from the rebuilt projections.
func assertProfileState(t *testing.T, store *MemoryStorage, wantRows int) {
	t.Helper()

	ctx := context.Background()

	items, _, _, err := store.Scan(ctx, "profile", "", nil, nil, 0, nil, nil, nil)
	if err != nil || len(items) != wantRows {
		t.Fatalf("scan rows=%d err=%v, want %d", len(items), err, wantRows)
	}

	gone, err := store.GetItem(ctx, "profile", Item{"pk": wideItem(2)["pk"], "sk": {S: ptr("ROW")}})
	if err != nil || gone != nil {
		t.Fatalf("deleted row resurrected: %v %v", gone != nil, err)
	}

	got, _, _, err := store.Query(ctx, "profile", "byField", "StringField001 = :v", "", nil,
		map[string]AttributeValue{":v": {S: ptr("updated")}}, 0, nil, true)
	if err != nil || len(got) != 1 || *got[0]["pk"].S != *wideItem(1)["pk"].S {
		t.Fatalf("GSI query after reload: n=%d err=%v", len(got), err)
	}

	if !reflect.DeepEqual(got[0]["Media"], wideItem(1)["Media"]) {
		t.Fatal("nested list changed across persistence")
	}
}
