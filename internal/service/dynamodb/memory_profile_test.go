package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// wideItem builds a synthetic wide row: ~300 attributes counting nested ones,
// with nested M/L, ~9 KB as DynamoDB-JSON. WHY: a bulk fill of such rows
// measured ~190 KiB of Kumo heap per row; this reproduces that shape offline.
func wideItem(i int) Item {
	s := func(v string) AttributeValue { return AttributeValue{S: &v} }
	n := func(v int) AttributeValue { x := strconv.Itoa(v); return AttributeValue{N: &x} }

	item := Item{
		"pk": s(fmt.Sprintf("ITEM#%09d", i)),
		"sk": s("ROW"),
	}

	// Flat scalar fields dominate the row.
	for f := 0; f < 160; f++ {
		if f%3 == 0 {
			item[fmt.Sprintf("NumericField%03d", f)] = n(i*7 + f)
		} else {
			item[fmt.Sprintf("StringField%03d", f)] = s(fmt.Sprintf("value-%d-%d-abcdef", i, f))
		}
	}

	// Media: a list of maps.
	media := make([]*AttributeValue, 0, 16)
	for m := 0; m < 16; m++ {
		url := s(fmt.Sprintf("https://cdn.example.com/items/%d/%d.jpg", i, m))
		order := n(m)
		kind := s("Photo")
		mm := AttributeValue{M: map[string]*AttributeValue{"MediaURL": &url, "Order": &order, "MediaCategory": &kind}}
		media = append(media, &mm)
	}
	item["Media"] = AttributeValue{L: media}

	// Rooms: a map of maps with small scalar leaves.
	rooms := map[string]*AttributeValue{}
	for r := 0; r < 12; r++ {
		lvl := s("Main")
		dim := s("12x14")
		area := n(150 + r)
		rm := AttributeValue{M: map[string]*AttributeValue{"Level": &lvl, "Dimensions": &dim, "Area": &area}}
		rooms[fmt.Sprintf("Room%02d", r)] = &rm
	}
	item["Rooms"] = AttributeValue{M: rooms}

	// A string set and a bool round out the type mix.
	item["Features"] = AttributeValue{SS: []string{"Garage", "Basement", "Fireplace", "Deck"}}
	t := true
	item["IsActive"] = AttributeValue{BOOL: &t}

	return item
}

// heapInUse forces a full collection and returns live heap bytes. WHY: per-item
// cost must count retained memory only, not garbage awaiting collection.
func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc
}

// TestMemoryProfileWideItems reports retained heap per wide nested item and the
// in-process fill write rate with persistence on. Opt-in (KUMO_RUN_MEMORY_PROFILE=1)
// because it allocates GBs on the pre-fix storage; KUMO_PROFILE_ITEMS sets N.
func TestMemoryProfileWideItems(t *testing.T) {
	if os.Getenv("KUMO_RUN_MEMORY_PROFILE") != "1" {
		t.Skip("set KUMO_RUN_MEMORY_PROFILE=1 to run the memory profile")
	}

	count := 20_000
	if v := os.Getenv("KUMO_PROFILE_ITEMS"); v != "" {
		count, _ = strconv.Atoi(v)
	}

	ctx := context.Background()

	// Encoded size reference: DynamoDB-JSON of one item, as the wire carries it.
	sample, err := json.Marshal(wideItem(0))
	if err != nil {
		t.Fatal(err)
	}

	attrs := countAttrs(wideItem(0))
	t.Logf("sample item: %d attributes (incl. nested), %d bytes DynamoDB-JSON", attrs, len(sample))

	// Phase 1: pure in-memory cost, no persistence, so heap delta is the table only.
	store := NewMemoryStorage("http://localhost:4566")
	createProfileTable(t, store)

	before := heapInUse()

	for i := 0; i < count; i++ {
		if _, err := store.PutItem(ctx, "profile", wideItem(i), false, ConditionInput{}); err != nil {
			t.Fatal(err)
		}
	}

	after := heapInUse()
	perItem := float64(after-before) / float64(count)
	t.Logf("in-memory: items=%d heap=%.1f MiB per-item=%.1f KiB (%.2fx DynamoDB-JSON)",
		count, float64(after-before)/(1<<20), perItem/1024, perItem/float64(len(sample)))
	// Stop the reaper and drop the store so phase 2's HeapSys measures phase 2 only.
	close(store.stopTTL)
	store = nil //nolint:ineffassign,wastedassign // releases the phase-1 heap.
	runtime.GC()

	// Phase 2: fill rate with persistence on, reported per window to expose the
	// slowdown the whole-store JSON snapshot caused.
	dir := t.TempDir()
	pstore := NewMemoryStorage("http://localhost:4566", WithDataDir(dir))
	t.Cleanup(func() { _ = pstore.Close(); close(pstore.stopTTL) })
	createProfileTable(t, pstore)

	window := count / 5
	start := time.Now()
	winStart := start

	for i := 0; i < count; i++ {
		if _, err := pstore.PutItem(ctx, "profile", wideItem(i), false, ConditionInput{}); err != nil {
			t.Fatal(err)
		}

		if (i+1)%window == 0 {
			el := time.Since(winStart)

			// HeapSys approximates RSS: it includes snapshot buffers and GC headroom.
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			t.Logf("persisted fill: items=%d window-rate=%.0f/s elapsed=%s heap-sys=%.0f MiB",
				i+1, float64(window)/el.Seconds(), time.Since(start).Round(time.Millisecond), float64(ms.HeapSys)/(1<<20))
			winStart = time.Now()
		}
	}

	// Restart: time a cold load of what was just persisted.
	if err := pstore.Close(); err != nil {
		t.Fatal(err)
	}

	loadStart := time.Now()
	reloaded := NewMemoryStorage("http://localhost:4566", WithDataDir(dir))
	t.Cleanup(func() { close(reloaded.stopTTL) })
	loadDur := time.Since(loadStart)

	got, err := reloaded.GetItem(ctx, "profile", Item{"pk": wideItem(count - 1)["pk"], "sk": wideItem(0)["sk"]})
	if err != nil || got == nil {
		t.Fatalf("reload lost the last item: %v", err)
	}

	t.Logf("reload: items=%d load=%s disk=%.1f MiB", count, loadDur.Round(time.Millisecond), float64(dirSize(t, dir))/(1<<20))
}

// createProfileTable uses a common table shape (pk/sk + one GSI)
// so index key projection is exercised.
func createProfileTable(t *testing.T, store *MemoryStorage) {
	t.Helper()

	_, err := store.CreateTable(context.Background(), &CreateTableRequest{
		TableName: "profile",
		KeySchema: []KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "sk", KeyType: "RANGE"}},
		AttributeDefinitions: []AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"}, {AttributeName: "sk", AttributeType: "S"},
			{AttributeName: "StringField001", AttributeType: "S"},
		},
		GlobalSecondaryIndexes: []GlobalSecondaryIndex{{
			IndexName: "byField", KeySchema: []KeySchemaElement{{AttributeName: "StringField001", KeyType: "HASH"}},
			Projection: Projection{ProjectionType: "ALL"},
		}},
		BillingMode: "PAY_PER_REQUEST",
	})
	if err != nil {
		t.Fatal(err)
	}
}

// countAttrs counts every attribute including nested map/list members.
func countAttrs(item Item) int {
	total := 0

	var walk func(av *AttributeValue)

	walk = func(av *AttributeValue) {
		total++

		for _, v := range av.M {
			walk(v)
		}

		for _, v := range av.L {
			walk(v)
		}
	}

	for _, v := range item {
		v := v
		walk(&v)
	}

	return total
}

// dirSize sums file sizes under dir to report on-disk footprint.
func dirSize(t *testing.T, dir string) int64 {
	t.Helper()

	var total int64

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range entries {
		if e.IsDir() {
			total += dirSize(t, dir+string(os.PathSeparator)+e.Name())

			continue
		}

		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}

		total += info.Size()
	}

	return total
}
