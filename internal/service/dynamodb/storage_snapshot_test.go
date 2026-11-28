package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStorageSnapshotRemainsPointInTime protects the copy-on-write contract
// that permits JSON encoding after the global storage lock is released.
func TestStorageSnapshotRemainsPointInTime(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewMemoryStorage("http://localhost:4566")
	t.Cleanup(func() { close(store.stopTTL) })

	_, err := store.CreateTable(ctx, &CreateTableRequest{
		TableName:             "snapshot-test",
		KeySchema:             []KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions:  []AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
		ProvisionedThroughput: &ProvisionedThroughput{ReadCapacityUnits: 3, WriteCapacityUnits: 4},
		Tags:                  []Tag{{Key: "stage", Value: "before"}},
		TableClass:            "STANDARD",
	})
	if err != nil {
		t.Fatal(err)
	}

	before := Item{
		"pk": {S: ptr("kept")},
		"profile": {M: map[string]*AttributeValue{
			"name":  {S: ptr("before")},
			"prefs": {L: []*AttributeValue{{S: ptr("alpha")}, {S: ptr("beta")}}},
		}},
	}
	if _, err := store.PutItem(ctx, "snapshot-test", before, false, ConditionInput{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutItem(ctx, "snapshot-test", Item{"pk": {S: ptr("deleted")}}, false, ConditionInput{}); err != nil {
		t.Fatal(err)
	}

	store.mu.RLock()
	snapshot := store.snapshotLocked()
	store.mu.RUnlock()

	// Mutate every live-state category after capture; none may leak backward.
	_, err = store.UpdateItem(ctx, "snapshot-test", Item{"pk": {S: ptr("kept")}},
		"SET profile.#name = :name, profile.prefs[0] = :pref",
		map[string]string{"#name": "name"},
		map[string]AttributeValue{":name": {S: ptr("after")}, ":pref": {S: ptr("changed")}},
		ReturnValuesAllNew, ConditionInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteItem(ctx, "snapshot-test", Item{"pk": {S: ptr("deleted")}}, false, ConditionInput{}); err != nil {
		t.Fatal(err)
	}
	if err := store.TagResource(ctx, snapshot.Tables["snapshot-test"].Table.TableARN, []Tag{{Key: "stage", Value: "after"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateTable(ctx, &UpdateTableRequest{TableName: "snapshot-test", TableClass: "STANDARD_INFREQUENT_ACCESS"}); err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	// Round-trip the captured bytes through the production decoder before
	// asserting, so the persisted representation itself is under test.
	restored := NewMemoryStorage("http://localhost:4566")
	t.Cleanup(func() { close(restored.stopTTL) })
	if err := restored.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}

	got, err := restored.GetItem(ctx, "snapshot-test", Item{"pk": {S: ptr("kept")}})
	if err != nil {
		t.Fatal(err)
	}
	if got["profile"].M["name"].S == nil || *got["profile"].M["name"].S != "before" {
		t.Fatalf("nested update leaked into snapshot: %#v", got["profile"])
	}
	if got["profile"].M["prefs"].L[0].S == nil || *got["profile"].M["prefs"].L[0].S != "alpha" {
		t.Fatalf("nested list update leaked into snapshot: %#v", got["profile"])
	}
	if got, err := restored.GetItem(ctx, "snapshot-test", Item{"pk": {S: ptr("deleted")}}); err != nil || got == nil {
		t.Fatalf("post-capture delete changed snapshot: item=%#v err=%v", got, err)
	}
	table, err := restored.DescribeTable(ctx, "snapshot-test")
	if err != nil {
		t.Fatal(err)
	}
	if table.TableClass != "STANDARD" || table.ProvisionedThroughput.ReadCapacityUnits != 3 {
		t.Fatalf("table metadata changed after capture: %#v", table)
	}
	tags, err := restored.ListTagsOfResource(ctx, table.TableARN)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 1 || tags[0].Value != "before" {
		t.Fatalf("tag mutation leaked into snapshot: %#v", tags)
	}
}

// TestMarshalJSONConcurrentMutations exercises every copy-on-write path under
// the race detector while snapshots are captured and encoded without the lock.
func TestMarshalJSONConcurrentMutations(t *testing.T) {
	ctx := context.Background()
	store := newSnapshotTestStore(t, 128, 256)

	var failed atomic.Bool
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("item-%06d", (worker*100+i)%128)
				switch worker {
				case 0:
					if _, err := store.PutItem(ctx, "snapshot-load", snapshotItem(key, i, 256), false, ConditionInput{}); err != nil {
						failed.Store(true)
					}
				case 1:
					if _, err := store.UpdateItem(ctx, "snapshot-load", Item{"pk": {S: &key}}, "SET #v = :v", map[string]string{"#v": "version"}, map[string]AttributeValue{":v": {N: ptr(fmt.Sprint(i))}}, ReturnValuesAllNew, ConditionInput{}); err != nil {
						failed.Store(true)
					}
				case 2:
					if _, err := store.DeleteItem(ctx, "snapshot-load", Item{"pk": {S: &key}}, false, ConditionInput{}); err != nil {
						failed.Store(true)
					}
				case 3:
					_, err := store.TransactWriteItems(ctx, []TransactWriteItem{{Update: &TransactUpdate{
						TableName: "snapshot-load", Key: Item{"pk": {S: &key}}, UpdateExpression: "SET #v = :v",
						ExpressionAttributeNames: map[string]string{"#v": "version"}, ExpressionAttributeValues: map[string]AttributeValue{":v": {N: ptr(fmt.Sprint(i))}},
					}}})
					if err != nil {
						failed.Store(true)
					}
				}
			}
		}()
	}

	for i := 0; i < 40; i++ {
		data, err := store.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		var decoded storageSnapshot
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("snapshot %d is not independently decodable: %v", i, err)
		}
	}
	wg.Wait()
	if failed.Load() {
		t.Fatal("a concurrent storage mutation failed")
	}
}

type latencySample struct {
	operation string
	duration  time.Duration
}

// TestSnapshotContentionWorkload is opt-in because it intentionally allocates a
// production-sized (~58.6 MB) store and repeatedly serializes it.
func TestSnapshotContentionWorkload(t *testing.T) {
	if os.Getenv("KUMO_RUN_SNAPSHOT_PERF") != "1" {
		t.Skip("set KUMO_RUN_SNAPSHOT_PERF=1 to run the production-sized workload")
	}

	const (
		itemCount   = 64_458
		payloadSize = 850
		workers     = 8
		snapshots   = 4
	)
	ctx := context.Background()
	store := newSnapshotTestStore(t, itemCount, payloadSize)

	// Sample lock-facing API latency continuously while full JSON snapshots run.
	stop := make(chan struct{})
	samples := make(chan latencySample, 4096)
	byOperation := map[string][]time.Duration{}
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for sample := range samples {
			byOperation[sample.operation] = append(byOperation[sample.operation], sample.duration)
		}
	}()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sequence := 0; ; sequence++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("item-%06d", (worker*7919+sequence)%itemCount)
				started := time.Now()
				operation := "get"
				var err error
				if sequence%4 == 0 {
					operation = "put"
					_, err = store.PutItem(ctx, "snapshot-load", snapshotItem(key, sequence, payloadSize), false, ConditionInput{})
				} else {
					_, err = store.GetItem(ctx, "snapshot-load", Item{"pk": {S: &key}})
				}
				if err != nil {
					t.Errorf("%s failed: %v", operation, err)
					return
				}
				samples <- latencySample{operation: operation, duration: time.Since(started)}
			}
		}()
	}

	var encodedBytes int
	for i := 0; i < snapshots; i++ {
		data, err := store.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		encodedBytes = len(data)
	}
	close(stop)
	wg.Wait()
	close(samples)
	<-collected

	for _, operation := range []string{"get", "put"} {
		values := byOperation[operation]
		if len(values) == 0 {
			t.Fatalf("no %s latency samples", operation)
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		p50 := values[len(values)*50/100]
		p90 := values[len(values)*90/100]
		max := values[len(values)-1]
		t.Logf("%s samples=%d p50=%s p90=%s max=%s", operation, len(values), p50, p90, max)
		if p90 > 20*time.Millisecond {
			t.Errorf("%s p90=%s exceeds the 20ms target", operation, p90)
		}
	}
	t.Logf("items=%d encoded=%0.1f MiB snapshots=%d", itemCount, float64(encodedBytes)/(1024*1024), snapshots)
}

func newSnapshotTestStore(t *testing.T, itemCount, payloadSize int) *MemoryStorage {
	t.Helper()
	store := NewMemoryStorage("http://localhost:4566")
	t.Cleanup(func() { close(store.stopTTL) })

	// Direct population avoids timing setup writes; the resulting table state is
	// exactly the immutable representation produced by PutItem.
	table := &Table{
		Name: "snapshot-load", KeySchema: []KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}}, TableStatus: "ACTIVE",
	}
	td := &tableData{Table: table, Items: make(map[string]storedItem, itemCount), projNames: projectedNames(table)}
	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("item-%06d", i)
		item := snapshotItem(key, i, payloadSize)
		store.putStoredLocked(td, store.serializeKey(table, item), item)
	}
	store.Tables[table.Name] = td
	return store
}

func snapshotItem(key string, version, payloadSize int) Item {
	payload := strings.Repeat("x", payloadSize)
	return Item{
		"pk": {S: &key}, "version": {N: ptr(fmt.Sprint(version))},
		"payload": {S: &payload},
		"nested":  {M: map[string]*AttributeValue{"label": {S: ptr("stable")}}},
	}
}
