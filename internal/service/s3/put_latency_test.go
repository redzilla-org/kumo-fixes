package s3

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/sivchari/kumo/internal/storage"
)

// TestPutLatencyIndependentOfStoredObjects guards against blob I/O under the
// snapshot lock: every snapshot tick listed the whole blob directory (and
// pruned it) under the read lock PUT waits on, and PUT streamed its body to
// disk while holding the write lock, so PUT stalls grew with stored data. Max
// PUT latency with 5000 existing objects must stay near the 100-object
// baseline while the flusher runs.
func TestPutLatencyIndependentOfStoredObjects(t *testing.T) {
	small := putLatency(t, 100)
	large := putLatency(t, 5000)

	t.Logf("max PUT: 100 objects %v, 5000 objects %v", small, large)

	// Max, not p90: a stall hits only PUTs that land inside a snapshot pass.
	if large > 4*small+100*time.Millisecond {
		t.Fatalf("PUT latency scales with stored objects: %v -> %v", small, large)
	}
}

// putLatency stores n distinct 16 KiB objects in a persisted bucket, then times
// 100 PUTs while a flusher snapshots every 20ms, as the 1s background loop does.
func putLatency(t *testing.T, n int) time.Duration {
	t.Helper()

	ctx := context.Background()

	// Best-effort cleanup: the process-wide 1s flusher may still be writing.
	dir, err := os.MkdirTemp("", "s3-put-latency")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	s := NewMemoryStorage(WithDataDir(dir))

	if err := s.CreateBucket(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	body := func(i int) *bytes.Reader {
		b := bytes.Repeat([]byte{byte(i)}, 16<<10)
		copy(b, fmt.Sprintf("%08d", i))

		return bytes.NewReader(b)
	}

	for i := range n {
		if _, err := s.PutObject(ctx, "b", fmt.Sprintf("old/%d", i), body(i), nil); err != nil {
			t.Fatal(err)
		}
	}

	storage.FlushSnapshots()

	// Snapshot continuously, the way PUT traffic keeps the store dirty.
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				storage.FlushSnapshots()
			}
		}
	}()

	lat := make([]time.Duration, 0, 100)

	for i := range 100 {
		start := time.Now()

		if _, err := s.PutObject(ctx, "b", fmt.Sprintf("new/%d", i), body(n+i), nil); err != nil {
			t.Fatal(err)
		}

		lat = append(lat, time.Since(start))

		// Sparse arrivals land inside a slow snapshot.
		time.Sleep(5 * time.Millisecond)
	}

	close(stop)
	<-done

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	slices.Sort(lat)

	t.Logf("n=%d PUT p50 %v p90 %v max %v", n, lat[49], lat[89], lat[99])

	return lat[99]
}
