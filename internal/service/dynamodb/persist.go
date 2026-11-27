package dynamodb

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/sivchari/kumo/internal/storage"
)

// On-disk format v2. WHY: v1 re-marshalled the
// whole store into one dynamodb.json about once a second, so a multi-GB fill
// held a multi-GB JSON buffer (doubling RSS), throttled writes, and a restart
// parsed the whole blob before serving. v2 keeps:
//
//	<dataDir>/dynamodb.d/meta.json   small: version, table metadata, tags
//	<dataDir>/dynamodb.d/<id>.items  one append-only binary log per table
//
// A flush appends only the records written since the previous flush. A table
// log is rewritten (compacted) only when it is new, when its garbage exceeds its
// live data, or after a failed flush. Load replays each log sequentially.
const (
	persistDirName  = "dynamodb.d"
	metaName        = "meta" // storage.SaveBytes appends ".json"
	persistVersion  = 2
	logMagic        = "KUMODDB\x02"
	recPut          = 'P'
	recDel          = 'D'
	compactMinBytes = 64 << 20
	ioBufferBytes   = 4 << 20
)

// persistedMeta is meta.json; Version gates future format changes.
type persistedMeta struct {
	Version int                       `json:"version"`
	Tables  map[string]persistedTable `json:"tables"`
	Tags    map[string][]Tag          `json:"tags,omitempty"`
}

type persistedTable struct {
	Table *Table `json:"table"`
	File  string `json:"file"`
}

// persistState is flush bookkeeping. fileBytes is owned by persistMu (only the
// single in-flight flush touches it); the rest is guarded by MemoryStorage.mu.
type persistState struct {
	persistMu    sync.Mutex
	fileBytes    map[string]int64
	metaDirty    bool
	removedFiles []string
}

func (m *MemoryStorage) persistDir() string {
	return filepath.Join(m.dataDir, persistDirName)
}

func newTableFileID() string {
	return uuid.New().String() + ".items"
}

// appendPutRecord / appendDeleteRecord encode one journal record.
func appendPutRecord(buf []byte, key string, data []byte) []byte {
	buf = append(buf, recPut)
	buf = appendString(buf, key)
	buf = binary.AppendUvarint(buf, uint64(len(data)))

	return append(buf, data...)
}

func appendDeleteRecord(buf []byte, key string) []byte {
	buf = append(buf, recDel)

	return appendString(buf, key)
}

// flushJob is one table file's work, captured under the lock and executed off it.
type flushJob struct {
	file    string
	pending []byte     // records to append
	rows    []flushRow // full contents, when compacting
	compact bool
}

type flushRow struct {
	key  string
	data []byte
}

// flush persists every change since the previous flush. It holds m.mu only to
// capture work (journal buffers swap in O(1); compaction copies row headers),
// then does all I/O unlocked.
func (m *MemoryStorage) flush() error {
	m.persist.persistMu.Lock()
	defer m.persist.persistMu.Unlock()

	m.mu.Lock()
	jobs, meta, removed := m.captureFlushLocked()
	m.mu.Unlock()

	err := m.writeFlush(jobs, meta, removed)
	if err != nil {
		// A partial write leaves files in an unknown state: rewrite everything on
		// the next flush rather than trusting any log.
		m.mu.Lock()

		for _, td := range m.Tables {
			td.needCompact = true
		}

		m.persist.metaDirty = true
		m.persist.removedFiles = append(m.persist.removedFiles, removed...)
		m.mu.Unlock()
	}

	return err
}

// captureFlushLocked snapshots pending work. Caller holds m.mu and persistMu.
func (m *MemoryStorage) captureFlushLocked() ([]flushJob, []byte, []string) {
	var meta []byte

	if m.persist.metaDirty {
		pm := persistedMeta{Version: persistVersion, Tables: make(map[string]persistedTable, len(m.Tables)), Tags: m.Tags}
		for name, td := range m.Tables {
			pm.Tables[name] = persistedTable{Table: td.Table, File: td.fileID}
		}

		// Marshal under the lock: metadata is small, and Table/Tags are mutable.
		data, err := json.Marshal(pm)
		if err != nil {
			panic(fmt.Sprintf("dynamodb: marshal meta: %v", err))
		}

		meta = data
		m.persist.metaDirty = false
	}

	var jobs []flushJob

	for _, td := range m.Tables {
		size := m.persist.fileBytes[td.fileID]
		garbage := size > compactMinBytes && size > 2*td.liveBytes

		switch {
		case td.needCompact || garbage:
			rows := make([]flushRow, 0, len(td.Items))
			for k, si := range td.Items {
				rows = append(rows, flushRow{key: k, data: si.data})
			}

			jobs = append(jobs, flushJob{file: td.fileID, rows: rows, compact: true})
			td.needCompact = false
			td.pending = nil
		case len(td.pending) > 0:
			jobs = append(jobs, flushJob{file: td.fileID, pending: td.pending})
			td.pending = nil
		}
	}

	removed := m.persist.removedFiles
	m.persist.removedFiles = nil

	return jobs, meta, removed
}

// writeFlush performs the captured I/O: table files first, then meta, then
// deletes, so meta never names a table whose file was already removed.
func (m *MemoryStorage) writeFlush(jobs []flushJob, meta []byte, removed []string) error {
	dir := m.persistDir()

	// Mkdir, not MkdirAll: a flush racing the removal of dataDir must fail
	// instead of resurrecting it (see storage.writeSnapshot).
	if err := os.Mkdir(dir, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create %s: %w", dir, err)
	}

	for _, j := range jobs {
		var err error
		if j.compact {
			err = m.compactFile(dir, j)
		} else {
			err = m.appendFile(dir, j)
		}

		if err != nil {
			return err
		}
	}

	if meta != nil {
		if err := storage.SaveBytes(dir, metaName, meta); err != nil {
			return fmt.Errorf("write meta: %w", err)
		}
	}

	for _, f := range removed {
		delete(m.persist.fileBytes, f)

		if err := os.Remove(filepath.Join(dir, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", f, err)
		}
	}

	return nil
}

// appendFile appends journal records, writing the header into a new file.
func (m *MemoryStorage) appendFile(dir string, j flushJob) error {
	path := filepath.Join(dir, j.file)

	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}

	info, err := f.Stat()
	if err == nil && info.Size() == 0 {
		_, err = f.WriteString(logMagic)
	}

	if err == nil {
		_, err = f.Write(j.pending)
	}

	if cerr := f.Close(); err == nil {
		err = cerr
	}

	if err != nil {
		return fmt.Errorf("append %s: %w", path, err)
	}

	m.persist.fileBytes[j.file] = info.Size() + int64(len(j.pending))
	if info.Size() == 0 {
		m.persist.fileBytes[j.file] += int64(len(logMagic))
	}

	return nil
}

// compactFile rewrites a table log with exactly its live rows, atomically.
func (m *MemoryStorage) compactFile(dir string, j flushJob) error {
	path := filepath.Join(dir, j.file)
	tmp := path + ".tmp"

	f, err := os.Create(filepath.Clean(tmp))
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}

	w := bufio.NewWriterSize(f, ioBufferBytes)
	_, err = w.WriteString(logMagic)

	var rec []byte

	written := int64(len(logMagic))

	for _, r := range j.rows {
		if err != nil {
			break
		}

		rec = appendPutRecord(rec[:0], r.key, r.data)
		_, err = w.Write(rec)
		written += int64(len(rec))
	}

	if err == nil {
		err = w.Flush()
	}

	// fsync before rename so a crash never leaves a renamed-but-empty log.
	if err == nil {
		err = f.Sync()
	}

	if cerr := f.Close(); err == nil {
		err = cerr
	}

	if err == nil {
		err = os.Rename(tmp, path)
	}

	if err != nil {
		_ = os.Remove(tmp)

		return fmt.Errorf("compact %s: %w", path, err)
	}

	m.persist.fileBytes[j.file] = written

	return nil
}

// loadPersisted restores state at construction: v2 if present, else migrates a
// v1 dynamodb.json. Any failure is fatal to the caller, because starting empty
// would let the next flush overwrite the user's data.
func (m *MemoryStorage) loadPersisted() error {
	dir := m.persistDir()

	raw, err := os.ReadFile(filepath.Join(dir, metaName+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return m.migrateLegacy()
	}

	if err != nil {
		return fmt.Errorf("read meta: %w", err)
	}

	var meta persistedMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return fmt.Errorf("parse meta: %w", err)
	}

	if meta.Version != persistVersion {
		return fmt.Errorf("unsupported dynamodb data version %d (want %d)", meta.Version, persistVersion)
	}

	if meta.Tags != nil {
		m.Tags = meta.Tags
	}

	// Replay tables concurrently: each is an independent sequential read.
	var (
		wg      sync.WaitGroup
		errMu   sync.Mutex
		loadErr error
	)

	for name, pt := range meta.Tables {
		td := &tableData{Table: pt.Table, Items: map[string]storedItem{}, fileID: pt.File}
		m.Tables[name] = td

		wg.Add(1)

		go func() {
			defer wg.Done()

			size, err := replayLog(filepath.Join(dir, td.fileID), td)
			errMu.Lock()
			defer errMu.Unlock()

			if err != nil && loadErr == nil {
				loadErr = fmt.Errorf("table %s: %w", td.Table.Name, err)
			}

			m.persist.fileBytes[td.fileID] = size
		}()
	}

	wg.Wait()

	if loadErr != nil {
		return loadErr
	}

	m.removeOrphanLogs(dir)

	return nil
}

// replayLog rebuilds td.Items from its log and returns the file size. A torn
// final record (crash mid-append) is dropped and the table marked for
// compaction; anything else malformed is corruption and fails the load.
func replayLog(path string, td *tableData) (int64, error) {
	td.projNames = projectedNames(td.Table)

	f, err := os.Open(filepath.Clean(path))
	if errors.Is(err, os.ErrNotExist) {
		// The table was created but never flushed: empty, and needs a file.
		td.needCompact = true

		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("open log: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat log: %w", err)
	}

	r := bufio.NewReaderSize(f, ioBufferBytes)

	magic := make([]byte, len(logMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		td.needCompact = true

		return info.Size(), nil //nolint:nilerr // a torn header is an empty log.
	}

	if string(magic) != logMagic {
		return 0, errors.New("bad log header")
	}

	for {
		torn, err := replayRecord(r, td)
		if errors.Is(err, io.EOF) {
			break
		}

		if torn {
			td.needCompact = true

			break
		}

		if err != nil {
			return 0, err
		}
	}

	return info.Size(), nil
}

// replayRecord applies one record. io.EOF means a clean end of log; torn means
// the log ended inside a record.
func replayRecord(r *bufio.Reader, td *tableData) (torn bool, err error) {
	op, err := r.ReadByte()
	if err != nil {
		return false, io.EOF
	}

	keyLen, err := binary.ReadUvarint(r)
	if err != nil {
		return true, err
	}

	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBytes); err != nil {
		return true, err
	}

	key := string(keyBytes)

	switch op {
	case recDel:
		if old, ok := td.Items[key]; ok {
			td.liveBytes -= recordSize(key, old.data)
			delete(td.Items, key)
		}

		return false, nil
	case recPut:
		dataLen, err := binary.ReadUvarint(r)
		if err != nil {
			return true, err
		}

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return true, err
		}

		si := storedItem{data: data}
		if err := si.reproject(td.projNames); err != nil {
			return false, fmt.Errorf("item %q: %w", key, err)
		}

		if old, ok := td.Items[key]; ok {
			td.liveBytes -= recordSize(key, old.data)
		}

		td.Items[key] = si
		td.liveBytes += recordSize(key, data)

		return false, nil
	default:
		return false, fmt.Errorf("bad record op %q", op)
	}
}

// removeOrphanLogs deletes logs no table references (a crash between meta and
// file removal, or a superseded legacy import). Best-effort: orphans are inert.
func (m *MemoryStorage) removeOrphanLogs(dir string) {
	live := map[string]bool{}
	for _, td := range m.Tables {
		live[td.fileID] = true
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, e := range entries {
		name := e.Name()
		if (strings.HasSuffix(name, ".items") && !live[name]) || strings.HasSuffix(name, ".items.tmp") {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
}

// migrateLegacy imports a v1 dynamodb.json, writes it as v2, then deletes the
// v1 file so later flushes never rewrite the whole store again.
func (m *MemoryStorage) migrateLegacy() error {
	legacy := filepath.Join(m.dataDir, "dynamodb.json")
	if _, err := os.Stat(legacy); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err := storage.Load(m.dataDir, "dynamodb", m); err != nil {
		return fmt.Errorf("load legacy snapshot: %w", err)
	}

	if err := os.MkdirAll(m.persistDir(), 0o750); err != nil {
		return fmt.Errorf("create %s: %w", m.persistDir(), err)
	}

	if err := m.flush(); err != nil {
		return fmt.Errorf("write migrated data: %w", err)
	}

	if err := os.Remove(legacy); err != nil {
		return fmt.Errorf("remove legacy snapshot: %w", err)
	}

	return nil
}

// finishLoadLocked prepares tables decoded from legacy JSON: projections, size
// accounting and fresh log files. Caller holds m.mu.
func (m *MemoryStorage) finishLoadLocked() error {
	for _, td := range m.Tables {
		if td.Items == nil {
			td.Items = map[string]storedItem{}
		}

		if err := td.reprojectAll(); err != nil {
			return err
		}

		td.liveBytes = 0
		for k, si := range td.Items {
			td.liveBytes += recordSize(k, si.data)
		}

		td.fileID = newTableFileID()
		td.needCompact = true
		td.pending = nil
	}

	m.persist.metaDirty = true

	return nil
}
