package dynamodb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// storedItem is one table row at rest: the full item as compact bytes plus a tiny projection of the attributes the hot paths
// read for every row (table/GSI/LSI keys, TTL). WHY: Query, Scan ordering and
// the TTL reaper walk all rows; the projection lets them do so without decoding
// multi-KB payloads, while GetItem/conditions/updates decode only the rows they
// touch. data is never mutated after creation, so persistence snapshots and
// readers may share it after the lock is released.
type storedItem struct {
	data []byte
	proj []projAttr
	// projComplete is false when some projected attribute is present but not a
	// plain S/N/B scalar; such rows fall back to a full decode.
	projComplete bool
}

// projAttr is a scalar attribute copied out of the encoded item. name aliases
// the table's projNames entry, so rows do not each allocate the name.
type projAttr struct {
	name string
	kind byte // 'S', 'N' or 'B'
	val  string
}

// projectedNames lists the attributes worth projecting for a table: every key
// attribute of the table and its indexes, plus the TTL attribute.
func projectedNames(t *Table) []string {
	seen := map[string]bool{}

	var names []string

	add := func(n string) {
		if n != "" && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}

	for _, ks := range t.KeySchema {
		add(ks.AttributeName)
	}

	for _, gsi := range t.GlobalSecondaryIndexes {
		for _, ks := range gsi.KeySchema {
			add(ks.AttributeName)
		}
	}

	for _, lsi := range t.LocalSecondaryIndexes {
		for _, ks := range lsi.KeySchema {
			add(ks.AttributeName)
		}
	}

	if t.TTLEnabled {
		add(t.TTLAttributeName)
	}

	return names
}

// scalarKind reports the single scalar type an AttributeValue holds, or 0 when
// it is anything else (document, set, BOOL/NULL, empty or malformed).
//
//nolint:gocritic // hugeParam: read-only inspection.
func scalarKind(av *AttributeValue) byte {
	if av.M != nil || av.L != nil || av.SS != nil || av.NS != nil || av.BS != nil || av.BOOL != nil || av.NULL != nil {
		return 0
	}

	switch {
	case av.S != nil && av.N == nil && av.B == nil:
		return 'S'
	case av.N != nil && av.S == nil && av.B == nil:
		return 'N'
	case av.B != nil && av.S == nil && av.N == nil:
		return 'B'
	default:
		return 0
	}
}

func scalarValue(kind byte, av *AttributeValue) string {
	switch kind {
	case 'S':
		return *av.S
	case 'N':
		return *av.N
	default:
		return string(av.B)
	}
}

// newStoredItem encodes item and projects names out of the in-hand Item.
func newStoredItem(names []string, item Item) storedItem {
	si := storedItem{data: encodeItem(item), projComplete: true}

	for _, n := range names {
		av, ok := item[n]
		if !ok {
			continue
		}

		si.addProj(n, &av)
	}

	return si
}

func (si *storedItem) addProj(name string, av *AttributeValue) {
	kind := scalarKind(av)
	if kind == 0 {
		si.projComplete = false

		return
	}

	si.proj = append(si.proj, projAttr{name: name, kind: kind, val: scalarValue(kind, av)})
}

// reproject rebuilds the projection from the encoded bytes, used after load or
// after the projected attribute set changes (TTL enabled).
func (si *storedItem) reproject(names []string) error {
	si.proj = nil
	si.projComplete = true

	return scanTopLevel(si.data, func(name, raw []byte) {
		for _, n := range names {
			if n != string(name) {
				continue
			}

			av, err := decodeValueBytes(raw)
			if err != nil {
				// Corrupt value: force full-decode fallback, which surfaces it.
				si.projComplete = false

				return
			}

			si.addProj(n, &av)

			return
		}
	})
}

// projected returns a projected attribute. known is false when the row must be
// decoded to answer (attribute absent from proj and projection incomplete).
func (si *storedItem) projected(name string) (av AttributeValue, found, known bool) {
	for i := range si.proj {
		p := &si.proj[i]
		if p.name != name {
			continue
		}

		v := p.val

		switch p.kind {
		case 'S':
			return AttributeValue{S: &v}, true, true
		case 'N':
			return AttributeValue{N: &v}, true, true
		default:
			return AttributeValue{B: []byte(v)}, true, true
		}
	}

	return AttributeValue{}, false, si.projComplete
}

// keyView materializes the projection as an Item (for key conditions and
// ordering), or nil when the projection is incomplete and the caller must decode.
func (si *storedItem) keyView() Item {
	if !si.projComplete {
		return nil
	}

	view := make(Item, len(si.proj))
	for _, p := range si.proj {
		av, _, _ := si.projected(p.name)
		view[p.name] = av
	}

	return view
}

// decode returns a fresh, caller-owned Item. A decode failure means the stored
// bytes are corrupt, which no request can recover from: fail hard.
func (si *storedItem) decode() Item {
	item, err := decodeItem(si.data)
	if err != nil {
		panic(fmt.Sprintf("dynamodb: %v", err))
	}

	return item
}

// MarshalJSON keeps the legacy single-file JSON shape (DynamoDB-JSON items)
// for MarshalJSON snapshots and migration tests.
func (si storedItem) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(si.decode())
	if err != nil {
		return nil, fmt.Errorf("marshal stored item: %w", err)
	}

	return data, nil
}

// UnmarshalJSON reads a legacy JSON item; the projection is rebuilt by the
// owner once the table schema is known (see finishLoadLocked).
func (si *storedItem) UnmarshalJSON(data []byte) error {
	var item Item
	if err := json.Unmarshal(data, &item); err != nil {
		return fmt.Errorf("unmarshal stored item: %w", err)
	}

	si.data = encodeItem(item)
	si.projComplete = false

	return nil
}

// recordSize approximates a row's journal footprint, used to decide when the
// append-only table log has accumulated enough garbage to compact.
func recordSize(key string, data []byte) int64 {
	return int64(1 + 2*binary.MaxVarintLen32 + len(key) + len(data))
}

// partIndex resolves Query's partition-key equality without walking the table:
// partition attribute name (table/GSI/LSI HASH) -> kind+value
// -> set of row keys. The row keys share the Items map keys' bytes, so the index
// holds references, never row copies. Rows lacking a scalar value stay out,
// matching DynamoDB's sparse indexes.
type partIndex map[string]map[string]map[string]struct{}

// partValueKey is the index key for a scalar partition value of the given kind.
func partValueKey(kind byte, val string) string {
	return string(kind) + val
}

// queryPartKey maps a queried partition value to its index key; ok is false
// for a non-scalar value, which the caller answers with a full walk.
func queryPartKey(av *AttributeValue) (string, bool) {
	kind := scalarKind(av)
	if kind == 0 {
		return "", false
	}

	return partValueKey(kind, scalarValue(kind, av)), true
}

// buildPartIndex (re)indexes every row under every HASH attribute of the table
// and its indexes. Called on create, after load and after reprojection.
func (td *tableData) buildPartIndex() {
	td.partIdx = partIndex{}

	add := func(ks []KeySchemaElement) {
		if name := keyAttrName(ks, "HASH"); name != "" {
			td.partIdx[name] = map[string]map[string]struct{}{}
		}
	}

	add(td.Table.KeySchema)

	for _, gsi := range td.Table.GlobalSecondaryIndexes {
		add(gsi.KeySchema)
	}

	for _, lsi := range td.Table.LocalSecondaryIndexes {
		add(lsi.KeySchema)
	}

	for key, si := range td.Items {
		td.indexRow(key, &si, true)
	}
}

// indexRow adds or removes one row under each indexed partition attribute it
// carries. A table without an index (nil partIdx) is left alone.
func (td *tableData) indexRow(key string, si *storedItem, add bool) {
	for i := range si.proj {
		p := &si.proj[i]

		parts, ok := td.partIdx[p.name]
		if !ok {
			continue
		}

		vk := partValueKey(p.kind, p.val)

		if add {
			if parts[vk] == nil {
				parts[vk] = map[string]struct{}{}
			}

			parts[vk][key] = struct{}{}

			continue
		}

		delete(parts[vk], key)

		if len(parts[vk]) == 0 {
			delete(parts, vk)
		}
	}
}

// partitionRows returns the row keys of one partition; ok is false when the
// attribute or value is not indexed and the caller must walk every row.
func (td *tableData) partitionRows(name string, want *AttributeValue) (map[string]struct{}, bool) {
	parts, ok := td.partIdx[name]
	if !ok {
		return nil, false
	}

	vk, ok := queryPartKey(want)
	if !ok {
		return nil, false
	}

	return parts[vk], true
}

// putStoredLocked stores item under key, projecting the table's hot
// attributes and journaling the write for persistence. Caller holds m.mu.
func (m *MemoryStorage) putStoredLocked(td *tableData, key string, item Item) {
	if old, ok := td.Items[key]; ok {
		td.liveBytes -= recordSize(key, old.data)
		td.indexRow(key, &old, false)
	}

	si := newStoredItem(td.projNames, item)
	td.Items[key] = si
	td.indexRow(key, &si, true)
	td.liveBytes += recordSize(key, si.data)

	if m.dataDir != "" {
		td.pending = appendPutRecord(td.pending, key, si.data)
	}
}

// deleteStoredLocked removes key and journals the delete. Caller holds m.mu.
func (m *MemoryStorage) deleteStoredLocked(td *tableData, key string) {
	old, ok := td.Items[key]
	if !ok {
		return
	}

	td.liveBytes -= recordSize(key, old.data)
	td.indexRow(key, &old, false)
	delete(td.Items, key)

	if m.dataDir != "" {
		td.pending = appendDeleteRecord(td.pending, key)
	}
}

// existingItem decodes the row at key, or returns nil when absent.
func (td *tableData) existingItem(key string) Item {
	si, ok := td.Items[key]
	if !ok {
		return nil
	}

	return si.decode()
}

// reprojectAll rebuilds every row's projection after the projected attribute
// set changes. Caller holds m.mu.
func (td *tableData) reprojectAll() error {
	td.projNames = projectedNames(td.Table)

	for key, si := range td.Items {
		if err := si.reproject(td.projNames); err != nil {
			return fmt.Errorf("table %s item %q: %w", td.Table.Name, key, err)
		}

		td.Items[key] = si
	}

	// Projections changed, so the partition index is rebuilt from them.
	td.buildPartIndex()

	return nil
}
