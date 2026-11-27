package dynamodb

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Items are stored as compact binary instead of the AttributeValue pointer
// graph. WHY: AttributeValue reserves a slot per DynamoDB type (~176 B) and
// every nested M/L member is its own allocation, so a wide ~9 KB row cost
// ~68 KiB of live heap (~190 KiB RSS). The encoding below is
// a flat []byte (no pointers, so the GC never scans it) decoded on demand.
//
// Item layout:  uvarint attrCount, then per attribute
//
//	uvarint nameLen, name, uvarint valueLen, value
//
// The per-attribute valueLen lets key/index projection skip unneeded values
// without decoding them (see scanTopLevel).
//
// Value layout: uvarint presence mask (bits below), then each present field in
// bit order. A mask instead of a single type tag keeps the encoding lossless
// for every AttributeValue the old in-memory model could hold, including the
// all-nil value and nil pointers inside M/L.
const (
	avS uint64 = 1 << iota
	avN
	avM
	avL
	avBOOL
	avNULL
	avSS
	avNS
	avB
	avBS
	avNilPtr // a nil *AttributeValue inside M or L
)

// errCorruptItem signals an encoded item that does not parse; stored bytes are
// produced only by encodeItem, so this is always a bug or disk corruption.
var errCorruptItem = errors.New("corrupt encoded dynamodb item")

// encodeItem serializes an item; the result shares nothing with the input, so
// encoding doubles as the deep copy writes used to need.
func encodeItem(item Item) []byte {
	buf := make([]byte, 0, 256)
	buf = binary.AppendUvarint(buf, uint64(len(item)))

	var scratch []byte

	for name, av := range item {
		buf = appendString(buf, name)
		// Encode into scratch first so the value can be length-prefixed.
		scratch = appendValue(scratch[:0], &av)
		buf = binary.AppendUvarint(buf, uint64(len(scratch)))
		buf = append(buf, scratch...)
	}

	return buf
}

// appendString writes a length-prefixed string.
func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))

	return append(buf, s...)
}

// appendValue writes one AttributeValue (recursively for M/L).
func appendValue(buf []byte, av *AttributeValue) []byte {
	if av == nil {
		return binary.AppendUvarint(buf, avNilPtr)
	}

	var mask uint64

	// Presence is pointer/slice non-nil-ness, exactly what MarshalJSON emits.
	if av.S != nil {
		mask |= avS
	}

	if av.N != nil {
		mask |= avN
	}

	if av.M != nil {
		mask |= avM
	}

	if av.L != nil {
		mask |= avL
	}

	if av.BOOL != nil {
		mask |= avBOOL
	}

	if av.NULL != nil {
		mask |= avNULL
	}

	if av.SS != nil {
		mask |= avSS
	}

	if av.NS != nil {
		mask |= avNS
	}

	if av.B != nil {
		mask |= avB
	}

	if av.BS != nil {
		mask |= avBS
	}

	buf = binary.AppendUvarint(buf, mask)

	// Fields follow in mask-bit order; decodeValue reads them in the same order.
	if av.S != nil {
		buf = appendString(buf, *av.S)
	}

	if av.N != nil {
		buf = appendString(buf, *av.N)
	}

	if av.M != nil {
		buf = binary.AppendUvarint(buf, uint64(len(av.M)))
		for k, v := range av.M {
			buf = appendString(buf, k)
			buf = appendValue(buf, v)
		}
	}

	if av.L != nil {
		buf = binary.AppendUvarint(buf, uint64(len(av.L)))
		for _, v := range av.L {
			buf = appendValue(buf, v)
		}
	}

	if av.BOOL != nil {
		buf = append(buf, boolByte(*av.BOOL))
	}

	if av.NULL != nil {
		buf = append(buf, boolByte(*av.NULL))
	}

	if av.SS != nil {
		buf = appendStrings(buf, av.SS)
	}

	if av.NS != nil {
		buf = appendStrings(buf, av.NS)
	}

	if av.B != nil {
		buf = binary.AppendUvarint(buf, uint64(len(av.B)))
		buf = append(buf, av.B...)
	}

	if av.BS != nil {
		buf = binary.AppendUvarint(buf, uint64(len(av.BS)))
		for _, b := range av.BS {
			buf = binary.AppendUvarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		}
	}

	return buf
}

// appendStrings writes a counted list of strings (SS/NS).
func appendStrings(buf []byte, ss []string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(ss)))
	for _, s := range ss {
		buf = appendString(buf, s)
	}

	return buf
}

func boolByte(b bool) byte {
	if b {
		return 1
	}

	return 0
}

// itemDecoder walks an encoded buffer; any overrun marks it failed so callers
// check one error at the end instead of after every field.
type itemDecoder struct {
	buf []byte
	pos int
	err error
}

func (d *itemDecoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}

	v, n := binary.Uvarint(d.buf[d.pos:])
	if n <= 0 {
		d.err = errCorruptItem

		return 0
	}

	d.pos += n

	return v
}

// bytes returns the next n bytes as a sub-slice (no copy).
func (d *itemDecoder) bytes(n uint64) []byte {
	if d.err != nil {
		return nil
	}

	if n > uint64(len(d.buf)-d.pos) {
		d.err = errCorruptItem

		return nil
	}

	b := d.buf[d.pos : d.pos+int(n)]
	d.pos += int(n)

	return b
}

// str copies the next length-prefixed string out of the buffer.
func (d *itemDecoder) str() string {
	return string(d.bytes(d.uvarint()))
}

func (d *itemDecoder) byteVal() byte {
	b := d.bytes(1)
	if len(b) == 0 {
		return 0
	}

	return b[0]
}

// decodeItem rebuilds a fresh Item the caller may mutate freely.
func decodeItem(data []byte) (Item, error) {
	d := &itemDecoder{buf: data}
	count := d.uvarint()

	// Cap the pre-size by the buffer length so a corrupt count cannot OOM.
	item := make(Item, min(count, uint64(len(data))))

	for i := uint64(0); i < count && d.err == nil; i++ {
		name := d.str()
		end := d.uvarint()
		start := d.pos
		av := d.value()

		if d.err == nil && uint64(d.pos-start) != end {
			d.err = errCorruptItem
		}

		if av != nil {
			item[name] = *av
		}
	}

	if d.err == nil && d.pos != len(data) {
		d.err = errCorruptItem
	}

	if d.err != nil {
		return nil, d.err
	}

	return item, nil
}

// value decodes one AttributeValue; nil means an encoded nil pointer.
func (d *itemDecoder) value() *AttributeValue {
	mask := d.uvarint()
	if mask&avNilPtr != 0 {
		return nil
	}

	av := &AttributeValue{}

	if mask&avS != 0 {
		s := d.str()
		av.S = &s
	}

	if mask&avN != 0 {
		n := d.str()
		av.N = &n
	}

	if mask&avM != 0 {
		count := d.uvarint()
		av.M = make(map[string]*AttributeValue, min(count, uint64(len(d.buf))))

		for i := uint64(0); i < count && d.err == nil; i++ {
			k := d.str()
			av.M[k] = d.value()
		}
	}

	if mask&avL != 0 {
		count := d.uvarint()
		av.L = make([]*AttributeValue, 0, min(count, uint64(len(d.buf))))

		for i := uint64(0); i < count && d.err == nil; i++ {
			av.L = append(av.L, d.value())
		}
	}

	if mask&avBOOL != 0 {
		b := d.byteVal() == 1
		av.BOOL = &b
	}

	if mask&avNULL != 0 {
		b := d.byteVal() == 1
		av.NULL = &b
	}

	if mask&avSS != 0 {
		av.SS = d.strings()
	}

	if mask&avNS != 0 {
		av.NS = d.strings()
	}

	if mask&avB != 0 {
		// Copy: a returned []byte must never alias the immutable stored buffer.
		av.B = append([]byte{}, d.bytes(d.uvarint())...)
	}

	if mask&avBS != 0 {
		count := d.uvarint()
		av.BS = make([][]byte, 0, min(count, uint64(len(d.buf))))

		for i := uint64(0); i < count && d.err == nil; i++ {
			av.BS = append(av.BS, append([]byte{}, d.bytes(d.uvarint())...))
		}
	}

	return av
}

func (d *itemDecoder) strings() []string {
	count := d.uvarint()
	out := make([]string, 0, min(count, uint64(len(d.buf))))

	for i := uint64(0); i < count && d.err == nil; i++ {
		out = append(out, d.str())
	}

	return out
}

// scanTopLevel visits each top-level attribute without decoding its value,
// handing visit the raw encoded value bytes. WHY: rebuilding key projections on
// load must not pay a full decode of multi-KB rows.
// The name is passed as bytes so callers compare it without an allocation.
func scanTopLevel(data []byte, visit func(name, raw []byte)) error {
	d := &itemDecoder{buf: data}
	count := d.uvarint()

	for i := uint64(0); i < count && d.err == nil; i++ {
		nameBytes := d.bytes(d.uvarint())
		raw := d.bytes(d.uvarint())

		if d.err == nil {
			visit(nameBytes, raw)
		}
	}

	if d.err != nil {
		return fmt.Errorf("scan item: %w", d.err)
	}

	return nil
}

// decodeValueBytes decodes one standalone encoded value (from scanTopLevel).
func decodeValueBytes(raw []byte) (AttributeValue, error) {
	d := &itemDecoder{buf: raw}
	av := d.value()

	if d.err == nil && d.pos != len(raw) {
		d.err = errCorruptItem
	}

	if d.err != nil {
		return AttributeValue{}, d.err
	}

	if av == nil {
		return AttributeValue{}, nil
	}

	return *av, nil
}
