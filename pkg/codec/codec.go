package codec

import (
	"encoding/binary"
	"io"
	"math"
)

func WriteUint32(w io.Writer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	w.Write(b[:])
}

func ReadUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, e := r.Read(b[:]); e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func WriteFloat64(w io.Writer, v float64) {
	vi := math.Float64bits(v)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], vi)
	w.Write(b[:])
}

func ReadFloat64(r io.Reader) (float64, error) {
	var b [8]byte
	if _, e := r.Read(b[:]); e != nil {
		return 0, e
	}
	v := binary.LittleEndian.Uint64(b[:])
	return math.Float64frombits(v), nil
}
