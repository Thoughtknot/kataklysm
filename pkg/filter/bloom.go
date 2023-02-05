package filter

import (
	"io"
	"kataklysm/pkg/codec"
	"kataklysm/pkg/hash"
	"math"
	"math/big"
)

type BloomFilter struct {
	fpProbability float64
	expectedSize  uint32
	numBits       uint32
	numHashes     uint32
	bits          *big.Int
}

func NewBloomFilter(probability float64, expectedSize uint32) *BloomFilter {
	numBits := -1.44 * float64(expectedSize) * math.Log2(probability)
	numHashes := math.Log2(2) * numBits / float64(expectedSize)
	return &BloomFilter{
		fpProbability: probability,
		expectedSize:  expectedSize,
		numBits:       uint32(numBits),
		numHashes:     uint32(numHashes),
		bits:          big.NewInt(0),
	}
}

func (f *BloomFilter) Write(w io.Writer) {
	codec.WriteFloat64(w, f.fpProbability)
	codec.WriteUint32(w, f.expectedSize)
	codec.WriteUint32(w, f.numBits)
	codec.WriteUint32(w, f.numHashes)
	bts := f.bits.Bytes()
	codec.WriteUint32(w, uint32(len(bts)))
	w.Write(bts)
}

func Read(r io.Reader) (*BloomFilter, error) {
	fpProbability, e0 := codec.ReadFloat64(r)
	if e0 != nil {
		return nil, e0
	}
	expectedSize, e1 := codec.ReadUint32(r)
	if e1 != nil {
		return nil, e1
	}
	numBits, e2 := codec.ReadUint32(r)
	if e2 != nil {
		return nil, e2
	}
	numHashes, e3 := codec.ReadUint32(r)
	if e3 != nil {
		return nil, e3
	}
	lenBytes, e4 := codec.ReadUint32(r)
	if e4 != nil {
		return nil, e4
	}
	bts := make([]byte, lenBytes)
	r.Read(bts)
	b := big.NewInt(0).SetBytes(bts)
	return &BloomFilter{
		fpProbability: fpProbability,
		expectedSize:  expectedSize,
		numBits:       numBits,
		numHashes:     numHashes,
		bits:          b,
	}, nil
}

func (f *BloomFilter) Add(key []byte) {
	var i uint32
	for i = 0; i < uint32(f.numHashes); i++ {
		bitSet := hash.Hash(key, i) % f.numBits
		f.bits.SetBit(f.bits, int(bitSet), 1)
	}
}

func (f *BloomFilter) Query(key []byte) bool {
	var i uint32
	for i = 0; i < uint32(f.numHashes); i++ {
		bitSet := hash.Hash(key, i) % f.numBits
		if f.bits.Bit(int(bitSet)) != 1 {
			return false
		}
	}
	return true
}
