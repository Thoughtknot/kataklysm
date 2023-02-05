package hash

import "encoding/binary"

func rol(v uint32, shift int) uint32 {
	return (v << shift) | (v >> (32 - shift))
}

func Hash(b []byte, seed uint32) uint32 {
	c1 := uint32(0xcc9e2d51)
	c2 := uint32(0x1b873593)
	r1 := 15
	r2 := 13
	m := uint32(5)
	n := uint32(0xe6546b64)
	hash := seed
	l := len(b)
	for i := 0; i+4 <= l; i += 4 {
		k := c1 * binary.LittleEndian.Uint32(b[i:i+4])
		k = rol(k, r1)
		k *= c2

		hash ^= k
		hash = rol(hash, r2)
		hash = hash*m + n
	}
	remi := uint32(0)
	rem := l & 3
	for i := rem; i > 0; i-- {
		remi <<= 8
		remi |= uint32(b[l-rem+i-1])
	}
	remi *= c1
	remi = rol(remi, r1)
	remi *= c2

	hash ^= remi
	hash ^= uint32(len(b))
	hash ^= (hash >> 16)
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16
	return hash
}
