package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"kataklysm/pkg/codec"
	"kataklysm/pkg/filter"
	"kataklysm/pkg/tree"
	"log"
	"os"
	"strconv"

	"golang.org/x/exp/mmap"
)

type Segment struct {
	i    uint32
	data *mmap.ReaderAt
	bf   *filter.BloomFilter
	si   *SparseIndex
}

func (s *Segment) Query(key string) ([]byte, error) {
	if !s.bf.Query([]byte(key)) {
		return nil, errors.New("key not found")
	}
	fn, e := s.si.rt.Floor(key)
	if e != nil {
		return nil, errors.New("segment empty")
	}
	offset := fn.Value()
	for {
		k, v, newOffset, e := readEntry(s.data, offset)
		if e != nil {
			return nil, errors.New("key not found")
		}
		if k == key {
			return v, nil
		}
		if k > key {
			return nil, errors.New("key not found")
		}
		offset = newOffset
	}
}

func readEntry(r *mmap.ReaderAt, offset uint32) (string, []byte, uint32, error) {
	kl, e := ReadUint32(r, offset)
	offset += 4
	if e != nil {
		return "", nil, 0, e
	}
	key := make([]byte, kl)
	r.ReadAt(key, int64(offset))
	offset += kl
	vl, _ := ReadUint32(r, offset)
	offset += 4
	val := make([]byte, vl)
	r.ReadAt(val, int64(offset))
	offset += vl
	return string(key), val, offset, nil
}

func ReadUint32(mmap *mmap.ReaderAt, offset uint32) (uint32, error) {
	var b [4]byte
	_, e := mmap.ReadAt(b[:], int64(offset))
	return binary.LittleEndian.Uint32(b[:]), e
}

func writeEntry(n *tree.Node[string, []byte], w io.Writer) uint32 {
	size := 0
	codec.WriteUint32(w, uint32(len(n.Key())))
	size += 4
	o, _ := w.Write([]byte(n.Key()))
	size += o
	codec.WriteUint32(w, uint32(len(n.Value())))
	size += 4
	ov, _ := w.Write(n.Value())
	size += ov
	return uint32(size)
}

func CreateSegment(i uint32, rb *tree.RedBlackTree[string, []byte], bf *filter.BloomFilter) *Segment {
	w1, f := os.OpenFile("data/filter-"+strconv.Itoa(int(i)), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if f != nil {
		log.Fatal("Could not open filter")
	}
	bf.Write(w1)
	w1.Sync()
	w1.Close()
	df := "data/segment-" + strconv.Itoa(int(i))
	fl, e := os.OpenFile(df, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	w := bufio.NewWriter(fl)
	if e != nil {
		log.Fatal("Could not open segment")
	}
	rt := tree.New[string, uint32]()
	it := rb.Iterator()
	iv := 0
	offset := uint32(0)
	for it.Next() {
		if iv%100 == 0 {
			//fmt.Println("Writing entry ", iv)
			rt.Put(it.Key(), offset)
		}
		offset += writeEntry(it.Node(), w)
		iv++
	}
	w.Flush()
	fl.Close()

	mv, e := mmap.Open(df)
	if e != nil {
		log.Fatal("Could not mmap segment")
	}
	return &Segment{
		i:    i,
		data: mv,
		bf:   bf,
		si:   CreateSparseIndex(i, rt),
	}
}

func CreateSparseIndex(i uint32, rb *tree.RedBlackTree[string, uint32]) *SparseIndex {
	f, e := os.OpenFile("data/sparseIndex-"+strconv.Itoa(int(i)), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		log.Fatal("Could not open segment")
	}
	w := bufio.NewWriter(f)
	it := rb.Iterator()
	offset := uint32(0)
	for it.Next() {
		codec.WriteUint32(w, uint32(len(it.Key())))
		offset += 4
		o, _ := w.WriteString(it.Key())
		offset += uint32(o)
		codec.WriteUint32(w, it.Value())
		offset += 4
	}
	w.Flush()
	return &SparseIndex{
		data: f,
		rt:   rb,
	}
}

func ReadSegment(i uint32) *Segment {
	w1, f := os.OpenFile("data/filter-"+strconv.Itoa(int(i)), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if f != nil {
		log.Fatal("Could not open filter")
	}
	w, e := mmap.Open("data/segment-" + strconv.Itoa(int(i)))
	if e != nil {
		log.Fatal("Could not open segment")
	}
	bf, f := filter.Read(w1)
	if f != nil {
		log.Fatal("Could not open filter")
	}
	return &Segment{
		i:    i,
		data: w,
		bf:   bf,
		si:   ReadSparseIndex(i),
	}
}

func ReadSparseIndex(i uint32) *SparseIndex {
	w, e := os.OpenFile("data/sparseIndex-"+strconv.Itoa(int(i)), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		log.Fatal("Could not open segment")
	}
	rb := tree.New[string, uint32]()
	for {
		keyLen, e := codec.ReadUint32(w)
		if e != nil {
			break
		}
		key := make([]byte, keyLen)
		w.Read(key)
		offset, e := codec.ReadUint32(w)
		if e != nil {
			log.Fatal("Could not open segment")
		}
		rb.Put(string(key), offset)
	}
	return &SparseIndex{
		data: w,
		rt:   rb,
	}
}

type SparseIndex struct {
	data *os.File
	rt   *tree.RedBlackTree[string, uint32]
}
