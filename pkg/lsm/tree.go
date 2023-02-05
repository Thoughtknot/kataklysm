package lsm

import (
	"io/ioutil"
	"kataklysm/pkg/filter"
	"kataklysm/pkg/tree"
	"log"
	"os"
	"strings"
)

type LSM struct {
	filter       *filter.BloomFilter
	memb         *tree.RedBlackTree[string, []byte]
	wal          *WAL
	segments     []*Segment
	expectedSize int
}

func CreateLSM(size int) *LSM {
	w, e := os.OpenFile("data/wal", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	wal, tr := NewWAL(w)
	if e != nil {
		log.Fatal("Could not open wal", e)
	}
	files, err := ioutil.ReadDir("data/")
	if err != nil {
		log.Fatal("Could not read data", e)
	}
	count := 0
	for _, v := range files {
		if strings.Contains(v.Name(), "segment-") {
			count++
		}
	}
	segments := make([]*Segment, 0)
	for i := 0; i < count; i++ {
		segments = append(segments, ReadSegment(uint32(i+1)))
	}
	ftr := filter.NewBloomFilter(0.01, uint32(size))
	it := tr.Iterator()
	for it.Next() {
		ftr.Add([]byte(it.Node().Key()))
	}
	return &LSM{
		filter:       ftr,
		memb:         tr,
		wal:          wal,
		segments:     segments,
		expectedSize: size,
	}
}

func (l *LSM) Set(k string, v []byte) {
	l.wal.Set(k, v)
	l.memb.Put(k, v)
	l.filter.Add([]byte(k))
	if l.memb.Size() > l.expectedSize {
		l.Flush()
	}
}

func (l *LSM) Flush() {
	newS := uint32(len(l.segments) + 1)
	l.segments = append(l.segments, CreateSegment(newS, l.memb, l.filter))
	l.memb = tree.New[string, []byte]()
	l.wal.Truncate()
	l.filter = filter.NewBloomFilter(0.01, uint32(l.expectedSize))
}

func (l *LSM) Sync() {
	l.wal.wal.Flush()
}

func (l *LSM) Get(k string) ([]byte, error) {
	r, e := l.memb.Get(k)
	if e == nil {
		return r, nil
	}
	return l.search(k), nil
}

func (l *LSM) search(k string) []byte {
	for i := len(l.segments) - 1; i >= 0; i-- {
		s := l.segments[i]
		r, e := s.Query(k)
		if e == nil {
			return r
		}
	}
	return nil
}
