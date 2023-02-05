package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"kataklysm/pkg/tree"
	"log"
	"os"
)

type WAL struct {
	wal  *bufio.Writer
	file *os.File
}

func NewWAL(f *os.File) (*WAL, *tree.RedBlackTree[string, []byte]) {
	bts, e := io.ReadAll(f)
	if e != nil {
		log.Fatal(e)
	}
	t := read(bytes.NewBuffer(bts))
	wal := &WAL{wal: bufio.NewWriter(f), file: f}
	return wal, t
}

func read(fl io.Reader) *tree.RedBlackTree[string, []byte] {
	t := tree.New[string, []byte]()
	i := 0
	for {
		h := make([]byte, 2)
		u := make([]byte, 4)
		_, e0 := fl.Read(h)
		if e0 != nil {
			break
		}
		hv := binary.LittleEndian.Uint16(h)
		k := make([]byte, hv)
		_, e1 := fl.Read(k)
		if e1 != nil {
			log.Fatal("Corrupted WAL log 1: ", e1)
		}
		_, e2 := fl.Read(u)
		if e2 != nil {
			log.Fatal("Corrupted WAL log 2: ", e2)
		}
		uv := binary.LittleEndian.Uint32(u)
		v := make([]byte, uv)
		_, e3 := fl.Read(v)
		if e3 != nil {
			log.Fatal("Corrupted WAL log 3: ", e3)
		}
		t.Put(string(k), v)
		i++
	}
	return t
}

func (w *WAL) Set(k string, v []byte) {
	h := make([]byte, 2)
	u := make([]byte, 4)
	binary.LittleEndian.PutUint16(h, uint16(len(k)))
	binary.LittleEndian.PutUint32(u, uint32(len(v)))
	_, e := w.wal.Write(h)
	if e != nil {
		log.Fatal(e)
	}
	_, e = w.wal.Write([]byte(k))
	if e != nil {
		log.Fatal(e)
	}
	_, e = w.wal.Write(u)
	if e != nil {
		log.Fatal(e)
	}
	_, e = w.wal.Write(v)
	if e != nil {
		log.Fatal(e)
	}
	//w.wal.Sync()
}

func (w *WAL) Truncate() {
	w.file.Truncate(0)
	w.wal.Reset(w.file)
}
