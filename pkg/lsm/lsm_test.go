package lsm

import (
	"strconv"
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	l := CreateLSM(100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		l.Set(s, []byte(s))
	}
	l.Flush()
}

func BenchmarkRead(b *testing.B) {
	l := CreateLSM(100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		l.Get(s)
	}
}
