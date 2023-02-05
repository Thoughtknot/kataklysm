package lsm

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

func Test_SetRead(t *testing.T) {
	t.Run("Set Read", func(t *testing.T) {
		var buf bytes.Buffer
		wal := WAL{wal: bufio.NewWriter(&buf)}
		wal.Set("apa", []byte("apa"))
		wal.Set("foo", []byte("foo"))
		wal.Set("critter", []byte("critter"))
		wal.wal.Flush()
		rbt := read(&buf)
		a, e0 := rbt.Get("apa")
		b, e1 := rbt.Get("foo")
		c, e2 := rbt.Get("critter")
		if e0 != nil || e1 != nil || e2 != nil || string(a) != "apa" || string(b) != "foo" || string(c) != "critter" {
			t.Errorf("Got %v %v %v %v %v %v", a, b, c, e0, e1, e2)
		}
	})
}

func Test_SetReadExtensive(t *testing.T) {
	t.Run("Set Read", func(t *testing.T) {
		file, err := os.Open("../../test.csv")
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)

		var buf bytes.Buffer
		wal := WAL{wal: bufio.NewWriter(&buf)}
		i := 0
		for scanner.Scan() {
			s := strings.Clone(scanner.Text())
			wal.Set(strings.Clone(s), []byte(s))
			i++
		}
		wal.wal.Flush()
		rbt := read(&buf)
		if rbt.Size() != 10000 {
			t.Errorf("Wrong size: %v", rbt.Size())
		}
	})
}
