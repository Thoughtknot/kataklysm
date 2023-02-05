package filter

import (
	"bytes"
	"math/big"
	"reflect"
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	type args struct {
		probability  float64
		expectedSize uint32
	}
	tests := []struct {
		name string
		args args
		want *BloomFilter
	}{
		{
			name: "Create Filter",
			args: args{0.01, 100000},
			want: &BloomFilter{fpProbability: 0.01, expectedSize: 100000, numBits: 956715, numHashes: 9, bits: big.NewInt(0)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBloomFilter(tt.args.probability, tt.args.expectedSize); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBloomFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBloomFilter_WriteRead(t *testing.T) {
	addedOne := NewBloomFilter(0.01, 100000)
	addedOne.Add([]byte("importantKey"))
	tests := []struct {
		name   string
		filter *BloomFilter
	}{
		{
			name:   "Sunny day",
			filter: NewBloomFilter(0.01, 100000),
		},
		{
			name:   "Single entry",
			filter: addedOne,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.filter
			w := &bytes.Buffer{}
			f.Write(w)
			r, e := Read(w)
			if e != nil {
				t.Errorf("Got error %v", e)
			} else if !reflect.DeepEqual(r, f) {
				t.Errorf("BloomFilter.Write() = %v, want %v", r, f)
			}
		})
	}
}

func TestBloomFilter_AddQuery(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		get  []byte
		want bool
	}{
		{
			name: "Exists 1",
			keys: []string{
				"foo",
				"bar",
				"baz",
			},
			get:  []byte("foo"),
			want: true,
		},
		{
			name: "Exists 2",
			keys: []string{
				"foo",
				"bar",
				"baz",
			},
			get:  []byte("bar"),
			want: true,
		},
		{
			name: "Exists 3",
			keys: []string{
				"foo",
				"bar",
				"baz",
			},
			get:  []byte("baz"),
			want: true,
		},
		{
			name: "Negative 1",
			keys: []string{
				"foo",
				"bar",
				"baz",
			},
			get:  []byte("apa"),
			want: false,
		},
		{
			name: "Negative 2",
			keys: []string{
				"foo",
				"bar",
				"baz",
			},
			get:  []byte("summer"),
			want: false,
		},
		{
			name: "Negative 3",
			keys: []string{
				"foo",
				"bar",
				"baz",
			},
			get:  []byte("knut"),
			want: false,
		},
		{
			name: "Negative 4",
			keys: []string{
				"foo",
				"bar",
				"baz",
			},
			get:  []byte("knat"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewBloomFilter(0.01, 100)
			println("foo")
			for _, v := range tt.keys {
				f.Add([]byte(v))
			}
			if got := f.Query(tt.get); got != tt.want {
				t.Errorf("BloomFilter.Query() = %v, want %v", got, tt.want)
			}
		})
	}
}
