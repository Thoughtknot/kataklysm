package hash

import "testing"

func TestHash(t *testing.T) {
	type args struct {
		b    []byte
		seed uint32
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "Sunny day",
			args: args{b: []byte("hell"), seed: 1},
			want: 1153506746,
		},
		{
			name: "Overflow",
			args: args{b: []byte("hello"), seed: 1},
			want: 3142237357,
		},
		{
			name: "Long",
			args: args{b: []byte("I am a tiny bunny"), seed: 0},
			want: 5299241,
		},
		{
			name: "Long again",
			args: args{b: []byte("I am a tiny bunny"), seed: 0},
			want: 5299241,
		},
		{
			name: "Long 3",
			args: args{b: []byte("I am a tiny bunny"), seed: 1},
			want: 3305694483,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Hash(tt.args.b, tt.args.seed); got != tt.want {
				t.Errorf("Hash() = %v, want %v", got, tt.want)
			}
		})
	}
}
