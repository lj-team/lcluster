package connect

import (
	"bytes"
	"testing"

	"github.com/lj-team/go-generic/encode/pack"
)

func TestSub(t *testing.T) {

	st := NewStub()
	if st == nil {
		t.Fatal("NewStub failed")
	}

	tSet := func(k, sk, v []byte) {
		st.Set(k, sk, v, true)

		res := st.Get(k, sk)
		if bytes.Compare(v, res) != 0 {
			t.Fatalf("Set failed for %v %v %v", k, sk, v)
		}
	}

	tSetIfMore := func(k, sk []byte, val int64, wait int64) {
		if st.SetIfMore(k, sk, val, true) != wait {
			t.Fatalf("SetIfMore failed for %v %v %v", k, sk, val)
		}
	}

	tBitAnd := func(k, sk []byte, val int64, wait int64) {
		if st.BitAnd(k, sk, val, true) != wait {
			t.Fatalf("BitAnd failed for %v %v %v", k, sk, val)
		}
	}

	tSet(pack.Encode(int64(1)), nil, pack.Encode(int64(11)))
	tSet(pack.Encode(int64(2)), nil, pack.Encode(int64(22)))
	tSet(pack.Encode(int64(3)), pack.Encode(int64(4)), pack.Encode(int64(54)))
	tSet(pack.Encode(int64(2)), nil, nil)

	tSetIfMore(pack.Encode(int64(2)), nil, 2, 2)
	tSetIfMore(pack.Encode(int64(2)), nil, 1, 2)

	tBitAnd(pack.Encode(int64(7)), nil, 0xffff, 0)
	tSet(pack.Encode(int64(7)), nil, pack.Encode(int64(0x0f00f0)))
	tBitAnd(pack.Encode(int64(7)), nil, 0xffff, 0x00f0)
}
