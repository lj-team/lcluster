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

	tBitAndNot := func(k, sk []byte, val int64, wait int64) {
		if st.BitAndNot(k, sk, val, true) != wait {
			t.Fatalf("BitAndNot failed for %v %v %v", k, sk, val)
		}
	}

	tBitOr := func(k, sk []byte, val int64, wait int64) {
		if st.BitOr(k, sk, val, true) != wait {
			t.Fatalf("BitOr failed for %v %v %v", k, sk, val)
		}
	}

	tBitXor := func(k, sk []byte, val int64, wait int64) {
		if st.BitXor(k, sk, val, true) != wait {
			t.Fatalf("BitXor failed for %v %v %v", k, sk, val)
		}
	}

	tSetNX := func(k, sk, val []byte, ok bool, wait []byte) {
		if st.SetNX(k, sk, val, true) != ok {
			t.Fatalf("SetNX failed for %v %v %v", k, sk, val)
		}

		if bytes.Compare(wait, st.Get(k, sk)) != 0 {
			t.Fatalf("SetNX failed for %v %v %v", k, sk, val)
		}
	}

	tGetInt := func(k, sk []byte, wait int64) {
		if st.GetInt(k, sk) != wait {
			t.Fatalf("GetInt failed for %v %v", k, sk)
		}
	}

	tHas := func(k, sk []byte, wait bool) {
		if st.Has(k, sk) != wait {
			t.Fatalf("Has failed for %v %v", k, sk)
		}
	}

	tDel := func(k, sk []byte, wait bool) {
		if st.Del(k, sk, true) != wait {
			t.Fatalf("Del failed for %v %v", k, sk)
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

	tBitAndNot(pack.Encode(int64(7)), nil, 0xffff0000, 0x00f0)
	tBitOr(pack.Encode(int64(7)), nil, 0xffff0000, 0xffff00f0)

	tBitXor(pack.Encode(int64(7)), nil, 0xff00f000, 0x00fff0f0)
	tBitXor(pack.Encode(int64(7)), nil, 0x0, 0x00fff0f0)
	tBitXor(pack.Encode(int64(7)), nil, 0x00f0f000, 0x000f00f0)

	tSetNX(pack.Encode(int64(7)), nil, pack.Int2Bytes(0x00f0f00f), false, pack.Int2Bytes(0x000f00f0))
	tSetNX(pack.Encode(int64(8)), nil, pack.Int2Bytes(0x00f0f00f), true, pack.Int2Bytes(0x00f0f00f))

	tGetInt(pack.Encode(int64(8)), nil, 0x00f0f00f)
	tGetInt(pack.Encode(int64(9)), nil, 0)

	tHas(pack.Encode(int64(8)), nil, true)
	tHas(pack.Encode(int64(9)), nil, false)

	tDel(pack.Encode(int64(8)), nil, true)
	tDel(pack.Encode(int64(9)), nil, false)
}
