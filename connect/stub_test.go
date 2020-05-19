package connect

import (
	"bytes"
	"testing"

	"github.com/lj-team/go-generic/encode/pack"
)

func TestSub(t *testing.T) {

	fp := FakeMultiProxy([]string{"t1", "t2", "t3"})

	st := fp.Get("t1")
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

	tInc := func(k, sk []byte, val int64, wait int64) {
		if st.Inc(k, sk, val, true) != wait {
			t.Fatalf("Inc failed for %v %v %v %v", k, sk, val, wait)
		}

		if st.GetInt(k, sk) != wait {
			t.Fatalf("Inc failed for %v %v %v %v", k, sk, val, wait)
		}
	}

	tDec := func(k, sk []byte, val int64, wait int64) {
		if st.Dec(k, sk, val, true) != wait {
			t.Fatalf("Dec failed for %v %v %v %v", k, sk, val, wait)
		}

		if st.GetInt(k, sk) != wait {
			t.Fatalf("Dec failed for %v %v %v %v", k, sk, val, wait)
		}
	}

	tSeqAdd := func(seq []byte, val interface{}) {
		st.SeqAdd(seq, val, true)
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

	tInc(pack.Encode(int64(10)), nil, 1, 1)
	tInc(pack.Encode(int64(10)), nil, 2, 3)
	tDec(pack.Encode(int64(10)), nil, 1, 2)
	tDec(pack.Encode(int64(10)), nil, 1, 1)
	tDec(pack.Encode(int64(10)), nil, 2, 0)

	tSeqAdd([]byte("seq"), int64(1))
	tSeqAdd([]byte("seq"), int64(2))
	tSeqAdd([]byte("seq"), int64(3))

	tSet([]byte("hash"), pack.Int2Bytes(1), pack.Int2Bytes(11))
	tSet([]byte("hash"), pack.Int2Bytes(2), pack.Int2Bytes(22))
	tSet([]byte("hash"), pack.Int2Bytes(3), pack.Int2Bytes(33))
	tSet([]byte("hash"), pack.Int2Bytes(4), pack.Int2Bytes(44))
	tSet([]byte("hash"), pack.Int2Bytes(5), pack.Int2Bytes(55))

	keys := st.HKeysAll([]byte("hash"))
	if len(keys) != 5 {
		t.Fatal("HKeysAll return invalid number of elements")
	}

	for i, v := range keys {
		if bytes.Compare(v, pack.Int2Bytes(int64(i+1))) != 0 {
			t.Fatal("HKeysAll return invalid list")
		}
	}

	keys = st.HKeys([]byte("hash"), 3, 1)
	if len(keys) != 3 {
		t.Fatal("HKeys return invalid number of elements")
	}

	for i, v := range keys {
		if bytes.Compare(v, pack.Int2Bytes(int64(i+2))) != 0 {
			t.Fatal("HKeys return invalid list")
		}
	}

	pairs := st.HAll([]byte("hash"))

	if len(pairs) != 5 {
		t.Fatal("HAll failed")
	}

	for i, v := range pairs {

		if bytes.Compare(pack.Int2Bytes(int64(i+1)), v.Key) != 0 {
			t.Fatal("HAll invalid key")
		}

		if bytes.Compare(pack.Int2Bytes(int64((i+1)*10+i+1)), v.Value) != 0 {
			t.Fatal("HAll invalid key")
		}
	}

	keys = st.HKeysRand([]byte("hash"), 3)
	if len(keys) != 3 {
		t.Fatal("HKeysRand failed")
	}

	if st.HSize([]byte("hash")) != 5 {
		t.Fatal("HSize failed")
	}

	st.HKill([]byte("hash"), false)

	pairs = st.HAll([]byte("hash"))
	if len(pairs) != 0 {
		t.Fatal("HKill failed")
	}

	if st.HSize([]byte("hash")) != 0 {
		t.Fatal("HSize failed")
	}

	keys = st.SeqRange([]byte("seq"), 2, 1)
	if len(keys) != 2 {
		t.Fatal("SeqRange failed")
	}

	for i, v := range keys {
		if bytes.Compare(v, pack.Int2Bytes(int64(i+2))) != 0 {
			t.Fatal("SeqRange failed")
		}
	}

	if st.SeqSize([]byte("seq")) != 3 {
		t.Fatal("SeqSize failed")
	}

	st.SeqKill([]byte("seq"), false)
	pairs = st.HAll([]byte("seq"))
	if len(pairs) != 0 {
		t.Fatal("SeqKill failed")
	}

	tSet([]byte("zset"), pack.Int2Bytes(1), pack.Int2Bytes(11))
	tSet([]byte("zset"), pack.Int2Bytes(2), pack.Int2Bytes(22))
	tSet([]byte("zset"), pack.Int2Bytes(3), pack.Int2Bytes(33))
	tSet([]byte("zset"), pack.Int2Bytes(4), pack.Int2Bytes(44))
	tSet([]byte("zset"), pack.Int2Bytes(5), pack.Int2Bytes(55))
	tSet([]byte("zset"), pack.Int2Bytes(6), pack.Int2Bytes(66))
	tSet([]byte("zset"), pack.Int2Bytes(7), pack.Int2Bytes(77))
	tSet([]byte("zset"), pack.Int2Bytes(8), pack.Int2Bytes(88))
	tSet([]byte("zset"), pack.Int2Bytes(9), pack.Int2Bytes(99))

	if st.HSize([]byte("zset")) != 9 {
		t.Fatal("Set failed")
	}

	if st.ZRangeSize([]byte("zset"), 0, 10) != 0 {
		t.Fatal("ZRangeSize failed")
	}

	if st.ZRangeSize([]byte("zset"), 10, 60) != 5 {
		t.Fatal("ZRangeSize failed")
	}

	if st.ZRangeSize([]byte("zset"), 0, 100) != 9 {
		t.Fatal("ZRangeSize failed")
	}

	for i, v := range st.ZRange([]byte("zset"), 10, 0, 0, 100) {
		if v.Value != int64((9-i)*10+9-i) {
			t.Fatal("ZRange failed")
		}
		if bytes.Compare(v.Key, pack.Int2Bytes(int64(9-i))) != 0 {
			t.Fatal("ZRange failed")
		}
	}

	st.ZKill([]byte("zset"), false)

	if st.HSize([]byte("zset")) != 0 {
		t.Fatal("ZKill failed")
	}
}
