package connect

import (
	"encoding/hex"
	"sync"

	"github.com/lj-team/go-generic/encode/pack"
)

type Stub struct {
	store map[string][]byte
	mt    sync.Mutex
}

func NewStub() *Stub {

	st := &Stub{
		store: make(map[string][]byte, 1024),
	}

	return st
}

func (st *Stub) makeKey(key, subkey []byte) []byte {
	size := 1 + len(key)

	keybuf := make([]byte, 2048)

	keybuf[0] = byte(size)
	copy(keybuf[1:], key)

	if subkey == nil {
		return keybuf[:size]
	}

	copy(keybuf[size:], subkey)
	size += len(subkey)

	if size > 512 {
		size = 512
	}

	return keybuf[:size]
}

func (st *Stub) Status() bool {
	return true
}

func (st *Stub) KeyTotal(n int) int {
	return len(st.store)
}

func (st *Stub) set(key, subkey []byte, value interface{}, sync bool) {
	k := hex.EncodeToString(st.makeKey(key, subkey))
	v := pack.Encode(value)

	if len(v) == 0 {
		delete(st.store, k)
	} else {
		st.store[k] = v
	}
}

func (st *Stub) Set(key, subkey []byte, value interface{}, sync bool) {
	st.mt.Lock()
	defer st.mt.Unlock()
	st.set(key, subkey, value, sync)
}

func (st *Stub) SetIfMore(key, subkey []byte, value int64, sync bool) int64 {
	st.mt.Lock()
	defer st.mt.Unlock()

	cur := st.get(key, subkey)
	val := pack.Bytes2Int(cur)

	if val >= value {
		if !sync {
			return 0
		}
		return val
	}

	st.set(key, subkey, value, false)

	return value
}

func (st *Stub) get(key, subkey []byte) []byte {
	k := hex.EncodeToString(st.makeKey(key, subkey))
	if v, has := st.store[k]; has {
		return v
	}
	return nil
}

func (st *Stub) Get(key, subkey []byte) []byte {
	st.mt.Lock()
	defer st.mt.Unlock()

	return st.get(key, subkey)
}

func (st *Stub) BitAnd(key, subkey []byte, value int64, sync bool) int64 {

	st.mt.Lock()
	defer st.mt.Unlock()

	cur := st.get(key, subkey)
	val := pack.Bytes2Int(cur)

	res := val & value
	st.set(key, subkey, res, true)

	if !sync {
		return 0
	}

	return res
}
