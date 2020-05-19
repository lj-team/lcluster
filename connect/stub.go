package connect

import (
	"bytes"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lj-team/go-generic/encode/pack"
	"github.com/lj-team/go-generic/slice"
)

type Stub struct {
	store map[string][]byte
	mt    sync.Mutex
}

func NewStub() Cluster {

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

func (st *Stub) KeyTotal(n int) int64 {
	return int64(len(st.store))
}

func (st *Stub) set(key, subkey []byte, value interface{}, sync bool) {
	k := hex.EncodeToString(st.makeKey(key, subkey))

	if value == nil {
		delete(st.store, k)
	} else {

		v := pack.Encode(value)

		if len(v) == 0 {
			delete(st.store, k)
		} else {
			st.store[k] = v
		}
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

func (st *Stub) BitAndNot(key, subkey []byte, value int64, sync bool) int64 {

	st.mt.Lock()
	defer st.mt.Unlock()

	cur := st.get(key, subkey)
	val := pack.Bytes2Int(cur)

	res := val &^ value
	st.set(key, subkey, res, true)

	if !sync {
		return 0
	}

	return res
}

func (st *Stub) BitOr(key, subkey []byte, value int64, sync bool) int64 {

	st.mt.Lock()
	defer st.mt.Unlock()

	cur := st.get(key, subkey)
	val := pack.Bytes2Int(cur)

	res := val | value
	st.set(key, subkey, res, true)

	if !sync {
		return 0
	}

	return res
}

func (st *Stub) BitXor(key, subkey []byte, value int64, sync bool) int64 {

	st.mt.Lock()
	defer st.mt.Unlock()

	cur := st.get(key, subkey)
	val := pack.Bytes2Int(cur)

	res := val ^ value
	st.set(key, subkey, res, true)

	if !sync {
		return 0
	}

	return res
}

func (st *Stub) SetNX(key, subkey []byte, value interface{}, sync bool) bool {

	st.mt.Lock()
	defer st.mt.Unlock()

	cur := st.get(key, subkey)
	if len(cur) > 0 {
		return false
	}

	st.set(key, subkey, value, sync)

	return sync
}

func (st *Stub) GetInt(key, subkey []byte) int64 {
	return pack.Bytes2Int(st.Get(key, subkey))
}

func (st *Stub) Has(key, subkey []byte) bool {
	res := st.Get(key, subkey)
	return len(res) > 0
}

func (st *Stub) Del(key, subkey []byte, sync bool) bool {

	st.mt.Lock()
	defer st.mt.Unlock()

	res := st.get(key, subkey)
	st.set(key, subkey, nil, true)

	if !sync {
		return true
	}

	return len(res) > 0
}

func (st *Stub) Inc(key, subkey []byte, val int64, sync bool) int64 {

	st.mt.Lock()
	defer st.mt.Unlock()

	res := st.get(key, subkey)
	cur := pack.Bytes2Int(res)

	if val > 0 {
		cur = cur + val
		st.set(key, subkey, pack.Int2Bytes(cur), sync)
		if sync {
			return cur
		}
	}

	return val
}

func (st *Stub) Dec(key, subkey []byte, val int64, sync bool) int64 {

	st.mt.Lock()
	defer st.mt.Unlock()

	res := st.get(key, subkey)
	cur := pack.Bytes2Int(res)

	if val > 0 {
		cur = cur - val
		if cur < 0 {
			st.set(key, subkey, nil, sync)
			cur = 0
		} else {
			st.set(key, subkey, pack.Int2Bytes(cur), sync)
		}
		if sync {
			return cur
		}
	}

	return val
}

func (st *Stub) SeqAdd(seq []byte, value interface{}, sync bool) {
	st.Set(seq, pack.Encode(time.Now().UnixNano(), value), oneByte, sync)
}

func (st *Stub) HKeysAll(key []byte) [][]byte {

	hash := hex.EncodeToString(st.makeKey(key, nil))

	var list []string

	for k := range st.store {

		if strings.Index(k, hash) == 0 && hash != k {
			list = append(list, k[len(hash):])
		}
	}

	sort.Sort(sort.StringSlice(list))

	var res [][]byte

	for _, v := range list {
		b, _ := hex.DecodeString(v)
		res = append(res, b)
	}

	return res
}

func (st *Stub) HKeys(key []byte, limit, offset int64) [][]byte {

	keys := st.HKeysAll(key)

	if offset < 0 {
		offset = 0
	}

	if offset >= int64(len(keys)) {
		return [][]byte{}
	}

	keys = keys[int(offset):]

	if limit <= 0 {
		return [][]byte{}
	}

	if len(keys) > int(limit) {
		keys = keys[:int(limit)]
	}

	return keys
}

func (st *Stub) HAll(key []byte) []Pair {

	keys := st.HKeysAll(key)

	var pairs []Pair

	for _, sk := range keys {
		pairs = append(pairs, Pair{Key: sk, Value: st.Get(key, sk)})
	}

	return pairs
}

func (st *Stub) HKeysRand(key []byte, limit int64) [][]byte {

	if limit < 1 {
		return nil
	}

	keys := st.HKeysAll(key)

	if len(keys) <= 1 {
		return keys
	}

	slice.Shuffle(keys)

	if len(keys) > int(limit) {
		keys = keys[:int(limit)]
	}

	return keys
}

func (st *Stub) HKill(key []byte, sync bool) {
	for _, sk := range st.HKeysAll(key) {
		st.Del(key, sk, sync)
	}
}

func (st *Stub) ZKill(key []byte, sync bool) {
	st.HKill(key, sync)
}

func (st *Stub) SeqKill(seq []byte, sync bool) {
	st.HKill(seq, sync)
}

func (st *Stub) HSize(key []byte) int64 {
	keys := st.HKeysAll(key)
	return int64(len(keys))
}

func (st *Stub) SeqRange(seq []byte, limit, offset int64) [][]byte {

	if limit < 1 {
		return [][]byte{}
	}

	keys := st.HKeysAll(seq)
	if offset < 0 {
		offset = 0
	}

	if len(keys) <= int(offset) {
		return [][]byte{}
	}

	keys = keys[int(offset):]
	if len(keys) > int(limit) {
		keys = keys[:int(limit)]
	}

	res := make([][]byte, 0, len(keys))

	for _, v := range keys {
		if len(v) < 9 {
			continue
		}

		res = append(res, v[8:])
	}

	return res
}

func (st *Stub) SeqSize(seq []byte) int64 {
	return st.HSize(seq)
}

func (st *Stub) ZRange(key []byte, limit, offset, min, max int64) []ZRec {

	var recs []ZRec

	for _, p := range st.HAll(key) {
		val := pack.Bytes2Int(p.Value)
		if min <= val && val <= max {
			recs = append(recs, ZRec{Key: p.Key, Value: val})
		}
	}

	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Value > recs[j].Value ||
			recs[i].Value == recs[j].Value && bytes.Compare(recs[i].Key, recs[j].Key) < 0
	})

	if offset < 0 {
		offset = 0
	}

	if len(recs) <= int(offset) {
		return []ZRec{}
	}

	recs = recs[int(offset):]

	if len(recs) > int(limit) {
		recs = recs[:int(limit)]
	}

	return recs
}

func (st *Stub) ZRangeSize(key []byte, min, max int64) int64 {

	var recs []ZRec

	for _, p := range st.HAll(key) {
		val := pack.Bytes2Int(p.Value)
		if min <= val && val <= max {
			recs = append(recs, ZRec{Key: p.Key, Value: val})
		}
	}

	return int64(len(recs))
}
