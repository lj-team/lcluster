package engine

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lj-team/go-generic/db/ldb"
	"github.com/lj-team/go-generic/encode/pack"
	"github.com/lj-team/lcluster/pb"
)

type HANDLER func(*pb.LCPROTO) *pb.LCPROTO

var mutex sync.Mutex

var callbacks map[pb.LCPROTO_Code]HANDLER = map[pb.LCPROTO_Code]HANDLER{
	pb.LCPROTO_BITAND:      handleBitAND,
	pb.LCPROTO_BITOR:       handleBitOR,
	pb.LCPROTO_BITXOR:      handleBitXOR,
	pb.LCPROTO_DEC:         handleDec,
	pb.LCPROTO_DECBY:       handleDecBy,
	pb.LCPROTO_DECR:        handleDecr,
	pb.LCPROTO_DEL:         handleDel,
	pb.LCPROTO_DELR:        handleDelR,
	pb.LCPROTO_GET:         handleGet,
	pb.LCPROTO_HALL:        handleHAll,
	pb.LCPROTO_HKEYS:       handleHKeys,
	pb.LCPROTO_HKEYSLIMIT:  handleHKeysLimit,
	pb.LCPROTO_HKEYSRANDOM: handleHKeysRandom,
	pb.LCPROTO_HKEYSTOTAL:  handleHKeysTotal,
	pb.LCPROTO_HKILL:       handleHKill,
	pb.LCPROTO_HAS:         handleHas,
	pb.LCPROTO_INC:         handleInc,
	pb.LCPROTO_INCBY:       handleIncBy,
	pb.LCPROTO_INCR:        handleIncr,
	pb.LCPROTO_KEYTOTAL:    handleKeyTotal,
	pb.LCPROTO_LOG:         handleLog,
	pb.LCPROTO_SET:         handleSet,
	pb.LCPROTO_SETR:        handleSetR,
	pb.LCPROTO_SETNX:       handleSetNX,
	pb.LCPROTO_ZKILL:       handleZKill,
	pb.LCPROTO_ZRANGE:      handleZRange,
	pb.LCPROTO_ZRANGESIZE:  handleZRangeSize,

	pb.LCPROTO_C_BITAND:     handleCBitAND,
	pb.LCPROTO_C_BITANDNOT:  handleCBitANDNOT,
	pb.LCPROTO_C_BITOR:      handleCBitOR,
	pb.LCPROTO_C_BITXOR:     handleCBitXOR,
	pb.LCPROTO_C_DEC:        handleCDec,
	pb.LCPROTO_C_DEL:        handleCDel,
	pb.LCPROTO_C_GET:        handleCGet,
	pb.LCPROTO_C_GETINT:     handleCGetInt,
	pb.LCPROTO_C_HALL:       handleCHAll,
	pb.LCPROTO_C_HAS:        handleCHas,
	pb.LCPROTO_C_HKEYS:      handleCHKeys,
	pb.LCPROTO_C_HKEYSRAND:  handleCHKeysRand,
	pb.LCPROTO_C_HKILL:      handleCHKill,
	pb.LCPROTO_C_HSIZE:      handleCHSize,
	pb.LCPROTO_C_INC:        handleCInc,
	pb.LCPROTO_C_KEYTOTAL:   handleCKeyTotal,
	pb.LCPROTO_C_NOP:        handleCNop,
	pb.LCPROTO_C_SET:        handleCSet,
	pb.LCPROTO_C_SETNX:      handleCSetNX,
	pb.LCPROTO_C_SETIFMORE:  handleCSetIfMore,
	pb.LCPROTO_C_ZKILL:      handleCZKill,
	pb.LCPROTO_C_ZRANGE:     handleCZRange,
	pb.LCPROTO_C_ZRANGESIZE: handleCZRangeSize,
}

func handler(req []byte) ([]byte, error) {

	var msg pb.LCPROTO

	err := proto.Unmarshal(req, &msg)
	if err != nil {
		return nil, err
	}

	f, ok := callbacks[msg.Code]
	if !ok {
		return nil, errors.New("unknown command code")
	}

	res := f(&msg)

	if res == nil {
		return nil, nil
	}

	res.Code = pb.LCPROTO_RESP
	rbuf, _ := proto.Marshal(res)

	return rbuf, nil
}

func bool2Bytes(val bool) []byte {
	buf := make([]byte, 1)

	if val {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	return buf
}

func handleDel(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	ldb.Del(msg.Key)
	mutex.Unlock()
	repl.Log(msg.Key, nil, 1)
	return nil
}

func handleCDel(msg *pb.LCPROTO) *pb.LCPROTO {
	res := int64(1)
	mutex.Lock()

	if msg.Sync && !ldb.Has(msg.Key) {
		res = 0
	}

	if res == 1 {
		ldb.Del(msg.Key)
	}

	mutex.Unlock()
	repl.Log(msg.Key, nil, 1)

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: res}
	}

	return nil
}

func handleDelR(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	has := ldb.Has(msg.Key)
	if has {
		ldb.Del(msg.Key)
	}
	mutex.Unlock()
	repl.Log(msg.Key, nil, 1)

	return &pb.LCPROTO{Key: msg.Key, Value: bool2Bytes(has)}
}

func handleSet(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	ldb.Set(msg.Key, msg.Value)
	mutex.Unlock()
	repl.Log(msg.Key, msg.Value, 1)
	return nil
}

func handleCSet(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	ldb.Set(msg.Key, msg.Value)
	mutex.Unlock()
	repl.Log(msg.Key, msg.Value, 1)
	if msg.Sync {
		return &pb.LCPROTO{Ivalue: 1}
	}
	return nil
}

func handleCSetIfMore(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	old := pack.Bytes2Int(ldb.Get(msg.Key))
	new := msg.Ivalue
	if new > old {
		ldb.Set(msg.Key, pack.Int2Bytes(new))
		old = new
	}
	res := pack.Int2Bytes(old)
	mutex.Unlock()
	repl.Log(msg.Key, res, 1)

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: old}
	}

	return nil
}

func handleSetR(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	ldb.Set(msg.Key, msg.Value)
	mutex.Unlock()
	repl.Log(msg.Key, msg.Value, 1)
	return &pb.LCPROTO{Value: pack.Int2Bytes(1)}
}

func handleSetNX(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	has := ldb.Has(msg.Key)
	ldb.Set(msg.Key, msg.Value)
	mutex.Unlock()
	repl.Log(msg.Key, msg.Value, 1)

	return &pb.LCPROTO{Value: bool2Bytes(!has)}
}

func handleCSetNX(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	has := ldb.Has(msg.Key)
	if !has {
		ldb.Set(msg.Key, msg.Value)
	}
	mutex.Unlock()
	repl.Log(msg.Key, msg.Value, 1)

	if msg.Sync {
		return &pb.LCPROTO{Value: bool2Bytes(!has)}
	}

	return nil
}

func handleGet(msg *pb.LCPROTO) *pb.LCPROTO {
	res := ldb.Get(msg.Key)
	repl.Log(msg.Key, res, 1)
	return &pb.LCPROTO{Value: res}
}

func handleCGet(msg *pb.LCPROTO) *pb.LCPROTO {
	res := ldb.Get(msg.Key)
	repl.Log(msg.Key, res, 1)
	return &pb.LCPROTO{Value: res}
}

func handleCNop(msg *pb.LCPROTO) *pb.LCPROTO {
	return &pb.LCPROTO{Value: []byte{1}}
}

func handleCGetInt(msg *pb.LCPROTO) *pb.LCPROTO {
	res := ldb.Get(msg.Key)
	repl.Log(msg.Key, res, 1)
	val := pack.Bytes2Int(res)
	return &pb.LCPROTO{Ivalue: val}
}

func handleCBitAND(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	res := ldb.Get(msg.Key)
	v1 := pack.Bytes2Int(res)
	v2 := msg.Ivalue
	ires := v1 & v2
	res = pack.Int2Bytes(ires)
	ldb.Set(msg.Key, res)
	mutex.Unlock()

	repl.Log(msg.Key, res, 1)

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: ires}
	}

	return nil
}

func handleCBitANDNOT(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	res := ldb.Get(msg.Key)
	v1 := pack.Bytes2Int(res)
	v2 := msg.Ivalue
	ires := v1 &^ v2
	res = pack.Int2Bytes(ires)
	ldb.Set(msg.Key, res)
	mutex.Unlock()

	repl.Log(msg.Key, res, 1)

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: ires}
	}

	return nil
}

func handleCBitOR(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	res := ldb.Get(msg.Key)
	v1 := pack.Bytes2Int(res)
	v2 := msg.Ivalue
	ires := v1 | v2
	res = pack.Int2Bytes(ires)
	ldb.Set(msg.Key, res)
	mutex.Unlock()

	repl.Log(msg.Key, res, 1)

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: ires}
	}

	return nil
}

func handleCBitXOR(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()
	res := ldb.Get(msg.Key)
	v1 := pack.Bytes2Int(res)
	v2 := msg.Ivalue
	ires := v1 ^ v2
	res = pack.Int2Bytes(ires)
	ldb.Set(msg.Key, res)
	mutex.Unlock()

	repl.Log(msg.Key, res, 1)

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: ires}
	}

	return nil
}

func handleBitAND(msg *pb.LCPROTO) *pb.LCPROTO {
	res := ldb.Get(msg.Key)
	v1 := pack.Bytes2Int(res)
	v2 := pack.Bytes2Int(msg.Value)
	res = pack.Int2Bytes(v1 & v2)
	ldb.Set(msg.Key, res)
	repl.Log(msg.Key, res, 1)
	return nil
}

func handleBitOR(msg *pb.LCPROTO) *pb.LCPROTO {
	res := ldb.Get(msg.Key)
	v1 := pack.Bytes2Int(res)
	v2 := pack.Bytes2Int(msg.Value)
	res = pack.Int2Bytes(v1 | v2)
	ldb.Set(msg.Key, res)
	repl.Log(msg.Key, res, 1)
	return nil
}

func handleBitXOR(msg *pb.LCPROTO) *pb.LCPROTO {
	res := ldb.Get(msg.Key)
	v1 := pack.Bytes2Int(res)
	v2 := pack.Bytes2Int(msg.Value)
	res = pack.Int2Bytes(v1 ^ v2)
	ldb.Set(msg.Key, res)
	repl.Log(msg.Key, res, 1)
	return nil
}

func handleHas(msg *pb.LCPROTO) *pb.LCPROTO {
	res := ldb.Has(msg.Key)
	return &pb.LCPROTO{Value: bool2Bytes(res)}
}

func handleCHas(msg *pb.LCPROTO) *pb.LCPROTO {
	res := ldb.Has(msg.Key)
	data := int64(0)
	if res {
		data = 1
	}
	return &pb.LCPROTO{Ivalue: data}
}

func handleCInc(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	res := ldb.Get(msg.Key)
	cur := pack.Bytes2Int(res)

	if msg.Ivalue > 0 {
		cur = cur + msg.Ivalue
	}

	res = pack.Int2Bytes(cur)
	ldb.Set(msg.Key, res)

	mutex.Unlock()

	repl.Log(msg.Key, res, 1)

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: cur}
	}

	return nil
}

func handleCDec(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	res := ldb.Get(msg.Key)
	cur := pack.Bytes2Int(res)

	if msg.Ivalue > 0 {
		cur = cur - msg.Ivalue
		if cur < 0 {
			cur = 0
		}
	}

	res = pack.Int2Bytes(cur)
	ldb.Set(msg.Key, res)

	mutex.Unlock()

	repl.Log(msg.Key, res, 1)

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: cur}
	}

	return nil
}

func handleIncr(msg *pb.LCPROTO) *pb.LCPROTO {

	mutex.Lock()

	res := ldb.Get(msg.Key)

	val := pack.Bytes2Int(res)
	val++
	buf := pack.Int2Bytes(val)

	ldb.Set(msg.Key, buf)

	mutex.Unlock()

	repl.Log(msg.Key, buf, 1)

	return &pb.LCPROTO{Value: buf}
}

func handleDecr(msg *pb.LCPROTO) *pb.LCPROTO {

	mutex.Lock()

	res := ldb.Get(msg.Key)

	val := pack.Bytes2Int(res)

	if val > 0 {
		val--
	}

	buf := pack.Int2Bytes(val)

	ldb.Set(msg.Key, buf)

	mutex.Unlock()

	repl.Log(msg.Key, buf, 1)

	return &pb.LCPROTO{Value: buf}
}

func handleInc(msg *pb.LCPROTO) *pb.LCPROTO {

	mutex.Lock()

	res := ldb.Get(msg.Key)

	val := pack.Bytes2Int(res)
	val++
	buf := pack.Int2Bytes(val)

	ldb.Set(msg.Key, buf)

	mutex.Unlock()

	repl.Log(msg.Key, buf, 1)

	return nil
}

func handleIncBy(msg *pb.LCPROTO) *pb.LCPROTO {

	mutex.Lock()

	res := ldb.Get(msg.Key)

	val := pack.Bytes2Int(res)
	val = val + pack.Bytes2Int(msg.Value)

	buf := pack.Int2Bytes(val)

	ldb.Set(msg.Key, buf)

	mutex.Unlock()

	repl.Log(msg.Key, buf, 1)

	return nil
}

func handleHKill(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	ldb.ForEach(msg.Key, false, func(key []byte, value []byte) bool {
		ldb.Del(key)
		repl.Log(key, nil, 1)
		return true
	})

	mutex.Unlock()

	return nil
}

func handleHAll(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	var res [][]byte

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		if res == nil {
			res = make([][]byte, 0, 100)
		}

		size := len(key)
		data := make([]byte, size)
		copy(data, key)
		res = append(res, data)

		size = len(value)
		data = make([]byte, size)
		copy(data, value)
		res = append(res, data)

		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{List: res}
}

func handleHKeys(msg *pb.LCPROTO) *pb.LCPROTO {

	res := make([][]byte, 0)

	return &pb.LCPROTO{List: res}
}

func handleCHSize(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	res := int64(0)

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {
		res++
		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{Ivalue: res}
}

func handleHKeysTotal(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	res := int64(0)

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {
		res++
		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{Value: pack.Int2Bytes(res)}
}

func handleKeyTotal(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	res := int64(0)

	ldb.ForEach([]byte{}, false, func(key []byte, value []byte) bool {
		res++
		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{Value: pack.Int2Bytes(res)}
}

func handleCKeyTotal(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	res := int64(0)

	ldb.ForEach([]byte{}, false, func(key []byte, value []byte) bool {
		res++
		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{Ivalue: res}
}

func handleCHKeys(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	var res [][]byte
	args := pack.Bytes2IntList(msg.Value)

	if args == nil || len(args) != 2 || args[0] == 0 {
		mutex.Unlock()
		return &pb.LCPROTO{List: res}
	}

	offset := args[1]
	limit := args[0]
	i := int64(-1)

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		i++

		if i < offset {
			return true
		}

		if limit <= 0 {
			return false
		}

		limit--

		if res == nil {
			res = make([][]byte, 0, 100)
		}

		size := len(key)
		data := make([]byte, size)
		copy(data, key)
		res = append(res, data)

		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{List: res}
}

func handleCHAll(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	var res [][]byte

	i := int64(-1)

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		i++

		if res == nil {
			res = make([][]byte, 0, 100)
		}

		size := len(key)
		data := make([]byte, size)
		copy(data, key)
		res = append(res, data)

		size = len(value)
		data = make([]byte, size)
		copy(data, value)
		res = append(res, data)

		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{List: res}
}

func handleHKeysLimit(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	var res [][]byte
	args := pack.Bytes2IntList(msg.Value)

	if args == nil || len(args) != 2 || args[0] == 0 {
		return &pb.LCPROTO{List: res}
	}

	offset := args[1]
	limit := args[0]
	i := int64(-1)

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		i++

		if i < offset {
			return true
		}

		if limit <= 0 {
			return false
		}

		limit--

		if res == nil {
			res = make([][]byte, 0, 100)
		}

		size := len(key)
		data := make([]byte, size)
		copy(data, key)
		res = append(res, data)

		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{List: res}
}

func handleCHKeysRand(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	var recs []*ZRec

	limit := int64(100)

	if msg.Value != nil && len(msg.Value) >= 8 {
		limit = pack.Bytes2Int(msg.Value)
	}

	if limit < 1 {
		mutex.Unlock()
		return &pb.LCPROTO{List: nil}
	}

	i := int64(-1)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		i++

		if i == 0 {
			recs = make([]*ZRec, 0, 100)
		}

		v := make([]byte, len(key))
		copy(v, key)

		recs = append(recs, &ZRec{Key: v, Value: rnd.Int63()})

		return true
	})

	mutex.Unlock()

	sort.Sort(ZSet(recs))

	if int64(len(recs)) > limit {
		recs = recs[:limit]
	}

	result := make([][]byte, len(recs))

	for i, v := range recs {
		result[i] = v.Key
	}

	return &pb.LCPROTO{List: result}
}

func handleHKeysRandom(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	var recs []*ZRec

	limit := int64(100)

	if msg.Value != nil && len(msg.Value) >= 8 {
		limit = pack.Bytes2Int(msg.Value)
	}

	if limit < 1 {
		return &pb.LCPROTO{List: nil}
	}

	i := int64(-1)

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		i++

		if i == 0 {
			recs = make([]*ZRec, 0, 100)
		}

		v := make([]byte, len(key))
		copy(v, key)

		recs = append(recs, &ZRec{Key: v, Value: rnd.Int63()})

		return true
	})

	mutex.Unlock()

	sort.Sort(ZSet(recs))

	if int64(len(recs)) > limit {
		recs = recs[:limit]
	}

	result := make([][]byte, len(recs))

	for i, v := range recs {
		result[i] = v.Key
	}

	return &pb.LCPROTO{List: result}
}

func handleDec(msg *pb.LCPROTO) *pb.LCPROTO {

	mutex.Lock()

	res := ldb.Get(msg.Key)

	val := pack.Bytes2Int(res)

	if val > 0 {
		val--
	}

	buf := pack.Int2Bytes(val)

	ldb.Set(msg.Key, buf)

	mutex.Unlock()

	repl.Log(msg.Key, buf, 1)

	return nil
}

func handleDecBy(msg *pb.LCPROTO) *pb.LCPROTO {

	mutex.Lock()

	res := ldb.Get(msg.Key)

	val := pack.Bytes2Int(res)
	val = val - pack.Bytes2Int(msg.Value)

	if val < 0 {
		val = 0
	}

	buf := pack.Int2Bytes(val)

	ldb.Set(msg.Key, buf)

	mutex.Unlock()

	repl.Log(msg.Key, buf, 1)

	return nil
}

func handleLog(msg *pb.LCPROTO) *pb.LCPROTO {

	mutex.Lock()
	defer mutex.Unlock()

	if msg.Counter == 1 {
		if msg.Value == nil || len(msg.Value) == 0 {
			ldb.Del(msg.Key)
		} else {
			ldb.Set(msg.Key, msg.Value)
		}
	}
	return nil
}

func handleCHKill(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	ldb.ForEach(msg.Key, false, func(key []byte, value []byte) bool {
		ldb.Del(key)
		repl.Log(key, nil, 1)
		return true
	})

	mutex.Unlock()

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: 1}
	}

	return nil
}

func handleCZKill(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	ldb.ForEach(msg.Key, false, func(key []byte, value []byte) bool {
		ldb.Del(key)
		repl.Log(key, nil, 1)
		return true
	})

	mutex.Unlock()

	if msg.Sync {
		return &pb.LCPROTO{Ivalue: 1}
	}

	return nil
}

func handleCZRange(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	args := pack.Bytes2IntList(msg.Value)
	if len(args) != 4 {
		mutex.Unlock()
		return &pb.LCPROTO{List: [][]byte{}}
	}

	limit := args[0]
	offset := args[1]
	min := args[2]
	max := args[3]

	if limit < 1 || min > max {
		mutex.Unlock()
		return &pb.LCPROTO{List: [][]byte{}}
	}

	var list []*ZRec

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		if list == nil {
			list = make([]*ZRec, 0, 100)
		}

		size := len(key)
		data := make([]byte, size)
		copy(data, key)

		i := pack.Bytes2Int(value)

		if i >= min && i <= max {
			list = append(list, &ZRec{Key: data, Value: i})
		}

		return true
	})

	mutex.Unlock()

	if offset >= int64(len(list)) {
		return &pb.LCPROTO{List: [][]byte{}}
	}

	last := offset + limit

	if last >= int64(len(list)) {
		last = int64(len(list))
	}

	sort.Sort(ZSet(list))

	list = list[offset:last]

	res := make([][]byte, len(list)*2)

	for i, v := range list {
		res[i*2] = v.Key
		res[i*2+1] = pack.Int2Bytes(v.Value)
	}

	return &pb.LCPROTO{List: res}
}

func handleCZRangeSize(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	args := pack.Bytes2IntList(msg.Value)
	if len(args) != 2 {
		mutex.Unlock()
		return &pb.LCPROTO{Ivalue: 0}
	}

	min := args[0]
	max := args[1]

	if min > max {
		mutex.Unlock()
		return &pb.LCPROTO{Value: pack.Int2Bytes(int64(0))}
	}

	total := int64(0)

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		i := pack.Bytes2Int(value)

		if i >= min && i <= max {
			total++
		}

		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{Ivalue: total}
}

func handleZKill(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	ldb.ForEach(msg.Key, false, func(key []byte, value []byte) bool {
		ldb.Del(key)
		repl.Log(key, nil, 1)
		return true
	})

	mutex.Unlock()

	return nil
}

func handleZRange(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	args := pack.Bytes2IntList(msg.Value)
	if len(args) != 4 {
		return &pb.LCPROTO{List: [][]byte{}}
	}

	limit := args[0]
	offset := args[1]
	min := args[2]
	max := args[3]

	if limit < 1 || min > max {
		return &pb.LCPROTO{List: [][]byte{}}
	}

	var list []*ZRec

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		if list == nil {
			list = make([]*ZRec, 0, 100)
		}

		size := len(key)
		data := make([]byte, size)
		copy(data, key)

		i := pack.Bytes2Int(value)

		if i >= min && i <= max {
			list = append(list, &ZRec{Key: data, Value: i})
		}

		return true
	})

	mutex.Unlock()

	if offset >= int64(len(list)) {
		return &pb.LCPROTO{List: [][]byte{}}
	}

	last := offset + limit

	if last >= int64(len(list)) {
		last = int64(len(list))
	}

	sort.Sort(ZSet(list))

	list = list[offset:last]

	res := make([][]byte, len(list)*2)

	for i, v := range list {
		res[i*2] = v.Key
		res[i*2+1] = pack.Int2Bytes(v.Value)
	}

	return &pb.LCPROTO{List: res}
}

func handleZRangeSize(msg *pb.LCPROTO) *pb.LCPROTO {
	mutex.Lock()

	args := pack.Bytes2IntList(msg.Value)
	if len(args) != 2 {
		return &pb.LCPROTO{Value: pack.Int2Bytes(int64(0))}
	}

	min := args[0]
	max := args[1]

	if min > max {
		return &pb.LCPROTO{Value: pack.Int2Bytes(int64(0))}
	}

	total := int64(0)

	ldb.ForEach(msg.Key, true, func(key []byte, value []byte) bool {

		i := pack.Bytes2Int(value)

		if i >= min && i <= max {
			total++
		}

		return true
	})

	mutex.Unlock()

	return &pb.LCPROTO{Value: pack.Int2Bytes(total)}
}
