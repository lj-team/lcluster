package connect

import (
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/lj-team/go-generic/encode/pack"
	"github.com/lj-team/go-generic/log"
	"github.com/lj-team/lcluster/codecs"
	"github.com/lj-team/lcluster/pb"

	//"os"
	"sync"
	"time"
)

type Conn struct {
	addr      string
	last_time int64
	last_try  int64
	conn      net.Conn
	encoder   codecs.Encode
	decoder   codecs.Decode
	buffer    []byte
	keybuf    []byte
	pool      *Pool
	unret     int
	sync.Mutex
}

func NewConn(addr string) *Conn {
	n := &Conn{
		addr:      addr,
		last_time: 0,
		last_try:  0,
		encoder:   codecs.Encode{},
		decoder:   codecs.Decode{},
		buffer:    make([]byte, 40960),
		keybuf:    make([]byte, 2560),
		unret:     0,
	}

	return n
}

var oneByte = []byte{1}

func (n *Conn) KeepAlive() bool {

	if time.Now().Unix()-n.last_time > 60 && n.conn != nil {
		log.Trace("close connect to " + n.addr + " by timeout")
		n.conn.Close()
		n.conn = nil
		n.unret = 0
	}

	var err error

	if n.conn == nil && time.Now().Unix()-n.last_try > 5 {
		log.Trace("try connect " + n.addr)
		n.unret = 0
		n.last_try = time.Now().Unix()
		n.last_time = n.last_try
		n.decoder = codecs.Decode{}
		n.conn, err = net.Dial("tcp", n.addr)
		if err != nil {
			log.Trace("connect to " + n.addr + " failed")
			n.conn = nil
		}
	}

	return n.conn != nil
}

func (n *Conn) send(pm *pb.LCPROTO) bool {

	if n.unret >= NOP_AFTER {
		n.Nop()
	}

	data, _ := proto.Marshal(pm)

	msg := n.encoder.Write(data)

	for i := 0; i < 2; i++ {
		if !n.KeepAlive() {
			continue
		}

		wt, err := n.conn.Write(msg)
		if err == nil && wt == len(msg) {
			n.last_time = time.Now().Unix()
			return true
		}

		if wt < len(msg) {
			log.Trace("output buffer full")
			if BUFFER_FULL_KILL {
				<-time.After(time.Second * 2)
				//os.Exit(1)
			}
		}

		n.conn.Close()
		n.conn = nil
	}

	return false
}

func (n *Conn) Read() *pb.LCPROTO {

	n.unret = 0

	if n.conn == nil {
		return nil
	}

	for i := 0; i < 5; i++ {
		num, err := n.conn.Read(n.buffer)
		if err != nil {
			n.conn.Close()
			n.conn = nil
			return nil
		}

		var list [][]byte
		list = n.decoder.Write(n.buffer[:num])
		if len(list) > 0 {
			num = len(list) - 1
			var msg pb.LCPROTO
			if err = proto.Unmarshal(list[num], &msg); err != nil {
				n.conn.Close()
				n.conn = nil
				return nil
			}
			return &msg
		}
	}

	n.conn.Close()
	n.conn = nil
	return nil
}

func (n *Conn) makeKey(key, subkey []byte) []byte {
	size := 1 + len(key)

	n.keybuf[0] = byte(size)
	copy(n.keybuf[1:], key)

	if subkey == nil {
		return n.keybuf[:size]
	}

	copy(n.keybuf[size:], subkey)
	size += len(subkey)

	if size > 512 {
		size = 512
	}

	return n.keybuf[:size]
}

func (n *Conn) Send(command pb.LCPROTO_Code, key, subkey, value []byte) {

	msg := &pb.LCPROTO{
		Code:    command,
		Key:     n.makeKey(key, subkey),
		Value:   value,
		Counter: 0,
	}

	n.send(msg)
}

func (n *Conn) Set(key, subkey []byte, value interface{}, sync bool) {

	msg := &pb.LCPROTO{
		Code:  pb.LCPROTO_C_SET,
		Key:   n.makeKey(key, subkey),
		Value: pack.Encode(value),
		Sync:  sync,
	}

	n.send(msg)

	if sync {
		n.Read()
	}
}

func (n *Conn) BitAnd(key, subkey []byte, value int64, sync bool) int64 {

	msg := &pb.LCPROTO{
		Code:   pb.LCPROTO_C_BITAND,
		Key:    n.makeKey(key, subkey),
		Ivalue: value,
		Sync:   sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()
		return r.GetIvalue()
	}

	return 0
}

func (n *Conn) BitAndNot(key, subkey []byte, value int64, sync bool) int64 {

	msg := &pb.LCPROTO{
		Code:   pb.LCPROTO_C_BITANDNOT,
		Key:    n.makeKey(key, subkey),
		Ivalue: value,
		Sync:   sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()
		return r.GetIvalue()
	}

	return 0
}

func (n *Conn) BitOr(key, subkey []byte, value int64, sync bool) int64 {

	msg := &pb.LCPROTO{
		Code:   pb.LCPROTO_C_BITOR,
		Key:    n.makeKey(key, subkey),
		Ivalue: value,
		Sync:   sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()
		return r.GetIvalue()
	}

	return 0
}

func (n *Conn) BitXor(key, subkey []byte, value int64, sync bool) int64 {

	msg := &pb.LCPROTO{
		Code:   pb.LCPROTO_C_BITXOR,
		Key:    n.makeKey(key, subkey),
		Ivalue: value,
		Sync:   sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()
		return r.GetIvalue()
	}

	return 0
}

func (n *Conn) SetNX(key, subkey []byte, val interface{}, sync bool) bool {
	msg := &pb.LCPROTO{
		Code:  pb.LCPROTO_C_SETNX,
		Key:   n.makeKey(key, subkey),
		Value: pack.Encode(val),
		Sync:  sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()

		if r != nil && r.Value != nil && len(r.Value) > 0 {
			return r.Value[0] != 0
		}

		return false
	}

	return true
}

func (n *Conn) SetIfMore(key, subkey []byte, val int64, sync bool) int64 {

	msg := &pb.LCPROTO{
		Code:   pb.LCPROTO_C_SETIFMORE,
		Key:    n.makeKey(key, subkey),
		Ivalue: val,
		Sync:   sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()
		return r.GetIvalue()
	}

	return 0
}

func (n *Conn) Del(key, subkey []byte, sync bool) bool {

	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_DEL,
		Key:  n.makeKey(key, subkey),
		Sync: sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()
		return r.GetIvalue() != 0
	}

	return true
}

func (n *Conn) Nop() {
	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_NOP,
	}

	n.unret = 0

	n.send(msg)
	n.Read()
}

func (n *Conn) Get(key, subkey []byte) []byte {

	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_GET,
		Key:  n.makeKey(key, subkey),
	}

	n.send(msg)
	r := n.Read()

	if r != nil && r.Value != nil && len(r.Value) > 0 {
		return r.Value
	}

	return nil
}

func (n *Conn) Has(key, subkey []byte) bool {

	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_HAS,
		Key:  n.makeKey(key, subkey),
	}

	n.send(msg)
	r := n.Read()

	return r.GetIvalue() != 0
}

func (n *Conn) GetInt(key, subkey []byte) int64 {

	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_GETINT,
		Key:  n.makeKey(key, subkey),
	}

	n.send(msg)
	r := n.Read()

	if r != nil {
		return r.Ivalue
	}

	return 0
}

func (n *Conn) Inc(key, subkey []byte, val int64, sync bool) int64 {

	msg := &pb.LCPROTO{
		Code:   pb.LCPROTO_C_INC,
		Key:    n.makeKey(key, subkey),
		Ivalue: val,
		Sync:   sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()
		return r.GetIvalue()
	}

	return val
}

func (n *Conn) Dec(key, subkey []byte, val int64, sync bool) int64 {

	msg := &pb.LCPROTO{
		Code:   pb.LCPROTO_C_DEC,
		Key:    n.makeKey(key, subkey),
		Ivalue: val,
		Sync:   sync,
	}

	n.send(msg)

	if sync {
		r := n.Read()
		return r.GetIvalue()
	}

	return val
}

func (n *Conn) Do(command pb.LCPROTO_Code, key, subkey, value []byte) *pb.LCPROTO {
	n.Send(command, key, subkey, value)
	return n.Read()
}

func (n *Conn) Release() {
	if n.pool != nil {
		n.pool.Put(n)
	}
}

func (n *Conn) Log(key, value []byte, counter int) {

	if n == nil {
		return
	}

	msg := &pb.LCPROTO{
		Code:    pb.LCPROTO_LOG,
		Key:     key,
		Value:   value,
		Counter: int32(counter),
	}

	n.send(msg)
}

func (n *Conn) SeqAdd(seq []byte, value interface{}, sync bool) {
	n.Set(seq, pack.Encode(time.Now().UnixNano(), value), oneByte, sync)
}

func (n *Conn) HKill(key []byte, sync bool) {

	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_HKILL,
		Key:  n.makeKey(key, nil),
		Sync: sync,
	}

	n.send(msg)

	if sync {
		n.Read()
	}
}

func (n *Conn) SeqKill(seq []byte, sync bool) {
	n.HKill(seq, sync)
}

func (n *Conn) HKeysAll(hash []byte) [][]byte {
	limit := int64(100)
	offset := int64(0)
	result := make([][]byte, 0, 100)

	for {
		res := n.HKeys(hash, limit, offset)
		if res == nil || len(res) == 0 {
			break
		}

		for _, buf := range res {
			result = append(result, buf)
		}

		if len(res) < int(limit) {
			break
		}

		offset += limit
	}

	return result
}

func (n *Conn) KeyTotal() int64 {
	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_KEYTOTAL,
	}

	n.send(msg)
	r := n.Read()

	return r.GetIvalue()
}

func (n *Conn) HSize(key []byte) int64 {
	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_HSIZE,
		Key:  n.makeKey(key, nil),
	}

	n.send(msg)
	r := n.Read()

	return r.GetIvalue()
}

func (n *Conn) SeqSize(seq []byte) int64 {
	return n.HSize(seq)
}

func (n *Conn) HKeys(key []byte, limit, offset int64) [][]byte {
	msg := &pb.LCPROTO{
		Code:  pb.LCPROTO_C_HKEYS,
		Key:   n.makeKey(key, nil),
		Value: pack.Encode(limit, offset),
	}

	n.send(msg)
	r := n.Read()

	if r != nil && r.List != nil && len(r.List) > 0 {
		return r.List
	}

	return nil
}

func (n *Conn) HAll(hash []byte) []Pair {
	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_HALL,
		Key:  n.makeKey(hash, nil),
	}

	n.send(msg)
	r := n.Read()

	if r != nil && r.List != nil && len(r.List) > 0 {
		size := len(r.List) / 2
		result := make([]Pair, size)
		for i := 0; i < size; i++ {
			result[i].Key = r.List[i*2]
			result[i].Value = r.List[i*2+1]
		}
		return result
	}

	return nil
}

func (n *Conn) HKeysRand(key []byte, limit int64) [][]byte {
	msg := &pb.LCPROTO{
		Code:  pb.LCPROTO_C_HKEYSRAND,
		Key:   n.makeKey(key, nil),
		Value: pack.Encode(limit),
	}

	n.send(msg)
	r := n.Read()

	if r != nil && r.List != nil && len(r.List) > 0 {
		return r.List
	}

	return nil
}

func (n *Conn) SeqRange(seq []byte, limit, offset int64) [][]byte {
	list := n.HKeys(seq, limit, offset)

	if list != nil && len(list) > 0 {
		var res [][]byte
		for _, v := range list {
			if len(v) > 8 {
				if res == nil {
					res = make([][]byte, 0, len(list))
				}
				res = append(res, v[8:])
			}
		}
		return res
	}

	return nil
}

func (n *Conn) ZKill(key []byte, sync bool) {

	msg := &pb.LCPROTO{
		Code: pb.LCPROTO_C_ZKILL,
		Key:  n.makeKey(key, nil),
		Sync: sync,
	}

	n.send(msg)

	if sync {
		n.Read()
	}
}

func (n *Conn) ZRange(key []byte, limit, offset, min, max int64) []ZRec {

	msg := &pb.LCPROTO{
		Code:  pb.LCPROTO_C_ZRANGE,
		Key:   n.makeKey(key, nil),
		Value: pack.Encode(limit, offset, min, max),
	}

	n.send(msg)
	r := n.Read()

	if r != nil && r.List != nil && len(r.List)%2 == 0 {
		res := make([]ZRec, len(r.List)/2)

		for i := 0; i < len(r.List); i += 2 {
			res[i/2].Key = r.List[i]
			res[i/2].Value = pack.Bytes2Int(r.List[i+1])
		}

		return res
	}

	return nil
}

func (n *Conn) ZRangeSize(key []byte, min, max int64) int64 {

	msg := &pb.LCPROTO{
		Code:  pb.LCPROTO_C_ZRANGESIZE,
		Key:   n.makeKey(key, nil),
		Value: pack.Encode(min, max),
	}

	n.send(msg)
	r := n.Read()

	return r.GetIvalue()
}
