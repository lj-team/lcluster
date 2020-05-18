package connect

import (
	"github.com/lj-team/lcluster/hash/consistent"
	"github.com/lj-team/lcluster/pb"
)

type Proxy struct {
	conns  []*Conn
	hash   *consistent.Hash
	keybuf []byte
}

func NewProxy(addrs []string) Cluster {
	p := &Proxy{
		hash:   consistent.New(len(addrs)),
		conns:  make([]*Conn, len(addrs)),
		keybuf: make([]byte, 256),
	}

	for i := range p.conns {
		p.conns[i] = NewConn(addrs[i])
	}

	return p
}

func (p *Proxy) ProtoSend(msg *pb.LCPROTO) {
	size := int(msg.Key[0])
	n := p.hash.Get(msg.Key[1:size])
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	con.send(msg)
}

func (p *Proxy) ProtoDo(msg *pb.LCPROTO) *pb.LCPROTO {
	size := int(msg.Key[0])
	n := p.hash.Get(msg.Key[1:size])
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	con.send(msg)
	return con.Read()
}

func (p *Proxy) Set(key, subkey []byte, value interface{}, sync bool) {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	con.Set(key, subkey, value, sync)
}

func (p *Proxy) SetIfMore(key, subkey []byte, value int64, sync bool) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.SetIfMore(key, subkey, value, sync)
}

func (p *Proxy) BitAnd(key, subkey []byte, value int64, sync bool) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.BitAnd(key, subkey, value, sync)
}

func (p *Proxy) BitAndNot(key, subkey []byte, value int64, sync bool) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.BitAndNot(key, subkey, value, sync)
}

func (p *Proxy) BitOr(key, subkey []byte, value int64, sync bool) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.BitOr(key, subkey, value, sync)
}

func (p *Proxy) BitXor(key, subkey []byte, value int64, sync bool) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.BitXor(key, subkey, value, sync)
}

func (p *Proxy) SetNX(key, subkey []byte, value interface{}, sync bool) bool {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.SetNX(key, subkey, value, sync)
}

func (p *Proxy) Get(key, subkey []byte) []byte {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.Get(key, subkey)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n = p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.Get(key, subkey)
		con.Unlock()
		return v
	}

	return nil
}

func (p *Proxy) GetInt(key, subkey []byte) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.GetInt(key, subkey)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n := p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.GetInt(key, subkey)
		con.Unlock()
		return v
	}

	return 0
}

func (p *Proxy) Has(key, subkey []byte) bool {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.Has(key, subkey)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n := p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.Has(key, subkey)
		con.Unlock()
		return v
	}

	return false
}

func (p *Proxy) Del(key, subkey []byte, sync bool) bool {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.Del(key, subkey, sync)
}

func (p *Proxy) Inc(key, subkey []byte, val int64, sync bool) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.Inc(key, subkey, val, sync)
}

func (p *Proxy) Dec(key, subkey []byte, val int64, sync bool) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.Dec(key, subkey, val, sync)
}

func (p *Proxy) SeqAdd(seq []byte, value interface{}, sync bool) {
	n := p.hash.Get(seq)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	con.SeqAdd(seq, value, sync)
}

func (p *Proxy) HKill(key []byte, sync bool) {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	con.HKill(key, sync)
}

func (p *Proxy) SeqKill(seq []byte, sync bool) {
	n := p.hash.Get(seq)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	con.SeqKill(seq, sync)
}

func (p *Proxy) HKeysAll(key []byte) [][]byte {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.HKeysAll(key)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n := p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.HKeysAll(key)
		con.Unlock()
		return v
	}

	return [][]byte{}
}

func (p *Proxy) HAll(key []byte) []Pair {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.HAll(key)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n := p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.HAll(key)
		con.Unlock()
		return v
	}

	return []Pair{}
}

func (p *Proxy) HKeys(key []byte, limit, offset int64) [][]byte {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.HKeys(key, limit, offset)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n = p.hash.Next(n)
		con := p.conns[n]
		con.Lock()
		v := con.HKeys(key, limit, offset)
		con.Unlock()
		return v
	}

	return [][]byte{}
}

func (p *Proxy) HKeysRand(key []byte, limit int64) [][]byte {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.HKeysRand(key, limit)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n = p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.HKeysRand(key, limit)
		con.Unlock()
		return v
	}

	return [][]byte{}
}

func (p *Proxy) SeqRange(seq []byte, limit, offset int64) [][]byte {
	n := p.hash.Get(seq)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.SeqRange(seq, limit, offset)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n = p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.SeqRange(seq, limit, offset)
		con.Unlock()
		return v
	}

	return [][]byte{}
}

func (p *Proxy) HSize(key []byte) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	if con.KeepAlive() {
		v := con.HSize(key)
		con.Unlock()
		return v
	}
	con.Unlock()

	if QUORUM {
		n = p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.HSize(key)
		con.Unlock()
		return v
	}

	return 0
}

func (p *Proxy) KeyTotal(n int) int64 {
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	return con.KeyTotal()
}

func (p *Proxy) SeqSize(seq []byte) int64 {
	n := p.hash.Get(seq)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.SeqSize(seq)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n = p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.SeqSize(seq)
		con.Unlock()
		return v
	}

	return 0
}

func (p *Proxy) ZKill(key []byte, sync bool) {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()
	defer con.Unlock()
	con.ZKill(key, sync)
}

func (p *Proxy) ZRange(key []byte, limit, offset, min, max int64) []ZRec {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.ZRange(key, limit, offset, min, max)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n = p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.ZRange(key, limit, offset, min, max)
		con.Unlock()
		return v
	}

	return []ZRec{}
}

func (p *Proxy) ZRangeSize(key []byte, min, max int64) int64 {
	n := p.hash.Get(key)
	con := p.conns[n]
	con.Lock()

	if con.KeepAlive() {
		v := con.ZRangeSize(key, min, max)
		con.Unlock()
		return v
	}

	con.Unlock()

	if QUORUM {
		n = p.hash.Next(n)
		con = p.conns[n]
		con.Lock()
		v := con.ZRangeSize(key, min, max)
		con.Unlock()
		return v
	}

	return 0
}

func (p *Proxy) Status() bool {

	for _, c := range p.conns {
		c.Lock()
		c.Nop()
		v := c.KeepAlive()
		c.Unlock()
		if !v {
			return false
		}
	}

	return true
}
