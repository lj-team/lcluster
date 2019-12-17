package connect

import (
	"sync"
)

type Pool struct {
	Addr     string
	size     int
	connects []*Conn
	queue    chan int
	sync.Mutex
}

func NewPool(addr string, limit int) *Pool {
	p := &Pool{
		Addr:     addr,
		size:     limit,
		queue:    make(chan int, limit),
		connects: make([]*Conn, limit),
	}

	for i := 0; i < limit; i++ {
		p.queue <- 1
		p.connects[i] = NewConn(addr)
		p.connects[i].pool = p
	}

	return p
}

func (p *Pool) Get() (c *Conn) {
	<-p.queue

	p.Lock()
	defer p.Unlock()

	c = p.connects[p.size-1]
	p.size--

	return c
}

func (p *Pool) Put(c *Conn) {
	p.Lock()
	defer p.Unlock()

	c.pool = p
	p.connects[p.size] = c
	p.size++

	p.queue <- 1
}
