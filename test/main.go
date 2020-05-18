package main

import (
	"flag"
	"fmt"

	"github.com/lj-team/go-generic/encode/pack"
	"github.com/lj-team/go-generic/log"
	"github.com/lj-team/lcluster/connect"
)

var con connect.Cluster

func main() {

	conf := ""

	flag.StringVar(&conf, "c", "lcluster.json", "config file name")

	flag.Parse()

	cfg := LoadConfig(conf)

	log.Init(&cfg.Log)

	con = connect.NewProxy(cfg.Nodes)

	testNop()
	testBasic()
	testHash()
	testBits()
	testSetIfMore()
	testSeq()
	testZ()
}

func testNop() {
	key := []byte("nop")

	con.Del(key, nil, false)
	res := con.Get(key, nil)
	if res != nil && len(res) != 0 {
		panic("del not worl")
	}

	for i := 0; i < 11534; i++ {
		con.Inc(key, nil, 1, false)
	}

	if con.GetInt(key, nil) != 11534 {
		panic("testNop failed")
	}

	fmt.Println("NOP - OK")
}

func testSeq() {

	key := []byte("seq")

	con.SeqKill(key, true)

	if con.SeqSize(key) != 0 {
		panic("SeqKill not work")
	}

	con.SeqAdd(key, []byte("!"), false)
	con.SeqAdd(key, []byte("!"), false)
	con.SeqAdd(key, []byte("!!!"), false)
	con.SeqAdd(key, []byte("!"), false)
	con.SeqAdd(key, []byte("!"), false)

	if con.SeqSize(key) != 5 {
		panic("SeqSize not work")
	}

	for i, v := range con.SeqRange(key, 10, 0) {
		if i == 2 {
			if string(v) != "!!!" {
				panic("SeqRange not work")
			}
		} else {
			if string(v) != "!" {
				panic("SeqRange not work")
			}
		}
	}

	con.SeqKill(key, true)
	if con.SeqSize(key) != 0 {
		panic("SeqKill not work")
	}

	fmt.Println("Seq - OK")
}

func testBasic() {

	key := []byte("test1")
	con.Del(key, nil, false)

	if con.GetInt(key, nil) != 0 {
		panic("Del not work")
	}

	con.Set(key, nil, int64(1), false)
	if con.GetInt(key, nil) != 1 {
		panic("Set not work")
	}

	if con.SetNX(key, nil, int64(2), true) {
		panic("SetNX failed")
	}

	con.SetNX(key, nil, int64(3), false)

	if !con.Has(key, nil) {
		panic("Has not work")
	}

	con.Del(key, nil, false)
	if con.Has(key, nil) {
		panic("Del not work")
	}

	con.Inc(key, nil, 1, false)
	con.Inc(key, nil, 2, false)
	con.Inc(key, nil, 3, false)
	con.Inc(key, nil, 4, false)

	if con.GetInt(key, nil) != 10 {
		panic("Inc not work")
	}

	con.Dec(key, nil, 2, false)
	con.Dec(key, nil, 3, false)

	if con.GetInt(key, nil) != 5 {
		panic("Dec not work")
	}

	fmt.Println("Basic - OK")
}

func testHash() {

	hash := []byte("hash")

	con.HKill(hash, true)
	if con.HSize(hash) != 0 {
		panic("HKill or HSize not working")
	}

	con.Set(hash, []byte("1"), int64(11), false)
	con.Set(hash, []byte("2"), int64(22), false)
	con.Set(hash, []byte("3"), int64(33), false)
	con.Set(hash, []byte("4"), int64(44), true)

	if con.HSize(hash) != 4 {
		panic("HSize not work")
	}

	list := con.HAll(hash)
	if len(list) != 4 {
		panic("HAll not work")
	}

	if list[0].Key[0] != '1' || list[1].Key[0] != '2' || pack.Bytes2Int(list[2].Value) != 33 || pack.Bytes2Int(list[3].Value) != 44 {
		panic("HAll not work. bad return values")
	}

	fmt.Println("Hash - OK")
}

func testZ() {

	key := []byte("zset")

	con.ZKill(key, true)

	if con.ZRangeSize(key, 0, 10000) != 0 {
		panic("zkill not work")
	}

	con.Set(key, []byte("1"), int64(1), false)
	con.Set(key, []byte("2"), int64(2), false)
	con.Set(key, []byte("3"), int64(3), false)
	con.Set(key, []byte("4"), int64(10), true)

	if con.ZRangeSize(key, 2, 14) != 3 {
		panic("zrange return invalid value")
	}

	data := con.ZRange(key, 3, 0, 0, 5)
	if len(data) != 3 {
		panic("bad return size")
	}

	if string(data[0].Key) != "3" || string(data[1].Key) != "2" || string(data[2].Key) != "1" {
		panic("bad return list")
	}

	fmt.Println("ZSet - OK")
}

func testBits() {

	key := []byte("bits")
	var subkey []byte

	for i := 0; i < 2; i++ {

		if i == 1 {
			key = []byte("bits2")
			subkey = []byte("sub")
		}

		con.BitAnd(key, subkey, 0, false)
		if con.GetInt(key, subkey) != 0 {
			panic("BitAnd 0 failed")
		}

		con.BitOr(key, subkey, 0xf, false)
		if con.GetInt(key, subkey) != 0xf {
			panic("BitOr 0xf failed")
		}

		con.BitAnd(key, subkey, 0xA, false)
		if con.GetInt(key, subkey) != 0xA {
			panic("BitAnd 0xA failed")
		}

		con.BitXor(key, subkey, 0xF, false)
		if con.GetInt(key, subkey) != 0x5 {
			panic("BitXor 0xF failed")
		}

		if con.BitAndNot(key, subkey, 0x1, true) != 0x4 {
			panic("BitAndNot 0x1 failed")
		}

	}

	fmt.Println("bit operation - OK")
}

func testSetIfMore() {

	key := []byte("setifmore")
	var subkey []byte

	con.Del(key, nil, false)

	con.SetIfMore(key, subkey, 1, false)
	con.SetIfMore(key, subkey, 2, false)

	if val := con.SetIfMore(key, subkey, 3, true); val != 3 {
		panic(fmt.Sprintf("setif more not work. val=%d", val))
	}

	subkey = []byte("subkey")

	con.Del(key, subkey, false)

	con.SetIfMore(key, subkey, 12, false)
	con.SetIfMore(key, subkey, 22, false)

	if val := con.SetIfMore(key, subkey, 3, true); val != 22 {
		panic(fmt.Sprintf("setif more not work. val=%d", val))
	}

	fmt.Println("SetIfMore - OK")
}
