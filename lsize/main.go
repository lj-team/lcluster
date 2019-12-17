package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/lj-team/go-generic/log"
	"github.com/lj-team/lcluster/connect"
)

func main() {

	cfg := &log.Config{
		Template: "lsize-%Y%m%d.log",
		Period:   86400,
		Save:     10,
		Level:    "info",
	}

	log.Init(cfg)

	nodes := connect.LoadNodeList("/etc/lcluster/cluster.json")
	proxy := connect.NewProxy(nodes)

	if len(os.Args) != 2 {
		panic("use: lsize <num>")
	}

	n, err := strconv.ParseInt(os.Args[1], 10, 64)

	if err != nil || int(n) >= len(nodes) {
		panic("invalid node number")
	}

	total := proxy.KeyTotal(int(n))

	fmt.Println("key total=", total)
}
