package main

import (
	"flag"

	"github.com/lj-team/go-generic/daemon"
	"github.com/lj-team/go-generic/log"
	"github.com/lj-team/lcluster/connect"
	"github.com/lj-team/lcluster/server"
)

var PROXY *connect.Proxy

func main() {

	conf := ""
	is_daemon := false

	flag.StringVar(&conf, "c", "lnode.json", "config file name")
	flag.BoolVar(&is_daemon, "d", false, "run as daemon")

	flag.Parse()

	cfg := LoadConfig(conf)

	if is_daemon {
		daemon.Run(&cfg.Daemon)
	}

	log.Init(&cfg.Log)

	if is_daemon {
		log.Info("start application as daemon")
	} else {
		log.Info("start application")
	}

	PROXY = connect.NewProxy(cfg.Nodes)

	srv := &server.Server{
		Addr:     cfg.Server,
		Callback: Handler,
	}

	srv.Start()
}
