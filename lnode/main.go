package main

import (
	"flag"

	"github.com/lj-team/go-generic/daemon"
	"github.com/lj-team/go-generic/db/ldb"
	"github.com/lj-team/go-generic/log"
	"github.com/lj-team/lcluster/engine"
)

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

	ldb.Init(&cfg.Database)

	engine.Start(cfg.Server, cfg.Replica)
}
