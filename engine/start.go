package engine

import (
	"github.com/lj-team/lcluster/connect"
	"github.com/lj-team/lcluster/server"
)

var repl *connect.Conn

func Start(addr string, replica string) {

	if replica != "" {
		repl = connect.NewConn(replica)
	}

	srv := &server.Server{
		Addr:     addr,
		Callback: handler,
	}

	srv.Start()
}
