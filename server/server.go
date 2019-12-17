package server

import (
	"fmt"
	"net"
	"time"

	"github.com/lj-team/go-generic/log"
	"github.com/lj-team/lcluster/codecs"
)

type CALLBACK func([]byte) ([]byte, error)

type Server struct {
	Addr     string
	Callback CALLBACK
}

var nextId int64 = 0

func (s *Server) Start() {

	ln, err := net.Listen("tcp", s.Addr)

	if err != nil {
		panic("bind port error")
	}

	log.Info("server start " + s.Addr)

	for {

		conn, err := ln.Accept()

		if err != nil {
			continue
		}

		nextId++

		go s.connet_handler(conn, nextId)
	}
}

func (s *Server) connet_handler(conn net.Conn, id int64) {
	defer conn.Close()

	log.Debug(fmt.Sprintf("new conection #%d", id))

	buffer := make([]byte, 4098)

	encoder := codecs.Encode{}
	decoder := codecs.Decode{}

	for {
		conn.SetReadDeadline(time.Now().Add(time.Minute))
		n, err := conn.Read(buffer)
		if err != nil || n < 1 {
			log.Debug(fmt.Sprintf("connection #%d broken", id))
			break
		}

		list := decoder.Write(buffer[:n])

		for _, rec := range list {

			res, err1 := s.Callback(rec)
			if err1 != nil {
				log.Trace(err1.Error())
				log.Debug(fmt.Sprintf("connection #%d broken", id))
				return
			}

			if res != nil && len(res) > 0 {
				n, err = conn.Write(encoder.Write(res))
				if err != nil || n != len(res)+4 {
					log.Trace(err.Error())
					log.Debug(fmt.Sprintf("connection #%d broken", id))
					return
				}
			}
		}
	}

	log.Debug(fmt.Sprintf("connection #%d closed", id))
}
