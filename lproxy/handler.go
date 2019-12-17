package main

import (
	"github.com/golang/protobuf/proto"
	"github.com/lj-team/lcluster/pb"
)

var send_map map[pb.LCPROTO_Code]bool = map[pb.LCPROTO_Code]bool{
	pb.LCPROTO_DEC: true,
	pb.LCPROTO_DEL: true,
	pb.LCPROTO_INC: true,
	pb.LCPROTO_SET: true,
}

func Handler(req []byte) ([]byte, error) {

	var msg pb.LCPROTO

	err := proto.Unmarshal(req, &msg)
	if err != nil {
		return nil, err
	}

	_, ok := send_map[msg.Code]
	if ok {
		PROXY.ProtoSend(&msg)
		return nil, nil
	}

	res := PROXY.ProtoDo(&msg)

	if res == nil {
		res = &pb.LCPROTO{Key: msg.Key}
	}

	rbuf, _ := proto.Marshal(res)

	return rbuf, nil
}
