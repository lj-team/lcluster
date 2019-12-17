package codecs

import (
	"bytes"

	"github.com/lj-team/go-generic/encode/pack"
)

type Decode struct {
	data []byte
}

func (d *Decode) Write(data []byte) [][]byte {

	var list []byte

	if d.data == nil {
		list = bytes.Join([][]byte{[]byte{}, data}, nil)
	} else {
		list = bytes.Join([][]byte{d.data, data}, nil)
	}

	res := [][]byte{}

	size := int32(0)

	for len(list) > 4 {
		if pack.Decode(list, &size) != nil {
			break
		}
		size = size + 4
		if len(list) > int(size) {
			res = append(res, list[4:size])
			list = list[size:]
		} else if len(list) == int(size) {
			res = append(res, list[4:])
			list = []byte{}
		} else {
			break
		}
	}

	if len(list) > 0 {
		d.data = list
	} else {
		d.data = []byte{}
	}

	return res
}
