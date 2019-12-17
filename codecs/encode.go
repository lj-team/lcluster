package codecs

import (
	"bytes"

	"github.com/lj-team/go-generic/encode/pack"
)

type Encode struct {
}

func (e *Encode) Write(data []byte) []byte {

	size := int32(len(data))
	return bytes.Join([][]byte{pack.Encode(size), data}, nil)
}
