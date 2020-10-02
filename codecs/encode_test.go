package codecs

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {

	tF := func(msg string) {

		en := Encode{}
		dec := Decode{}

		res := en.Write([]byte(msg))

		list := dec.Write(res)

		if len(list) != 1 {
			t.Fatal("Encode/Decode failed")
		}

		if string(list[0]) != msg {
			t.Fatal("invalid decoded value")
		}
	}

	tF("hello")
	tF("Привет")
}
