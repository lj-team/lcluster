package engine

import (
	"testing"

	"github.com/lj-team/go-generic/db/ldb"
)

func TestEngine(t *testing.T) {
	ldb.Open("test=1 default=1")

	tBB := func(val bool) {

		res := bool2Bytes(val)

		if len(res) != 1 {
			t.Fatal("invalid")
		}

		if val && res[0] != 1 || !val && res[0] != 0 {
			t.Fatal("invalid")
		}

	}

	tBB(true)
	tBB(false)
}
