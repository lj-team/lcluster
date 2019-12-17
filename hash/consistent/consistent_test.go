package consistent

import (
	"strconv"
	"testing"
)

func TestConsistent(t *testing.T) {
	hash := New(10)

	wait := []int{9, 5, 1, 4, 9, 5, 1, 4, 9, 5}

	for i := int64(0); i < 10; i++ {
		key := []byte(strconv.FormatInt(i, 10))
		if hash.Get(key) != wait[int(i)] {
			t.Fatal("invalid function value")
		}
	}
}
