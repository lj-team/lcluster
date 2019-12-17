package engine

import (
	"bytes"
)

type ZRec struct {
	Key   []byte
	Value int64
}

type ZSet []*ZRec

func (s ZSet) Len() int {
	return len(s)
}

func (s ZSet) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ZSet) Less(i, j int) bool {
	return s[i].Value > s[j].Value || (s[i].Value == s[j].Value && bytes.Compare(s[i].Key, s[j].Key) < 0)
}
