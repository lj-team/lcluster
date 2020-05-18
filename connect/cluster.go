package connect

type Cluster interface {
	Set(key, subkey []byte, value interface{}, sync bool)
	SetIfMore(key, subkey []byte, value int64, sync bool) int64
	BitAnd(key, subkey []byte, value int64, sync bool) int64
	BitAndNot(key, subkey []byte, value int64, sync bool) int64
	BitOr(key, subkey []byte, value int64, sync bool) int64
	BitXor(key, subkey []byte, value int64, sync bool) int64
	SetNX(key, subkey []byte, value interface{}, sync bool) bool
	Get(key, subkey []byte) []byte
	GetInt(key, subkey []byte) int64
	Has(key, subkey []byte) bool
	Del(key, subkey []byte, sync bool) bool
	Inc(key, subkey []byte, val int64, sync bool) int64
	Dec(key, subkey []byte, val int64, sync bool) int64
	SeqAdd(seq []byte, value interface{}, sync bool)
	HKill(key []byte, sync bool)
	SeqKill(seq []byte, sync bool)
	HKeysAll(key []byte) [][]byte
	HAll(key []byte) []Pair
	HKeys(key []byte, limit, offset int64) [][]byte
	HKeysRand(key []byte, limit int64) [][]byte
	SeqRange(seq []byte, limit, offset int64) [][]byte
	HSize(key []byte) int64
	KeyTotal(n int) int64
	SeqSize(seq []byte) int64
	ZKill(key []byte, sync bool)
	ZRange(key []byte, limit, offset, min, max int64) []ZRec
	ZRangeSize(key []byte, min, max int64) int64
	Status() bool
}
