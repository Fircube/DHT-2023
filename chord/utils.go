package chord

import (
	"time"

	"crypto/sha1"
	"math/big"
)

const kSuccessorListSize = 10
const kFingerTableSize = 160 // sha1算法输出160bit

var (
	emitInterval = 10 * time.Second
	updateInterval = 100 * time.Millisecond
)

func Hash(s string) *big.Int {
	h := sha1.New()
	h.Write([]byte(s))
	hash := new(big.Int)
	hash.SetBytes(h.Sum(nil))
	return hash
}

func HashFinger(base *big.Int, fin int) *big.Int {
	fi := new(big.Int)
	fi = fi.SetBit(fi, fin, 1)
	sum := new(big.Int).Add(base, fi)
	mode := new(big.Int)
	mode = mode.SetBit(mode, 160, 1)
	return new(big.Int).Mod(sum, mode)
}

func Contain(id, start, end *big.Int) bool {
	if start.Cmp(end) < 0 {
		return start.Cmp(id) < 0 && end.Cmp(id) > 0
	} else {
		return start.Cmp(id) < 0 || end.Cmp(id) > 0
	}
}
