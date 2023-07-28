package kademlia

import (
	"crypto/sha1"
	"math/big"
	"time"
)

const (
	k         = 20
	alpha     = 3
	kHashSize = 160 // ID位数/kBucket个数
)

const (
	ExpireInterval    = time.Second
	RepublishInterval = time.Second
	ExpireTime   = 960 * time.Second
	RepublishTime= 600 * time.Second
)

func Hash(s string) *big.Int {
	h := sha1.New()
	h.Write([]byte(s))
	hash := new(big.Int)
	hash.SetBytes(h.Sum(nil))
	return hash
}

func Xor(a, b *big.Int) big.Int {
	var result big.Int
	result.Xor(a, b)
	return result
}

func cpl(a, b *big.Int) int {
	dis := Xor(a, b)
	return dis.BitLen() - 1
}
