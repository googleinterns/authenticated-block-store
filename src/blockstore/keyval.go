package blockstore

// This file implements key/value primitives.

import (
	"math/rand"
	"time"
)

// The key is 7 least significcant bytes.
// The highmost byte is reserved for flags.
const flagMask = uint64(0xFF) << 56
const keyMask = ^(uint64(0xFF) << 56)
const maxKey = ^(uint64(0xFF) << 56)

// Size of a Block can be arbitrary. It is Intended to be 4096 to fit in a 4KB page.
const blockSize = 4096

type Block [blockSize]byte

// Making sure the highest byte of a key is zeroed.
func CleanKey(keyIn uint64) (uint64, bool) {
	return keyIn & keyMask, (keyIn >> 56) <= 0
}

// Helper function that generates a block of zero.
func GetZeroBlock() *Block {
	var newBlock *Block = new(Block)
	for index, _ := range newBlock {
		newBlock[index] = byte(0)
	}
	return newBlock
}

// Helper function that generates a block of random data.
func GetRandomBlock() *Block {
	var newBlock *Block = new(Block)
	myRand := randomGen()
	for index, _ := range newBlock {
		newBlock[index] = byte(myRand.Intn(256))
	}
	return newBlock
}

// Helper function that generates a random key.
func GetRandomKey() uint64 {
	myRand := randomGen()
	return uint64(myRand.Intn(int(maxKey)))
}

func randomGen() *rand.Rand {
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)
	return random
}
