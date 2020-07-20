package blockstore

// This file implements block primitives.

import (
	"math/rand"
	"time"
)

// Size of a Block can be arbitrary. It is Intended to be 4096 to fit in a 4KB page.
// TODO small value is picked for testing purposes. Should be configured later.
const blockSize = 16

type Block [blockSize]byte

// Helper function that generates a random block of data.
func GetRandomBlock() *Block {
	var newBlock *Block = new(Block)
	myRand := randomGen()
	for index, _ := range newBlock {
		newBlock[index] = byte(myRand.Intn(256))
	}
	return newBlock
}

func randomGen() *rand.Rand {
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)
	return random
}
