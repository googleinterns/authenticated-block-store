package blockstore

import (
	"log"
	"testing"
)

// This simply tests if a random block is generated with the correct size.
func TestRandomBlock(t *testing.T) {
	var testBlock *Block
	testBlock = GetRandomBlock()

	if testBlock == nil {
		t.Error("Block was not generated.")
	}
	log.Printf("Block -- TestRandomBlock -- Random block generated with size = %d", len(testBlock))

	if len(testBlock) != blockSize {
		t.Error("Block was generated with wrong size.")
	}
}
