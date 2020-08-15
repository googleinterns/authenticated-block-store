//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package blockstore

// This file implements key/value primitives.

import (
	"encoding/binary"
	"math/rand"
	"sort"
	"time"
)

// The key is 7 least significant bytes.
// The most significant byte is reserved for flags.
const flagsMask = uint64(0xFF) << 56
const keyMask = ^(uint64(0xFF) << 56)
const maxKey = ^(uint64(0xFF) << 56)

// The random generator which will be seeded once.
var myRand *rand.Rand

// This block describes flag bits.
const (
	// Defines if an entry is modified/dirty
	// This is used in the tableManager, so the entry is not evicted.
	flagDirty = uint8(1) << iota
	// Defines if an entry is marked for removal.
	flagRemove
	// Other bits are reserved.
)

// Size of a Block can be arbitrary. It is Intended to be 4096 to fit in a 4KB page.
const blockSize = 4096

type Block [blockSize]byte

// A struct that holds information for a (key, data block) pair.
// The flags byte would indicate if the keyVal is marked for removal.
type keyVal struct {
	key   uint64
	block *Block
	flags byte
}

// A helper function to sort slice of keys, which of are of type uint64.
func uint64Sort(src []uint64) {
	var tmpSlice []int
	tmpSlice = make([]int, len(src), len(src))
	for i, k := range src {
		tmpSlice[i] = int(k)
	}
	sort.Ints(tmpSlice)
	for i, k := range tmpSlice {
		src[i] = uint64(k)
	}
}

// Function that makes sure the most significant byte of a key is zeroed.
func CleanKey(keyIn uint64) (uint64, bool) {
	return keyIn & keyMask, (keyIn >> 56) <= 0
}

// Encodes a (key, flag) into a uint64.
func encodeKeyFlags(key uint64, flags byte) uint64 {
	var val uint64
	val = ((uint64(flags) << 56) & flagsMask) | (key & keyMask)
	return val
}

// Decomposes a uint64 to a (key, flag).
func decodeKeyFlags(val uint64) (uint64, byte) {
	var key uint64 = val & keyMask
	var flags byte = byte((val & flagsMask) >> 56)
	return key, flags
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
	for index, _ := range newBlock {
		newBlock[index] = byte(myRand.Intn(256))
	}
	return newBlock
}

// Helper function that generates a random key.
func GetRandomKey() uint64 {
	return uint64(myRand.Intn(int(maxKey)))
}

// Creates a random generator and seeds it.
func randomGen() *rand.Rand {
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)
	return random
}

// Creates a byte slice from a keyVal, containing the block|key|flags.
// This is used for hashing for verifying integrity.
func packBlockKeyFlags(kv *keyVal) []byte {
	dst := make([]byte, blockSize+8+1)
	copy(dst[:], kv.block[:])
	binary.LittleEndian.PutUint64(dst[blockSize:], kv.key)
	dst[len(dst)-1] = kv.flags
	return dst
}
