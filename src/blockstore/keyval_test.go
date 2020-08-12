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

import (
	"log"
	"os"
	"testing"
)

// This simply tests if a wrong key is rejected/cleaned correctly or not.
func check(t *testing.T, v1 uint64, expect bool) {
	var v2 uint64
	var ok bool
	log.Printf("KeyVal -- TestCleanKey -- Testing with key: 0x%016X (expecting: %t)\n", v1, expect)
	v2, ok = CleanKey(v1)
	if expect == true {
		if !ok || v1 != v2 {
			t.Errorf("Error happened at key=%X , CleanKey(key)=%X\n", v1, v2)
		}
	} else {
		if ok || v1 == v2 {
			t.Errorf("Error happened at key=%X , CleanKey(key)=%X\n", v1, v2)
		}
	}
}

// Checks different keys. Starting from 0, 1, then shifts it by 4 bits until
// reaching the bound. Then goes further and makes sure CleanKey rejects.
func TestCleanKey(t *testing.T) {
	var key uint64

	check(t, 0, true)

	for key = 1; ; key *= 16 {
		if key > maxKey {
			break
		}
		check(t, key, true)
	}
	check(t, maxKey, true)
	check(t, maxKey+1, false)

}

// This simply generates many random keys and makes sure they are not out of
// bound.
func TestRandomKey(t *testing.T) {
	var count = 10000
	log.Printf("KeyVal -- TestRandomKey -- Testing %d random keys.\n", count)
	for i := 0; i < count; i++ {
		if GetRandomKey() > maxKey {
			t.Error("Random key was out of bound.")
		}
	}
}

// This simply tests if a zero block is generated with the correct size.
func TestZeroBlock(t *testing.T) {
	var testBlock *Block
	testBlock = GetZeroBlock()

	if testBlock == nil {
		t.Error("Block was not generated.")
	}
	log.Printf("KeyVal -- TestZeroBlock -- Zero block generated with size = %d", len(testBlock))
	if len(testBlock) != blockSize {
		t.Error("Block was generated with wrong size.")
	}
	for i := 0; i < len(testBlock); i++ {
		if testBlock[i] != 0 {
			t.Error("Zero block contains non-zero byte.")
		}
	}
}

// This simply tests if a random block is generated with the correct size.
func TestRandomBlock(t *testing.T) {
	var testBlock *Block
	testBlock = GetRandomBlock()

	if testBlock == nil {
		t.Error("Block was not generated.")
	}
	log.Printf("KeyVal -- TestRandomBlock -- Random block generated with size = %d", len(testBlock))

	if len(testBlock) != blockSize {
		t.Error("Block was generated with wrong size.")
	}
}

// Tests encodeKeyFlag and decodeKeyFlag functions.
// Generates random Key and Flags, ecnodes and decodes them and
// compares them with originals.
func TestEncodeDecodeKeyFlags(t *testing.T) {
	var myKey, outKey uint64
	var myFlags, outFlags byte
	var encoded uint64
	var testSize = 10000

	log.Printf("KeyVal -- TestEncodeDecodeKeyFlags -- Testing %d random key,flags.", testSize)

	myMap := make(map[uint64]bool)
	for i := 0; i < testSize; i++ {
		// Generating a new random key.
		for {
			myKey = GetRandomKey()
			if _, exist := myMap[myKey]; exist == false {
				break
			}
		}
		myMap[myKey] = true

		// Generate a pseodo random flags byte
		myFlags = byte(len(myMap) * int(myKey))
		encoded = encodeKeyFlags(myKey, myFlags)
		outKey, outFlags = decodeKeyFlags(encoded)

		if outKey != myKey || outFlags != myFlags {
			t.Error("Decoded key/flags doesn't match the original.")
		}
	}

}

// Adding the initializer for random generator for the test functions.
func TestMain(m *testing.M) {
	if myRand == nil {
		myRand = randomGen()
	}
	stat := m.Run()
	os.Exit(stat)
}
