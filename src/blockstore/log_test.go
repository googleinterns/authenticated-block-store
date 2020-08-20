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
	"testing"
)

// Directory for test log files.
const dirTest = "dir_test"

// Tests if the encryption and then decryption of a block gets the original.
func TestEncryptDecrypt(t *testing.T) {
	var b1, b2, b3 *Block
	var err error

	log.Println("Log -- TestEncryptDecrypt -- Testing with a random block.")

	b1 = GetRandomBlock()
	b2 = GetZeroBlock()
	b3 = GetZeroBlock()

	if b1 == nil || b2 == nil || b3 == nil {
		t.Fatal("Could not allocate block")
	}

	err = encryptData(b2[:], b1[:])
	if err != nil {
		t.Fatal("Failure in encryption.")
	}

	err = decryptData(b3[:], b2[:])
	if err != nil {
		t.Fatal("Failure in decryption.")
	}

	if !compareByteSlice(b1[:], b3[:]) {
		t.Fatal("Decrypted block does not match to original.")
	}

}

// A simple function that tests hash wrapper.
func TestHash(t *testing.T) {
	s := []byte("TEST")
	ref := []byte{152, 72, 22, 253, 50, 150, 34, 135, 110, 20, 144, 118, 52, 38, 78, 111, 51, 46, 159, 179}

	var d []byte

	d = make([]byte, hashLength, hashLength)

	hashData(d, s)

	log.Println("Log -- TestHash -- Hash result of ", s, " is ", d)
	if !compareByteSlice(d[:], ref[:]) {
		t.Fatal("Hash does not match the reference.")
	}
}

// Tests the generation of the index table from keyVals, and decomposing them back.
func TestIndex(t *testing.T) {
	var inKV, outKV []*keyVal
	var converted []byte
	var kv *keyVal
	var myKey uint64
	var sortedKeys []uint64
	var myFlags byte
	var err error
	var testSize = 1000

	log.Printf("Log -- TestIndex -- Testing %d random key,flags.", testSize)

	myMap := make(map[uint64]bool, testSize)
	sortedKeys = make([]uint64, testSize)
	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if _, exist := myMap[myKey]; exist == false {
				break
			}
		}
		myMap[myKey] = true
		sortedKeys[i] = myKey
	}
	uint64Sort(sortedKeys)

	inKV = make([]*keyVal, testSize)
	for i := 0; i < testSize; i++ {
		// Generate a pseudo random flags byte
		myKey = sortedKeys[i]
		myFlags = byte(i * int(myKey))
		myFlags = myFlags & ^flagDirty & ^flagRemove
		kv = new(keyVal)
		kv.key = myKey
		kv.flags = myFlags
		kv.block = GetRandomBlock()
		inKV[i] = kv
	}

	converted, err = generateIndex(inKV)
	if err != nil {
		t.Fatal("Could not generate Index.")
	}

	outKV, err = decomposeIndex(converted)
	if err != nil || outKV == nil || len(outKV) != len(inKV) {
		t.Fatal("Decomposed list size error.")
	}
	for i, _ := range inKV {
		if (inKV[i].key != outKV[i].key) || (inKV[i].flags != outKV[i].flags) {
			t.Fatal("Decomposed list does not match original list.")
		}
	}

}

// Writes keyVals to the log and then reads them.
// Then tries reading nonexistent keys.
// TODO first make it work for the headLog, then the chain.
func TestLogWriteRead(t *testing.T) {
	var lm *logManager
	var inKV []*keyVal
	var kv *keyVal
	var myKey uint64
	var sortedKeys []uint64
	var myFlags byte
	var err error
	var testSize = 100

	log.Printf("Log -- TestLogWriteRead -- Testing %d random key,flags,blocks.", testSize)

	myMap := make(map[uint64]bool, testSize)
	sortedKeys = make([]uint64, testSize)
	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if _, exist := myMap[myKey]; exist == false {
				break
			}
		}
		myMap[myKey] = true
		sortedKeys[i] = myKey
	}
	uint64Sort(sortedKeys)

	inKV = make([]*keyVal, testSize)
	for i := 0; i < testSize; i++ {
		// Generate a pseudo random flags byte
		myKey = sortedKeys[i]
		myFlags = byte(i * int(myKey))
		myFlags = myFlags & ^flagDirty & ^flagRemove
		kv = new(keyVal)
		kv.key = myKey
		kv.flags = myFlags
		kv.block = GetRandomBlock()

		inKV[i] = kv
	}

	lm, err = newLogManager(dirTest, "writeRead")

	if err != nil {
		t.Fatal("Could not create LogManager.")

	}
	err = lm.write(inKV)
	if err != nil {
		t.Fatal("Write failed.")
	}

	// Read each and every key from the file and compare them.
	for i := 0; i < testSize; i++ {
		kv, err = lm.read(inKV[i].key)
		if err != nil {
			t.Fatal("Read error.")
		}
		if kv == nil || kv.block == nil {
			t.Fatal("Written key not found or empty")
		}
		if kv.key != inKV[i].key || kv.flags != inKV[i].flags {
			t.Fatal("Written key found, but does not match.")
		}

		if !compareByteSlice(kv.block[:], inKV[i].block[:]) {
			t.Fatal("Written key found, but data block does not match.")
		}
	}

	// Read non-existing keys
	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if _, exist := myMap[myKey]; exist == false {
				break
			}
		}
		kv, err = lm.read(maxKey)
		if err != nil || kv != nil {
			t.Fatal("Unexpected behavior when searching for nonexistent key.")
		}
	}
}

// Writes keyVals to multipe log files in chain, and then reads them all.
func TestLogWriteChain(t *testing.T) {
	var lm *logManager
	var inKV [][]*keyVal
	var kv *keyVal
	var myKey uint64
	var sortedKeys [][]uint64
	var myFlags byte
	var err error
	var testSize = 100

	log.Printf("Log -- TestLogWriteChain -- Testing %d random key,flags,blocks for each of %d log files.", testSize, maxLogFiles)

	myMap := make(map[uint64]bool, testSize)
	sortedKeys = make([][]uint64, maxLogFiles)
	for j := 0; j < maxLogFiles; j++ {
		sortedKeys[j] = make([]uint64, testSize)
	}

	for j := 0; j < maxLogFiles; j++ {
		for i := 0; i < testSize; i++ {
			// Generate a new random key.
			for {
				myKey = GetRandomKey()
				if _, exist := myMap[myKey]; exist == false {
					break
				}
			}
			myMap[myKey] = true
			sortedKeys[j][i] = myKey
		}
		uint64Sort(sortedKeys[j])
	}

	inKV = make([][]*keyVal, maxLogFiles)
	for j := 0; j < maxLogFiles; j++ {
		inKV[j] = make([]*keyVal, testSize)
		for i := 0; i < testSize; i++ {
			// Generate a pseudo random flags byte
			myKey = sortedKeys[j][i]
			myFlags = byte(i * int(myKey))
			myFlags = myFlags & ^flagDirty & ^flagRemove
			kv = new(keyVal)
			kv.key = myKey
			kv.flags = myFlags
			kv.block = GetRandomBlock()

			inKV[j][i] = kv
		}
	}

	lm, err = newLogManager(dirTest, "writeChain")
	if err != nil {
		t.Fatal("Could not create LogManager.")

	}

	for j := 0; j < maxLogFiles; j++ {
		err = lm.write(inKV[j])
		if err != nil {
			t.Fatal("Write failed.")
		}
	}

	// Read each and every key from the file and compare them.
	for j := 0; j < maxLogFiles; j++ {
		for i := 0; i < testSize; i++ {
			kv, err = lm.read(inKV[j][i].key)
			if err != nil {
				t.Fatal("Read error.")
			}
			if kv == nil || kv.block == nil {
				t.Fatal("Written key not found or empty")
			}
			if kv.key != inKV[j][i].key || kv.flags != inKV[j][i].flags {
				t.Fatal("Written key found, but does not match.")
			}

			if !compareByteSlice(kv.block[:], inKV[j][i].block[:]) {
				t.Fatal("Written key found, but data block does not match.")
			}
		}
	}

}

// Function that tests extraction of header from a log file.
// First writes a log file, then tries to extract its header and compare.
func TestLogExtractHeader(t *testing.T) {
	var lm *logManager
	var lh *logHeader
	var inKV []*keyVal
	var kv *keyVal
	var myKey uint64
	var sortedKeys []uint64
	var myFlags byte
	var err error
	var name string
	var testSize = 100

	log.Printf("Log -- TestLogExtractHeader -- Testing %d random key,flags,blocks.", testSize)

	myMap := make(map[uint64]bool, testSize)
	sortedKeys = make([]uint64, testSize)
	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if _, exist := myMap[myKey]; exist == false {
				break
			}
		}
		myMap[myKey] = true
		sortedKeys[i] = myKey
	}
	uint64Sort(sortedKeys)

	inKV = make([]*keyVal, testSize)
	for i := 0; i < testSize; i++ {
		// Generate a pseudo random flags byte
		myKey = sortedKeys[i]
		myFlags = byte(i * int(myKey))
		myFlags = myFlags & ^flagDirty & ^flagRemove
		kv = new(keyVal)
		kv.key = myKey
		kv.flags = myFlags
		kv.block = GetRandomBlock()

		inKV[i] = kv

	}

	lm, err = newLogManager(dirTest, "extractHeader")
	if err != nil {
		t.Fatal("Could not create LogManager.")
	}
	// Make sure a NEW file is created.
	for checkFileExists(lm.generateName()) {
		lm.nextFD++
	}
	// This will be the full name of test file.
	name = lm.generateName()

	err = lm.write(inKV)
	if err != nil {
		t.Fatal("Write failed.")
	}

	lh, err = extractHeader(name)
	if err != nil {
		t.Fatal("Error in reading header.")
	}

	if lh == nil || lh.headerVersion != lm.headLog.headerVersion ||
		lh.numOfKeys != lm.headLog.numOfKeys ||
		!compareByteSlice(lh.merkleRoot, lm.headLog.merkleRoot) ||
		!compareByteSlice(lh.nextRoot, lm.headLog.nextRoot) {
		t.Error("Extracted info does not match.")
	}
}

// Tests opening of previously saved log files.
// Writes keyVals to multipe log files in chain, and then closes the logManager.
// It then Opens them and reads previous key.
func TestLogCloseOpen(t *testing.T) {
	var lm *logManager
	var inKV [][]*keyVal
	var kv *keyVal
	var myKey uint64
	var sortedKeys [][]uint64
	var myFlags byte
	var err error
	var testSize = 100

	log.Printf("Log -- TestLogCloseOpen -- Testing %d random key,flags,blocks for each of %d log files.", testSize, maxLogFiles)

	myMap := make(map[uint64]bool, testSize)
	sortedKeys = make([][]uint64, maxLogFiles)
	for j := 0; j < maxLogFiles; j++ {
		sortedKeys[j] = make([]uint64, testSize)
	}

	for j := 0; j < maxLogFiles; j++ {
		for i := 0; i < testSize; i++ {
			// Generate a new random key.
			for {
				myKey = GetRandomKey()
				if _, exist := myMap[myKey]; exist == false {
					break
				}
			}
			myMap[myKey] = true
			sortedKeys[j][i] = myKey
		}
		uint64Sort(sortedKeys[j])
	}

	inKV = make([][]*keyVal, maxLogFiles)
	for j := 0; j < maxLogFiles; j++ {
		inKV[j] = make([]*keyVal, testSize)
		for i := 0; i < testSize; i++ {
			// Generate a pseudo random flags byte
			myKey = sortedKeys[j][i]
			myFlags = byte(i * int(myKey))
			myFlags = myFlags & ^flagDirty & ^flagRemove
			kv = new(keyVal)
			kv.key = myKey
			kv.flags = myFlags
			kv.block = GetRandomBlock()

			inKV[j][i] = kv
		}
	}

	lm, err = newLogManager(dirTest, "closeOpen")
	if err != nil {
		t.Fatal("Could not create LogManager.")

	}

	for j := 0; j < maxLogFiles; j++ {
		err = lm.write(inKV[j])
		if err != nil {
			t.Fatal("Write failed.")
		}
	}

	log.Println("Log -- TestLogCloseOpen -- Closing, re-openning, and reading the data.")
	err = lm.close()
	if err != nil {
		t.Fatal("Could not close.")
	}
	err = lm.open(dirTest, "closeOpen")
	if err != nil {
		t.Fatal("Could not re-open.")
	}
	// Read each and every key from the file and compare them.
	for j := 0; j < maxLogFiles; j++ {
		for i := 0; i < testSize; i++ {
			kv, err = lm.read(inKV[j][i].key)
			if err != nil {
				t.Fatal("Read error.")
			}
			if kv == nil || kv.block == nil {
				t.Fatal("Written key not found or empty")
			}
			if kv.key != inKV[j][i].key || kv.flags != inKV[j][i].flags {
				t.Fatal("Written key found, but does not match.")
			}

			if !compareByteSlice(kv.block[:], inKV[j][i].block[:]) {
				t.Fatal("Written key found, but data block does not match.")
			}
		}
	}

}

// Writes unique keyVals to multipe log files in chain, and then merges them all.
// TODO test merge with overwrites and removals.
func TestLogMergeSimple(t *testing.T) {
	var lm *logManager
	var inKV [][]*keyVal
	var kv *keyVal
	var myKey uint64
	var sortedKeys [][]uint64
	var myFlags byte
	var err error
	var testSize = 100

	log.Printf("Log -- TestLogMergeSimple -- Testing %d random key,flags,blocks for each of %d log files.", testSize, maxLogFiles)

	myMap := make(map[uint64]bool, testSize)
	sortedKeys = make([][]uint64, maxLogFiles)
	for j := 0; j < maxLogFiles; j++ {
		sortedKeys[j] = make([]uint64, testSize)
	}

	for j := 0; j < maxLogFiles; j++ {
		for i := 0; i < testSize; i++ {
			// Generate a new random key.
			for {
				myKey = GetRandomKey()
				if _, exist := myMap[myKey]; exist == false {
					break
				}
			}
			myMap[myKey] = true
			sortedKeys[j][i] = myKey
		}
		uint64Sort(sortedKeys[j])
	}

	inKV = make([][]*keyVal, maxLogFiles)
	for j := 0; j < maxLogFiles; j++ {
		inKV[j] = make([]*keyVal, testSize)
		for i := 0; i < testSize; i++ {
			// Generate a pseudo random flags byte
			myKey = sortedKeys[j][i]
			myFlags = byte(i * int(myKey))
			myFlags = myFlags & ^flagDirty & ^flagRemove
			kv = new(keyVal)
			kv.key = myKey
			kv.flags = myFlags
			kv.block = GetRandomBlock()

			inKV[j][i] = kv
		}
	}

	lm, err = newLogManager(dirTest, "mergeSimple")
	if err != nil {
		t.Fatal("Could not create LogManager.")

	}

	for j := 0; j < maxLogFiles; j++ {
		err = lm.write(inKV[j])
		if err != nil {
			t.Fatal("Write failed.")
		}
	}

	log.Println("Log -- TestLogMergeSimple -- Merging log files.")
	lm.merge()
	log.Println("Log -- TestLogMergeSimple -- Merging copmlete, testing keyVals.")

	// Read each and every key from the file and compare them.
	for j := 0; j < maxLogFiles; j++ {
		for i := 0; i < testSize; i++ {
			kv, err = lm.read(inKV[j][i].key)
			if err != nil {
				t.Fatal("Read error.")
			}
			if kv == nil || kv.block == nil {
				t.Fatal("Written key not found or empty")
			}
			if kv.key != inKV[j][i].key || kv.flags != inKV[j][i].flags {
				t.Fatal("Written key found, but does not match.")
			}

			if !compareByteSlice(kv.block[:], inKV[j][i].block[:]) {
				t.Fatal("Written key found, but data block does not match.")
			}
		}
	}

}
