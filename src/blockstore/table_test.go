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

// Tries writing to, and reading from a table.
// This calls multiWriteRead function with different number of entries.
// Starts from 1, doubles the number of entries, until full TableSize.
func TestTableWriteRead(t *testing.T) {
	var testSize int

	testSize = 0
	for {
		log.Printf("Table -- TestTableWriteRead -- Calling multiWriteRead with size = %d\n", testSize)
		if multiWriteRead(testSize, t) == false {
			t.Fatalf("Failed multiple write/read test at size = %d", testSize)
		}
		if testSize == tableSize {
			break
		} else {
			testSize *= 2
			if testSize == 0 {
				testSize = 1
			}
			if testSize > tableSize {
				testSize = tableSize
			}
		}
	}
}

// Generates random blocks, writes them to table, and reads them,
// comparing to ensure they match.
func multiWriteRead(testSize int, t *testing.T) bool {
	var myKey uint64
	var writeBlock, readBlock []*Block
	var kv *keyVal
	var tmpKV keyVal
	var err error
	var tb *tableManager

	writeBlock = make([]*Block, testSize)
	readBlock = make([]*Block, testSize)

	tb, err = newTableManager()
	if err != nil {
		t.Error(err)
		return false
	}

	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}

		writeBlock[i] = GetRandomBlock()

		tmpKV.key = myKey
		// Generate a pseudo random flags byte
		tmpKV.flags = byte(i * int(myKey))
		tmpKV.block = writeBlock[i]

		err = tb.write(&tmpKV)
		if err != nil {
			t.Error("Write failed: Error occured during write.")
			return false
		}

		kv, err = tb.read(myKey)

		if err != nil {
			t.Error("Read failed: Written key is not found.")
			return false
		}
		readBlock[i] = kv.block
	}

	for i := 0; i < testSize; i++ {
		if writeBlock[i] != readBlock[i] {
			t.Error("Failure: Two blocks don't match!")
			return false
		}
	}
	return true
}

// Tests if the table returns correct dirtyList.
// Writes blocks without committing, then verifies the result from DirtyList method.
// Starts from 1, doubles the number of entries, until full TableSize.
func TestDirtyList(t *testing.T) {
	var myKey uint64
	var sortedKeys []uint64
	var dirtyList []*keyVal
	var tmpKV keyVal
	var tmpBlock *Block
	var err error
	var tb *tableManager
	var index, testSize int

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	testSize = 0
	index = 0
	for {
		for ; index < testSize; index++ {
			// Generate a new random key.
			for {
				myKey = GetRandomKey()
				if entry, _ := tb.getEntry(myKey); entry == nil {
					break
				}
			}
			sortedKeys = append(sortedKeys, myKey)
			tmpBlock = GetRandomBlock()

			tmpKV.key = myKey
			// Generate a pseudo random flags byte
			tmpKV.flags = byte(index * int(myKey))
			tmpKV.block = tmpBlock

			err = tb.write(&tmpKV)
			if err != nil {
				t.Fatal("Write failed: Error occured during write.")
			}
		}
		uint64Sort(sortedKeys)
		log.Printf("Table -- TestDirtyList -- Testing DirtyList at size = %d", len(sortedKeys))
		dirtyList, err = tb.getDirtyList()
		if err != nil {
			t.Fatal("Failure: DirtyList() error")
		}

		for i, k := range sortedKeys {
			if dirtyList[i].key != k {
				t.Error("Comparison failed. the keys and dirty list differ")
			}
		}

		if testSize == tableSize {
			break
		} else {
			testSize *= 2
			if testSize == 0 {
				testSize = 1
			}
			if testSize > tableSize {
				testSize = tableSize
			}
		}

	}

}

// Tests if the commitKey method works correctly.
// Writes blocks, then commits them by random, and verifies the
// result from DirtyList method.
func TestCommit(t *testing.T) {
	var myKey uint64
	var sortedKeys []uint64
	var dirtyList []*keyVal
	var tmpKV keyVal
	var tmpBlock *Block
	var err error
	var tb *tableManager
	var testSize int = tableSize
	myRand := randomGen()

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}
		sortedKeys = append(sortedKeys, myKey)
		tmpBlock = GetRandomBlock()

		tmpKV.key = myKey
		// Generate a pseudo random flags byte
		tmpKV.flags = byte(i * int(myKey))
		tmpKV.block = tmpBlock

		err = tb.write(&tmpKV)
		if err != nil {
			t.Fatal("Write failed: Error occured during write.")
		}
	}
	uint64Sort(sortedKeys)

	var index int
	for {

		// Make random commits.
		for len(sortedKeys) > testSize {
			index = (myRand.Intn(len(sortedKeys)))
			err = tb.commitKey(sortedKeys[index])
			if err != nil {
				t.Fatalf("Commit item %d failed.", index)
			}
			sortedKeys = append(sortedKeys[:index], sortedKeys[index+1:]...)
		}

		log.Printf("Table -- TestCommmit -- Testing DirtyList at size = %d", len(sortedKeys))
		dirtyList, err = tb.getDirtyList()
		if err != nil {
			t.Fatal("Failure: DirtyList() error")
		}

		for i, k := range sortedKeys {
			if dirtyList[i].key != k {
				t.Error("Comparison failed. the keys and dirty list differ")
			}
		}

		if testSize == 0 {
			break
		} else {
			testSize /= 2
		}

	}

}

// Writes the table, rewriting each entry multiple times.
// Each time a new data block is generated and compared with the updated entry.
func TestReWrite(t *testing.T) {
	var myKey uint64
	var kv *keyVal
	var tmpKV keyVal
	var writeBlock, readBlock *Block
	var err error
	var tb *tableManager
	var testSize = tableSize
	var retries int = 8

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("Table -- TestReWrite -- Writing %d entries, each on %d times.", testSize, retries)
	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}

		for j := 1; j <= retries; j++ {
			writeBlock = GetRandomBlock()
			tmpKV.key = myKey
			// Generate a pseudo random flags byte
			tmpKV.flags = byte(i * int(myKey))
			tmpKV.block = writeBlock
			err = tb.write(&tmpKV)
			if err != nil {
				t.Fatalf("Entry #%d write #%d failed: Error occured during write.", i, j)
			}

			kv, err = tb.read(myKey)
			if err != nil {
				t.Fatalf("Entry #%d read #%d failed: Written key is not found.", i, j)
			}
			readBlock = kv.block
			if readBlock != writeBlock {
				t.Fatalf("Entry #%d read #%d failed: Block does not match.", i, j)
			}
		}
	}

}

// Tests if markRemove works as expected. Writes entries to table, then calls
// markRemove and then tries to read them. It check this twice for each entry.
func TestMarkRemove(t *testing.T) {
	var myKey uint64
	var kv *keyVal
	var tmpKV keyVal
	var writeBlock, readBlock *Block
	var err error
	var tb *tableManager
	var tmpEntry *tableEntry
	var testSize = tableSize

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("Table -- TestMarkRemove -- Writing %d entries, mark each for removal.", testSize)
	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}
		writeBlock = GetRandomBlock()

		tmpKV.key = myKey
		// Generate a pseudo random flags byte
		tmpKV.flags = byte(i * int(myKey))
		tmpKV.block = writeBlock

		err = tb.write(&tmpKV)
		if err != nil {
			t.Fatalf("Entry #%d write failed: Error occured during write.", i)
		}

		kv, err = tb.read(myKey)
		if err != nil {
			t.Fatalf("Entry #%d read failed: Written key is not found.", i)
		}
		readBlock = kv.block
		if readBlock != writeBlock {
			t.Fatalf("Entry #%d read failed: Block does not match.", i)
		}

		// Mark the same key for removal.
		err = tb.markRemove(myKey)
		if err != nil {
			t.Fatalf("Entry #%d first call to markRemove failed.", i)
		}

		tmpEntry, err = tb.getEntry(myKey)
		if err != nil || tmpEntry == nil {
			t.Fatalf("Entry #%d getEntry failed after markRemove.", i)
		}

		kv, err = tb.read(myKey)
		if err != nil {
			t.Fatalf("Entry #%d read failed: Written key is not found after markRemove.", i)
		}
		readBlock = kv.block
		if readBlock != nil {
			t.Fatalf("Entry #%d read failed: Block is not nil after markRemove.", i)
		}

		// Mark the same key for removal again.
		err = tb.markRemove(myKey)
		if err != nil {
			t.Fatalf("Entry #%d second call to markRemove failed.", i)
		}

		tmpEntry, err = tb.getEntry(myKey)
		if err != nil || tmpEntry == nil {
			t.Fatalf("Entry #%d getEntry failed after markRemove twice.", i)
		}

		kv, err = tb.read(myKey)
		if err != nil {
			t.Fatalf("Entry #%d read failed: Written key is not found after markRemove twice.", i)
		}
		readBlock = kv.block
		if readBlock != nil {
			t.Fatalf("Entry #%d read failed: Block is not nil after markRemove twice.", i)
		}
	}
}

// Tests if removing entries works. First fills the table. Then removes entries
// one by one and checks removal. Then retries filling the table and repeat
// multiple times to ensure functionality.
func TestRemove(t *testing.T) {
	var myKey uint64
	var keys []uint64
	var writeBlock *Block
	var err error
	var tb *tableManager
	var tmpEntry *tableEntry
	var testSize = tableSize
	var retries = 8
	var tmpKV keyVal

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("Table -- TestRemove -- Writing %d entries, removing one bye one, and repeating %d times.", testSize, retries)
	for try := 1; try <= retries; try++ {
		keys = keys[:0]
		// Fill the table.
		for i := 0; i < testSize; i++ {
			// Generate new random key.
			for {
				myKey = GetRandomKey()
				if entry, _ := tb.getEntry(myKey); entry == nil {
					break
				}
			}
			keys = append(keys, myKey)

			writeBlock = GetRandomBlock()

			tmpKV.key = myKey
			// Generate a pseudo random flags byte
			tmpKV.flags = byte(i * int(myKey))
			tmpKV.block = writeBlock

			err = tb.write(&tmpKV)
			if err != nil {
				t.Fatal("Write failed: Error occured during write.")
			}
		}
		// Make sure the table is full.
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}
		writeBlock = GetRandomBlock()

		tmpKV.key = myKey
		// Generate a pseudo random flags byte
		tmpKV.flags = byte(try * int(myKey))
		tmpKV.block = writeBlock

		err = tb.write(&tmpKV)
		if err == nil {
			t.Fatal("Write succeeded unexpectedly. The table should have been full.")
		}
		// Remove entries one by one.
		for i := 0; i < testSize; i++ {
			err = tb.markRemove(keys[i])
			if err != nil {
				t.Fatalf("Entry #%d call to markRemove failed.", i)
			}
			tmpEntry, err = tb.getEntry(keys[i])
			if err != nil || tmpEntry == nil {
				t.Fatalf("Entry #%d getEntry failed after markRemove but before removal.", i)
			}
			err = tb.commitKey(keys[i])
			if err != nil {
				t.Fatalf("Entry #%d call to commitKey failed.", i)
			}
			tmpEntry, err = tb.getEntry(keys[i])
			if err == nil || tmpEntry != nil {
				t.Fatalf("Entry #%d remove failed.", i)
			}
		}
	}
}

// Tests if cache behavior is as expected.
// Fills the table and commits entries. Reads the entries based on sorted keys
// to predict the evict order. Then rewrites entries to the table and ensures
// the eviction order. Finally verifies that all new and old uncommitted entries
// are accessible.
func TestCache(t *testing.T) {
	var myKey uint64
	var kv *keyVal
	// List of uncommitted and committed keys.
	var keysUC, keysC []uint64
	var writeBlock *Block
	var err error
	var tb *tableManager
	var testSize = tableSize / 3
	var tmpEntry *tableEntry
	var index int
	var tmpKV keyVal

	myRand := randomGen()

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("Table -- TestCache -- Writing all %d entries.\n", tableSize)
	for i := 0; i < tableSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}
		keysUC = append(keysUC, myKey)
		writeBlock = GetRandomBlock()

		tmpKV.key = myKey
		// Generate a pseudo random flags byte
		tmpKV.flags = byte(i * int(myKey))
		tmpKV.block = writeBlock

		err = tb.write(&tmpKV)
		if err != nil {
			t.Fatalf("Entry #%d write failed: Error occured during write.", i)
		}
	}
	// Sort keysUC by key, to change the order of reads.
	uint64Sort(keysUC)

	// Read in order, before making commits.
	for _, k := range keysUC {
		kv, err = tb.read(k)
		if err != nil || kv.block == nil {
			t.Fatal("Error occured during reading.")
		}
	}
	log.Printf("Table -- TestCache -- Commiting %d entries.", testSize)
	// Make random commits.
	for len(keysC) < testSize {
		index = (myRand.Intn(len(keysUC)))
		myKey = keysUC[index]
		err = tb.commitKey(myKey)
		if err != nil {
			t.Fatalf("Commit item %d failed.", index)
		}
		keysC = append(keysC, myKey)
		keysUC = append(keysUC[:index], keysUC[index+1:]...)
	}
	// After committing testSize entries, table should be able to accept exactly testSize
	// entries again. Note that even though commits were by random, they were accessed
	// from the sorted keysUC list. So the evicted ones will also be sorted.
	log.Printf("Table -- TestCache -- Adding %d new entries to the full table.", testSize)
	uint64Sort(keysC)
	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}
		keysUC = append(keysUC, myKey)
		writeBlock = GetRandomBlock()

		tmpKV.key = myKey
		// Generate a pseudo random flags byte
		tmpKV.flags = byte(i * int(myKey))
		tmpKV.block = writeBlock

		err = tb.write(&tmpKV)
		if err != nil {
			t.Fatalf("Entry #%d write failed: Error occured during write.", i)
		}
		// Checks to see the correct item is evicted.
		// Since items were read based on keysC sorted list, evicted
		// items should follow the same order.
		myKey = keysC[i]
		tmpEntry, err = tb.getEntry(myKey)
		if err == nil || tmpEntry != nil {
			t.Fatal("Expected committed entry is not evicted.")
		}
		for j := i + 1; j < len(keysC); j++ {
			tmpEntry, err = tb.getEntry(keysC[j])
			if err != nil || tmpEntry == nil {
				t.Fatal("Unexpected committed entry is evicted.")
			}

		}
	}
	// Make sure all committed entries are evicted.
	for _, k := range keysC {
		tmpEntry, err = tb.getEntry(k)
		if err == nil || tmpEntry != nil {
			t.Fatal("Committed entry should have been evicted.")
		}
	}
	// Make sure all old and new uncommitted entries are accessible.
	if len(keysUC) != tableSize {
		t.Fatalf("Table should be full again at size %d. However, the new size is %d.", tableSize, len(keysUC))
	}
	for _, k := range keysUC {
		kv, err = tb.read(k)
		if err != nil || kv.block == nil {
			t.Fatal("Committed entry should have been evicted.")
		}
	}

}
