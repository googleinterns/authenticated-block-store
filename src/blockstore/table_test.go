package blockstore

import (
	"log"
	"sort"
	"testing"
)

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

// Tries writing to and reading from a table.
// This will call multiWriteRead function with different number of entries.
// Starting from 1, doubling the number of entries, until full TableSize.
func TestWriteRead(t *testing.T) {
	var testSize int

	testSize = 0
	for {
		log.Printf("Table -- TestWriteRead -- Calling multiWriteRead with size = %d\n", testSize)
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
	var err error
	var tb *tableManager
	myRand := randomGen()

	writeBlock = make([]*Block, testSize)
	readBlock = make([]*Block, testSize)

	tb, err = newTableManager()
	if err != nil {
		t.Error(err)
		return false
	}

	for i := 0; i < testSize; i++ {
		// Generating a new random key.
		for {
			myKey = uint64(myRand.Intn(int(maxKey)))
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}

		writeBlock[i] = GetRandomBlock()

		err = tb.write(myKey, writeBlock[i])
		if err != nil {
			t.Error("Write failed: Error occured during write.")
			return false
		}

		readBlock[i], err = tb.read(myKey)

		if err != nil {
			t.Error("Read failed: Written key is not found.")
			return false
		}
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
// Writes blocks without commiting, then verifying the result from DirtyList method.
// Starting from 1, doubling the number of entries, until full TableSize.
func TestDirtyList(t *testing.T) {
	var myKey uint64
	var sortedKeys, dirtyList []uint64
	var tmpBlock *Block
	var err error
	var tb *tableManager
	var index, testSize int
	myRand := randomGen()

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	testSize = 0
	index = 0
	for {
		for ; index < testSize; index++ {
			// Generating a new random key.
			for {
				myKey = uint64(myRand.Intn(int(maxKey)))
				if entry, _ := tb.getEntry(myKey); entry == nil {
					break
				}
			}
			sortedKeys = append(sortedKeys, myKey)
			tmpBlock = GetRandomBlock()

			err = tb.write(myKey, tmpBlock)
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
		uint64Sort(dirtyList)

		for i, k := range sortedKeys {
			if dirtyList[i] != k {
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
	var sortedKeys, dirtyList []uint64
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
		// Generating a new random key.
		for {
			myKey = uint64(myRand.Intn(int(maxKey)))
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}
		sortedKeys = append(sortedKeys, myKey)
		tmpBlock = GetRandomBlock()

		err = tb.write(myKey, tmpBlock)
		if err != nil {
			t.Fatal("Write failed: Error occured during write.")
		}
	}
	uint64Sort(sortedKeys)

	var index int
	for {

		// Random commits
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
		uint64Sort(dirtyList)

		for i, k := range sortedKeys {
			if dirtyList[i] != k {
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
// Each time a new datablock is generated and compared with the updated entry.
func TestReWrite(t *testing.T) {
	var myKey uint64
	var writeBlock, readBlock *Block
	var err error
	var tb *tableManager
	var testSize = tableSize
	var retries int = 8
	myRand := randomGen()

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("Table -- TestReWrite -- Writing %d entries, each on %d times.", testSize, retries)
	for i := 0; i < testSize; i++ {
		// Generating a new random key.
		for {
			myKey = uint64(myRand.Intn(int(maxKey)))
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}

		for j := 1; j <= retries; j++ {
			writeBlock = GetRandomBlock()
			err = tb.write(myKey, writeBlock)
			if err != nil {
				t.Fatalf("Entry #%d write #%d failed: Error occured during write.", i, j)
			}

			readBlock, err = tb.read(myKey)
			if err != nil {
				t.Fatalf("Entry #%d read #%d failed: Written key is not found.", i, j)
			}
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
	var writeBlock, readBlock *Block
	var err error
	var tb *tableManager
	var tmpEntry *tableEntry
	var testSize = tableSize
	myRand := randomGen()

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("Table -- TestMarkRemove -- Writing %d entries, mark each for removal.", testSize)
	for i := 0; i < testSize; i++ {
		// Generating a new random key.
		for {
			myKey = uint64(myRand.Intn(int(maxKey)))
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}
		writeBlock = GetRandomBlock()

		err = tb.write(myKey, writeBlock)
		if err != nil {
			t.Fatalf("Entry #%d write failed: Error occured during write.", i)
		}

		readBlock, err = tb.read(myKey)
		if err != nil {
			t.Fatalf("Entry #%d read failed: Written key is not found.", i)
		}
		if readBlock != writeBlock {
			t.Fatalf("Entry #%d read failed: Block does not match.", i)
		}

		// Marking same key for removal.
		err = tb.markRemove(myKey)
		if err != nil {
			t.Fatalf("Entry #%d first call to markRemove failed.", i)
		}

		tmpEntry, err = tb.getEntry(myKey)
		if err != nil || tmpEntry == nil {
			t.Fatalf("Entry #%d getEntry failed after markRemove.", i)
		}

		readBlock, err = tb.read(myKey)
		if err != nil {
			t.Fatalf("Entry #%d read failed: Written key is not found after markRemove.", i)
		}
		if readBlock != nil {
			t.Fatalf("Entry #%d read failed: Block is not nil after markRemove.", i)
		}

		// Marking same key for removal again.
		err = tb.markRemove(myKey)
		if err != nil {
			t.Fatalf("Entry #%d second call to markRemove failed.", i)
		}

		tmpEntry, err = tb.getEntry(myKey)
		if err != nil || tmpEntry == nil {
			t.Fatalf("Entry #%d getEntry failed after markRemove twice.", i)
		}

		readBlock, err = tb.read(myKey)
		if err != nil {
			t.Fatalf("Entry #%d read failed: Written key is not found after markRemove twice.", i)
		}
		if readBlock != nil {
			t.Fatalf("Entry #%d read failed: Block is not nil after markRemove twice.", i)
		}
	}
}

// Tests if removing entries works. First fills the table. Then removes entries
// one by one and checks removal. Then retries filling the table and repeat
// multiple times to ensure functinoality.
func TestRemove(t *testing.T) {
	var myKey uint64
	var keys []uint64
	var writeBlock *Block
	var err error
	var tb *tableManager
	var tmpEntry *tableEntry
	var testSize = tableSize
	var retries = 8
	myRand := randomGen()

	tb, err = newTableManager()
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("Table -- TestRemove -- Writing %d entries, removing one bye one, and repeating %d times.", testSize, retries)
	for try := 1; try <= retries; try++ {
		keys = keys[:0]
		// Filling the table.
		for i := 0; i < testSize; i++ {
			// Generating a new random key.
			for {
				myKey = uint64(myRand.Intn(int(maxKey)))
				if entry, _ := tb.getEntry(myKey); entry == nil {
					break
				}
			}
			keys = append(keys, myKey)

			writeBlock = GetRandomBlock()

			err = tb.write(myKey, writeBlock)
			if err != nil {
				t.Fatal("Write failed: Error occured during write.")
			}
		}
		// Making sure the table is full.
		// Generating a new random key.
		for {
			myKey = uint64(myRand.Intn(int(maxKey)))
			if entry, _ := tb.getEntry(myKey); entry == nil {
				break
			}
		}
		writeBlock = GetRandomBlock()
		err = tb.write(myKey, writeBlock)
		if err == nil {
			t.Fatal("Write succeeded unexpectedly. The table should have been full.")
		}
		// Removing entries one by one.
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

//TODO test if cache behavior is as expected.
