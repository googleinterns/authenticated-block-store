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
			t.Errorf("Failed multiple write/read test at size = %d", testSize)
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
		t.Error(err)
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
				t.Error("Write failed: Error occured during write.")
			}
		}
		uint64Sort(sortedKeys)
		log.Printf("Table -- TestDirtyList -- Testing DirtyList at size = %d", len(sortedKeys))
		dirtyList, err = tb.getDirtyList()
		if err != nil {
			t.Error("Failure: DirtyList() error")
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

// Tests if the CommitKey method works correctly.
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
		t.Error(err)
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
			t.Error("Write failed: Error occured during write.")
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
				t.Errorf("Commit item %d failed.", index)
			}
			sortedKeys = append(sortedKeys[:index], sortedKeys[index+1:]...)
		}

		log.Printf("Table -- TestCommmit -- Testing DirtyList at size = %d", len(sortedKeys))
		dirtyList, err = tb.getDirtyList()
		if err != nil {
			t.Error("Failure: DirtyList() error")
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

//TODO test if re-writing an entry works as expected.
//TODO test if markRemove works as expected. write/markRemove/read an entry and it should return nil,nil
//TODO test if removal works as expected. write/markRemove/commit/read should return error.
// Also write all entries, then markRemove, commit, and write a new entry should work.
//TODO test if cache behavior is as expected.
