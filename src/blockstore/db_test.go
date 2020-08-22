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

func TestDBSimple(t *testing.T) {
	var db *DataBase
	var writeBlock []*Block
	var myKey uint64
	var testSize int
	var err error

	testSize = 5 * tableSize

	log.Println("DataBase -- TestDBSimple -- Writing more entries than the table can hold. It needs to flush it to disk.")

	db, err = CreateDataBase(dirTest, "db")
	if err != nil {
		t.Error("Could not create DataBase.")
	}

	myMap := make(map[uint64]bool, testSize)
	writeBlock = make([]*Block, testSize)

	for i := 0; i < testSize; i++ {
		// Generate a new random key.
		for {
			myKey = GetRandomKey()
			if _, exist := myMap[myKey]; exist == false {
				break
			}
		}
		myMap[myKey] = true
		writeBlock[i] = GetRandomBlock()

		err = db.Write(myKey, writeBlock[i])
		if err != nil {
			t.Error("Write failed during iteration: ", i)
		}

	}
}
