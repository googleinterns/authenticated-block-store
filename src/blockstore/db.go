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

// This file implements a data base that includes table managar and
// log manager components, and exports APIs to the user.

import (
	"errors"
	"log"
)

type DataBase struct {
	tm *tableManager
	lm *logManager
}

// Constructs a DataBase and initializes the members.
func CreateDataBase(path string, prefix string) (*DataBase, error) {
	var db *DataBase = new(DataBase)
	var err error

	db.tm, err = newTableManager()
	if err != nil {
		return nil, errors.New("Could not create the tableManager")
	}

	db.lm, err = newLogManager(path, prefix)
	if err != nil {
		return nil, errors.New("Could not create the logManager")
	}
	return db, nil
}

// Constructs a new DataBase, then opens existing chain of log files.
func OpenDataBase(path string, prefix string) (*DataBase, error) {
	var db *DataBase = new(DataBase)
	var err error

	db.tm, err = newTableManager()
	if err != nil {
		return nil, errors.New("Could not create the tableManager")
	}

	db.lm, err = newLogManager(path, prefix)
	if err != nil {
		return nil, errors.New("Could not create the logManager")
	}
	err = db.lm.open(path, prefix)
	if err != nil {
		return nil, errors.New("Could not open log files. -> " + err.Error())
	}
	return db, nil
}

/////////////////////////////////////////////////////////////
// Exported methods that are called on a data base object. //
/////////////////////////////////////////////////////////////

// Reads a single block of data addressed by the key.
// First searches memory table, then log files.
func (db *DataBase) Read(keyIn uint64) (*Block, error) {
	var kv *keyVal
	var err error

	kv, err = db.tm.read(keyIn)

	// Found in table.
	if kv != nil {
		if kv.flags&flagRemove == flagRemove {
			return nil, nil
		}
		return kv.block, nil
	}

	// Searching log files.
	kv, err = db.lm.read(keyIn)
	if err != nil {
		return nil, errors.New("Failure during read. -> " + err.Error())
	}
	if kv != nil {
		if kv.flags&flagRemove == flagRemove {
			return nil, nil
		}
		return kv.block, nil
	}

	// Not found.
	return nil, nil
}

// Writes a single block of data addressed with the key into the memory table.
// If the memory table is full, tries to flush the table to disk, and retry.
func (db *DataBase) Write(keyIn uint64, block *Block) error {
	var kv *keyVal = new(keyVal)
	var err error

	key, ok := CleanKey(keyIn)
	if !ok {
		log.Println("Key out of range.")
		return errors.New("Key out of range.")
	}

	kv.key = key
	kv.block = block

	err = db.tm.write(kv)
	// Successful write to table.
	if err == nil {
		return nil
	}

	// TODO identify error. Assuming cache full.
	err = db.Flush()
	if err != nil {
		return errors.New("Failure in flushing. -> " + err.Error())
	}

	// Try writing again.
	err = db.tm.write(kv)
	if err != nil {
		return errors.New("Failure in writing to table.")
	}
	// Successful write to table after flush.
	return nil
}

// Explicit call to flush the memory table to the disk.
func (db *DataBase) Flush() error {
	var err error
	var dl []*keyVal
	dl, err = db.tm.getDirtyList()
	if err != nil {
		return errors.New("Could not obtain dirty list. -> " + err.Error())
	}

	if len(dl) == 0 {
		return nil
	}

	err = db.lm.write(dl)
	if err != nil {
		return errors.New("Could not flush table to disk. -> " + err.Error())
	}

	// Commit all items on memory table.
	for _, item := range dl {
		err = db.tm.commitKey(item.key)
		if err != nil {
			return errors.New("Failure during committing keys. -> " + err.Error())
		}
	}

	return nil
}

// Explicit call to merge the log files on disk into a new one.
func (db *DataBase) Merge() error {
	var err error
	err = db.Flush()
	if err != nil {
		return errors.New("Failure during flush. -> " + err.Error())
	}
	err = db.lm.merge()
	if err != nil {
		return errors.New("Failure during merge. -> " + err.Error())
	}
	return nil
}

// Flushes the memory table, closes all handles to the log files, then
// invalidates the handles to tableManager and logManager.
func (db *DataBase) Close() error {
	var err error
	err = db.Flush()
	if err != nil {
		return errors.New("Failure during flush. -> " + err.Error())
	}
	err = db.lm.close()
	if err != nil {
		return errors.New("Failure during closing logManager. -> " + err.Error())
	}
	db.tm = nil
	db.lm = nil
	return nil
}
