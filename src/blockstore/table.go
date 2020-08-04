package blockstore

// This file implements a table manager for (key, value) store in memory.
// Including a LRU (Least Recently Used) approximation cache. The keys are
// 7-byte long integers. i.e. uint64 numbers with highest most byte being zero.
// The "value"s are tableEntry struct pointers. Each entry holds a pointer to
// // a data block, and pointers for implementing a LRU cache.
// This table is supposed to be used in the DataBase. It is basically a cached
// version of the log files, held in memory for faster reads, and a place for
// writes, before a commit is made to the log files.

import (
	"errors"
	"log"
)

// This constant describes the number of entries in the memory cache table.
// TODO small value is picked for testing purposes. Should be configured later.
const tableSize = 64

// This block describes flag bits.
const (
	// Defines if a table entry is dirty and cannot be evicted.
	flagDirty = uint8(1) << iota
	// Defines if a table entry is marked for removal.
	flagRemove
	// Other bits are reserved.
)

// This implements the struct for a single table entry.
type tableEntry struct {
	// Points to the data block.
	block *Block
	flags uint8

	// Pointers to the next/previous entry in LRU list.
	lruNext *tableEntry
	lruPrev *tableEntry
	// They key that this entry belongs to. It allows us to remove the key
	// from LRU list.
	assignedKey uint64
}

// This implements the table manager. It is used as a receiver for methods such as write()/read().
type tableManager struct {
	// A map of key->entry. Default map construct is used for simplicity.
	data map[uint64]*tableEntry
	// Dummy entries at the head/tail, points to the most/least recently used entry.
	lruHead *tableEntry
	lruTail *tableEntry

	// A static dirtyList which is re-used. Though this variable not exported,
	// a pointer to the slice is returned to the caller of getDirtyList().
	dirtyList []uint64
}

// Constructs a table manager and initialize the members.
func newTableManager() (*tableManager, error) {
	var tb *tableManager = new(tableManager)
	if tb == nil {
		return nil, errors.New("Could not create the TableManager")
	}
	tb.data = make(map[uint64]*tableEntry)
	if tb.data == nil {
		return nil, errors.New("Could not allocate the map for table.")
	}
	tb.dirtyList = make([]uint64, 0, tableSize)
	if tb.dirtyList == nil {
		return nil, errors.New("Could not allocate dirtyList.")
	}
	tb.lruHead = new(tableEntry)
	tb.lruTail = new(tableEntry)
	if tb.lruHead == nil || tb.lruTail == nil {
		return nil, errors.New("Could not allocate LRU Head/Tail.")
	}

	tb.lruHead.lruNext = tb.lruTail
	tb.lruTail.lruPrev = tb.lruHead
	return tb, nil
}

////////////////////////////////////////////////
// Methods that are called on a table object. //
////////////////////////////////////////////////

// Obtains a table entry by key.
// It is not expected to be called from outside.
func (tb *tableManager) getEntry(keyIn uint64) (*tableEntry, error) {
	key, ok := CleanKey(keyIn)
	if !ok {
		log.Println("Key out of range.")
		return nil, errors.New("Key out of range.")
	}
	entry, ok := tb.data[key]
	if !ok {
		return nil, errors.New("Key not found in table.")
	}
	return entry, nil
}

// Reads a datablock by key. if nil is returned with no error, the entry is
// marked for removal.
func (tb *tableManager) read(keyIn uint64) (*Block, error) {
	entry, err := tb.getEntry(keyIn)
	if err != nil {
		log.Println("Could not obtain entry.")
		return nil, errors.New("Could not obtain entry.")
	}
	if entry.flags&flagRemove != 0 {
		// dataBase should be able to tell if a dirtyKey is marked
		// for removal so it can write it as removed in log.
		return nil, nil
	}
	tb.updateLRUCacheHead(entry)
	return entry.block, nil
}

// Writes a (key, data block) pair in table.
func (tb *tableManager) write(keyIn uint64, val *Block) error {
	key, ok := CleanKey(keyIn)
	if !ok {
		log.Println("Key out of range.")
		return errors.New("Key out of range.")
	}
	entry, err := tb.getEntry(key)
	if err != nil {
		if len(tb.data) >= tableSize {
			// Table is full, we need to pick a cache victim.
			err := tb.evict()
			if err != nil {
				return errors.New("Table is full and needs to be flushed.")
			}
		}
		entry = new(tableEntry)
		if entry == nil {
			log.Println("Could not allocate table entry.")
			return errors.New("Could not allocate table entry.")
		}
	}
	entry.block = val
	entry.flags = flagDirty
	entry.assignedKey = key
	tb.data[key] = entry

	tb.updateLRUCacheHead(entry)
	return nil
}

// A key removal is 3 steps:
// 1-Mark as removed: Resides in the table, but not retrievable by read.
// 2-Mark as committed: The key is explicitly marked as commited.
// 3-Once an entry that is marked for removal is commited, it is removed.
func (tb *tableManager) markRemove(keyIn uint64) error {
	var err error
	var entry *tableEntry
	entry, err = tb.getEntry(keyIn)
	if err != nil {
		log.Println("Could not obtain entry.")
		return errors.New("Could not obtain entry.")
	}
	err = tb.write(keyIn, nil)
	if err != nil {
		log.Println("Could not write nil to entry for removal.")
		return errors.New("Marking for removal failed.")
	}
	entry.flags = flagDirty | flagRemove
	return nil
}

// remove() is not expected to be called from outside.
// A key removal is 3 steps:
// 1-Mark as removed: Resides in the table, but not retrievable by read.
// 2-Mark as committed: The key is explicitly marked as commited.
// 3-Once an entry that is marked for removal is commited, it is removed.
func (tb *tableManager) remove(keyIn uint64) error {
	key, ok := CleanKey(keyIn)
	if !ok {
		log.Println("Key out of range.")
		return errors.New("Key out of range.")
	}
	entry, ok := tb.data[key]
	if !ok {
		log.Println("Key not found in table.")
		return errors.New("Key not found in table.")
	}

	tb.removeFromLRUCache(entry)
	delete(tb.data, key)
	return nil
}

// Marks an entry as commited. e.g. called after writing to log file.
func (tb *tableManager) commitKey(keyIn uint64) error {
	key, ok := CleanKey(keyIn)
	if !ok {
		log.Println("Key out of range.")
		return errors.New("Key out of range.")
	}
	entry, ok := tb.data[key]
	if !ok {
		log.Println("Key not found in table.")
		return errors.New("Key not found in table.")
	}
	if entry.flags&flagDirty == 0 {
		// It is already comited.
		return nil
	}
	// If it has been marked for removal, the entry is then removed.
	if entry.flags&flagRemove != 0 {
		tb.remove(key)
		return nil
	}
	entry.flags = entry.flags & ^flagDirty
	return nil
}

// Removes item from LRU list.
func (tb *tableManager) removeFromLRUCache(entry *tableEntry) error {
	if entry == nil || entry == tb.lruHead || entry == tb.lruTail {
		return errors.New("Table entry not valid")
	}
	if entry.lruNext != nil && entry.lruPrev != nil {
		entry.lruNext.lruPrev = entry.lruPrev
		entry.lruPrev.lruNext = entry.lruNext
	}
	entry.lruNext = nil
	entry.lruPrev = nil
	return nil
}

// Moves an entry to the head of LRU list. e.g. upon recent usage or commit.
func (tb *tableManager) updateLRUCacheHead(entry *tableEntry) error {
	if entry == nil || entry == tb.lruHead || entry == tb.lruTail {
		return errors.New("Table entry not valid")
	}
	// First removes it from LRU list if exists.
	tb.removeFromLRUCache(entry)

	entry.lruNext = tb.lruHead.lruNext
	entry.lruPrev = tb.lruHead
	entry.lruNext.lruPrev = entry
	tb.lruHead.lruNext = entry
	return nil
}

// Returns a list of dirty entries (For commiting purpose).
// Note that his returns a pointer to he internal slice.
func (tb *tableManager) getDirtyList() ([]uint64, error) {
	// Reset the slice.
	tb.dirtyList = tb.dirtyList[:0]
	for k, ent := range tb.data {
		if ent.flags&flagDirty > 0 {
			tb.dirtyList = append(tb.dirtyList, k)
		}
	}
	return tb.dirtyList, nil
}

// Tries to evict/remove an old entry.
func (tb *tableManager) evict() error {
	var victim *tableEntry

	// Sweeping from tail to head, looking for a non-dirty candidate.
	dirtyFound := false
	for victim = tb.lruTail.lruPrev; victim != tb.lruHead; victim = victim.lruPrev {
		if victim.flags&flagDirty == 0 {
			dirtyFound = true
			break
		}
	}
	if dirtyFound == false {
		return errors.New("No victim found, maybe need a flush.")
	}
	tb.remove(victim.assignedKey)
	return nil
}
