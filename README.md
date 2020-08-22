# Authenticated Block Store

Secure persistent storage for trusted applications within an untrusted environment.

## Installation

There is no installation needed, you can set up your Go environment, clone the
repo and use this package. This package does not depend on other packages.


## Usage

Currently it is ready for testing. Once the higher level APIs are implemented,
you can include this package, and use higher level primitives to Create or Open
a DataBase, and call Write, Read, Remove, Flush, Merge methods.

## Testing

You can test the implementation by running:
```shell
./src/blockstore$ go test
```

## How it works

The DataBase takes (key,value) pairs and stores/loads them to/from memory and persistent
storage. The key is an 8-byte unsigned integer, which consists of 56 bits index
and 8 bits of flags. The index refers to a single value. The value is a fixed
size block of data.
The implementation of the storage uses the idea of log structured merge (LSM)
tree storage. This allows us to write to disk in sequential batches and not worry
about seek time.

Refer to <a href="https://en.wikipedia.org/wiki/Log-structured_merge-tree" target="_blank">`https://en.wikipedia.org/wiki/Log-structured_merge-tree`</a>

<a href="https://en.wikipedia.org/wiki/Log-structured_merge-tree"><img src="https://upload.wikimedia.org/wikipedia/commons/f/f2/LSM_Tree.png" title="LSM" alt="Log-structured merge-tree" width="80%"></a>

Updates are written along with the added entries into new files. As the number
of files grow, multiple files can be merged into a single larger file and older
versions of an entry can be discarded.

A read from the storage may need searching multiple files (starting from the
most recent one), until the entry is found.

<!-- language: lang-none -->
     _____________________      _____________________              _____________________
    |                     |    |                     |            |                     |
    |        Head         |    |                     |            |                     |
    |  (file version N)   | -> | (file version N-1)  | -> .... -> |  (file version 1)   |
    |_____________________|    |_____________________|            |_____________________|

A cache table is implemented in memory to improve access time.
The writes are reflected on the memory table, until table is full or
upon a manual or automatic flush, where the modified entries are written to
disk. The memory table also is a cache for reading entries. If an entry cannot
be found on the memory table, the files are searched in order.

The information on the disk are encrypted. To verify the integrity of the
information, a Merkle tree is formed and written to each file.
Merkle tree allows us to verify the authenticity of a part within a large file.

Refer to <a href="https://en.wikipedia.org/wiki/Merkle_tree" target="_blank">`https://en.wikipedia.org/wiki/Merkle_tree`</a>

<a href="https://en.wikipedia.org/wiki/Merkle_tree"><img src="https://upload.wikimedia.org/wikipedia/commons/thumb/9/95/Hash_Tree.svg/1280px-Hash_Tree.svg.png" title="Merkle Tree" alt="Merkle Tree" width="80%"></a>

The Merkle root itself is encrypted and stored in the file for authentication of
the Merkle tree. Any change in any of the blocks, will result in a different
Merkle hash.
Furthermore, the Merkle root is used to link the files together in a chain.
Each new file will include the Merkle root of the previous file.
Each log file looks something like this:

<!-- language: lang-none -->
     ____________ Log file ___________
    |                                 |
    |    ________ Header _________    |
    |   |                         |   |
    |   |         Version         |   |
    |   |    Number of entries    |   |
    |   |       Merkle root       |   |
    |   | Merkle root of next log |   |
    |   |_________________________|   |
    |                                 |
    |    ____ Encrypted Index ____    |
    |   |                         |   |
    |   |   (List of key,flags)   |   |
    |   |     (Sorted by key)     |   |
    |   |_________________________|   |
    |                                 |
    |    ______ Merkle Tree ______    |
    |   |                         |   |
    |   |  (Hash of data blocks)  |   |
    |   | (Hash of nodes to root) |   |
    |   |(Includes encrypted root)|   |
    |   |_________________________|   |
    |                                 |
    |    _ Encrypted Data Blocks _    |
    |   |                         |   |
    |   |                         |   |
    |   .           .             .   |
    |   .           .             .   |
    |   .           .             .   |
    |   |                         |   |
    |   |_________________________|   |
    |________________________________ |

## Structure of the code

The DataBase consists of two major components:
- **tableManager** in `src/blockstore/table.go`
  - Manages a cached version of entries in memory.
- **logManager** in `src/blockstore/log.go`
  - Manages access to persistent storage.

The primitives of key and value are defined in `src/blockstore/keyval.go`
- **keyVal**
  - This struct holds the key, flag, and a pointer to a block in memory. It is
    used within and between major components of the DataBase.


### tableManager
This component implements a cached version of entries in memory. Information in
the memory are held in map of (key, entry) items. Each entry is a `keyVal` struct
and pointers to implement least recently used (LRU) cache.
It provides:
- **write(keyVal)** writes a single entry (add/update).
- **read(key) -> keyVal** reads a single entry from table. (could be marked for
  removal)
- **markRemove(key)** marks an entry in table for removal (add entry if does not
  exist). The item will be removed upon commit.
- **commitKey(key)** marks an entry as not-dirty, so it can be evicted. Should be called after
  flushing to log file.
- **getDirtyList() -> [ ]keyVal** returns the list of all non-committed entries.

Furthermore it implements the LRU cache and update entries upon calls to write
and read


### logManager
This component implements access to persistent storage.
It provides:
- **write([ ]keyVal)** writes a list of keyVal entries to a new log file.
  It encrypts the data, creates the Merkle tree, and updates the head of the chain.
  A keyVal might be marked for removal in its flags, which includes a dummy block.
- **read(key) -> keyVal** searches the log files starting from the head. If the
  key is found in the index table, the data block is retrieved and decrypted.
  If the key was marked for removal, it is returned as well. The flags must be
  checked in order to prevent access to dummy block and respond appropriately.
- **merge()** merges existing log files in the chain into one new log file.
  Sorted entries are read, and a new sorted index table is created. For
  redundant entries, the newest version is kept. If the newest version is marked
  for removal, the entry will not be included in the new log file.
  The older files are removed upon successful write, and the head of chain is
  updated.
- **open(path)** opens an existing chained log files from the persistent storage
  and forms the chain in memory.



