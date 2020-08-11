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

// This file implements a log manager for (key, value) store on disk.
// Each log file consists of a header, then a list of keys and offsets for
// datablocks. The keys are 7-byte long integers. i.e. uint64 numbers with
// highest most byte being zero.
// The log manager is supposed to be used in the DataBase. The table gets
// flushed to disk.
// Each log fiel consists of
//  ----------------
//       HEADER
//      Key-table
//     MerkleTree
//     Data blocks
//  ----------------
// The header holds the important information about the file and log chain.
// Such as merkleRoot, merkleRoot of the next log, number of entries.
// The merkleTree of the dataBlocks is stored to provide authenticity.
// The key table is a sorted list of keys, and the offset of corresponding
// data block.
// The merkeTree is a binary tree with leaves being hash of data blocks, and
// each parent node being hash of its children, all the way to the root.
// Furthermore, we include a signed version of the merkleRoot for authenticity
// Each hash is a []byte, and the merkleTree itself is list of hash, thus it is
// defined as [][]byte.
// The number of leaves would be the first power of 2 greateror equal to number
// of data blocks. For all the empty leaves (no data block), we put 0 hash.
// We also have the same number of middle nodes (including the signed root)
// Example: For a list of size [5..8], we would have 8 leaves, and 16 hash in
// total.
// Note that the number 8 is also the index of first leaf and each middle
// node [n] is the parent of two nodes/leaves [2n] , [2n+1]
//
// Ecnrypted root                  [0]
// root (or if a single leaf)      [1]
//                        [2]               [3]
//                    [4]     [5]      [6]      [7]
//                  [8][9] [10][11] [12][13] [14][15]
//

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"log"
	"os"
	"strconv"
)

// The number of log files before a merge is called.
const maxLogFiles = 5

// Depending on the type of hash, this defines the length of hash.
const hashLength = 20

// Used in file names, a digit will be injected in between.
const defaultPrefix = "testFile"
const defaultSuffix = ".log"

// A version for the log file, to determin the structure.
const defaultHeaderVersion = 1

// Default size for header of the file.
// As for version 1, we keep it simple and static:
// 8 bytes for version (uint64)
// 8 bytes whole header size (uint64)
// 8 bytes key index size (uint64)
// 20 bytes merkleRoot
// 20 bytes root of next log to hep form the chain.
const defaultHeaderSize = 64

// Struct that stores the major information within the header of a log file.
// And other variables that help logManager locate log files.
type logHeader struct {
	headerVersion int
	// Number of keys stored in a log file, defines the size of the
	// key table portion.
	numOfKeys  int
	merkleRoot []byte
	nextRoot   []byte

	// A pointer to the log file itself, kept in memory.
	file *os.File

	// Pointer to next logHeader in chain.
	nextLog *logHeader
}

// Main log maanger object, which holds a pointer to the head log file (if any)
// It can have its own distinct name/path. It also includes an increasing
// counter for adding new files.
// TODO add path.
type logManager struct {
	headLog    *logHeader
	namePrefix string
	// A file descriptor number that increases, used in file names.
	nextFD int
}

// Constructs a log manager and initialize the members.
// TODO accept path/default name.
// TODO verify the same path/prefix does not exist.
func newLogManager() (*logManager, error) {
	var lm *logManager = new(logManager)
	if lm == nil {
		return nil, errors.New("Could not create the LogManager")
	}
	lm.namePrefix = defaultPrefix
	lm.nextFD = 0
	return lm, nil
}

// A helper function that checks whether a file exists.
func checkFileExists(name string) bool {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// A placeholder function to encrypt a block.
// Currently it just copies.
func encryptBlock(dstBlock, srcBlock *Block) error {
	if srcBlock == nil || dstBlock == nil || len(srcBlock) != len(dstBlock) {
		log.Println("Block encryption fail.")
		return errors.New("Block encryption fail.")
	}
	for i, v := range srcBlock {
		dstBlock[i] = v
	}
	return nil
}

// A placeholder function to decrypt a block.
// Currently it just copies.
func decryptBlock(dstBlock, srcBlock *Block) error {
	if srcBlock == nil || dstBlock == nil || len(srcBlock) != len(dstBlock) {
		return errors.New("Block decryption fail.")
	}
	for i, v := range srcBlock {
		dstBlock[i] = v
	}
	return nil
}

// Hash function that takes srcBytes of arbitrary size, and dstHash
// with a fixed, expected size. Calculates the hash of srcBytes, then
// Overwrites the dstHash with the newly created hash.
func hashData(dstHash, srcBytes []byte) error {
	if srcBytes == nil || dstHash == nil || len(dstHash) != hashLength {
		return errors.New("Hashing fail.")
	}

	h := sha1.New()
	h.Write(srcBytes)
	s := h.Sum(nil)

	if len(s) != hashLength {
		return errors.New("Hash length fail.")
	}

	for i, v := range s {
		dstHash[i] = v
	}
	return nil
}

// Function that takes the merkleRoot and encrypts it for later authentication.
// expects srcHash and dstHash to be of the same fixed length.
// Overwrites the dstHash.
// TODO Right now it's a dummy encryption.
func signMerkleRoot(dstHash, srcHash []byte) error {
	if srcHash == nil || dstHash == nil || len(srcHash) != hashLength || len(dstHash) != hashLength {
		return errors.New("Sign merkle fail.")
	}
	for i, b := range srcHash {
		dstHash[i] = 255 - b
	}
	return nil
}

// Takes a list of keyVals, and generate an index table byte slice to be
// written in the log file (list of keys included in the log file).
// The keyVals must be sorted by key. The flags will be concatenated to keys
// from the left (1 byte flags | 7 byte key)
func generateIndex(kvList []*keyVal) ([]byte, error) {
	var dst []byte
	var tmp uint64
	var maxKey uint64 = 0

	dst = make([]byte, 8*len(kvList), 8*len(kvList))
	if dst == nil {
		return nil, errors.New("Could not allcoate memory for index table.")
	}

	for i, kv := range kvList {
		if kv.key < maxKey {
			return nil, errors.New("Input KeyVals are not sorted.")
		}
		maxKey = kv.key
		tmp = encodeKeyFlags(kv.key, kv.flags)
		binary.LittleEndian.PutUint64(dst[i*8:i*8+8], tmp)
	}
	return dst, nil
}

// Counter part of generateIndex(), takes the index table bytes from the
// log file, and interprets them as list of uint64 (key,flags) tuples.
// Extracts a list of keyVals from them (while ignoreing blocks).
func decomposeIndex(src []byte) ([]*keyVal, error) {
	var count int
	var dst []*keyVal
	var kv *keyVal
	var tmp uint64

	if src == nil || len(src)%8 != 0 {
		return nil, errors.New("Error in input.")
	}

	count = len(src) / 8
	dst = make([]*keyVal, count)
	if dst == nil {
		return nil, errors.New("Could not allocate memory.")
	}

	for i := 0; i < count; i++ {
		kv = new(keyVal)
		if kv == nil {
			return nil, errors.New("Could not allocate memory.")
		}
		tmp = binary.LittleEndian.Uint64(src[i*8 : i*8+8])
		kv.key, kv.flags = decodeKeyFlags(tmp)
		dst[i] = kv
	}
	return dst, nil

}

// A helper function that generates the header for the log file to be written.
func generateHeader(lh *logHeader) ([]byte, error) {
	var headerSize int = defaultHeaderSize
	res := make([]byte, headerSize, headerSize)

	if res == nil {
		log.Println("Could not allocate memory for header.")
		return nil, errors.New("Could not allocate memory for header.")
	}
	// Writing the version of the header.
	binary.LittleEndian.PutUint64(res[0:8], uint64(lh.headerVersion))

	// Writing the size of header itself.
	binary.LittleEndian.PutUint64(res[8:16], uint64(headerSize))

	// Writing the size of indexTable.
	binary.LittleEndian.PutUint64(res[16:24], uint64(lh.numOfKeys))

	//TODO adding merkleRoot and root of next log

	return res, nil
}

/////////////////////////////////////////////////////
// Methods that are called on a logManager object. //
/////////////////////////////////////////////////////

// A helper function that generates a file name.
func (lm *logManager) generateName() string {
	return lm.namePrefix + strconv.Itoa(lm.nextFD) + defaultSuffix
}

// Writes a log file at the head.
// The input is a slice of keyVals that MUST be sorted by key.
// During write, the flags are appended to key (1 byte flags + 7 bytes key)
// TODO making sure to take care of removals.
// TODO make sure it moves the head.
func (lm *logManager) write(kvList []*keyVal) error {
	var err error
	var newHeader *logHeader
	var headerBytes, indexBytes []byte
	var bytesWritten int
	var encryptedBlock *Block
	var hashList [][]byte
	var numOfLeaves, numOfNodes, firstLeafIndex int
	var treeOffset int64

	// Fist assign a new header file.
	newHeader = new(logHeader)
	if newHeader == nil {
		log.Println("Could not allocate header.")
		return errors.New("Could not allocate header.")
	}
	newHeader.headerVersion = defaultHeaderVersion
	newHeader.numOfKeys = len(kvList)
	if lm.headLog != nil {
		newHeader.nextLog = lm.headLog
		newHeader.nextRoot = lm.headLog.merkleRoot
	}

	//TODO move down after completion/verification of write to
	// prevent corruption of head.
	lm.headLog = newHeader

	// Making sure we are writing a NEW file.
	for checkFileExists(lm.generateName()) {
		lm.nextFD++
	}
	newHeader.file, err = os.Create(lm.generateName())
	if err != nil {
		log.Println("Could not create log file.")
		return errors.New("Could not create log file.")
	}
	lm.nextFD++

	// Generate the header, key index table, and merkleTree in memory.
	headerBytes, err = generateHeader(newHeader)
	if err != nil {
		return errors.New("Could not generate header.")
	}

	indexBytes, err = generateIndex(kvList)
	if err != nil {
		return errors.New("Could not generate index table.")
	}

	numOfLeaves = 1
	for numOfLeaves < len(kvList) {
		numOfLeaves *= 2
	}
	// Item [0] will be the encrypted Root, Root will be [1],
	// and leaves are hash of blocks
	numOfNodes = 2 * numOfLeaves
	firstLeafIndex = numOfLeaves

	hashList = make([][]byte, numOfNodes, numOfNodes)
	if hashList == nil {
		log.Println("Could not allocate memory for hashList.")
		return errors.New("Could not allocate memory for hashList.")
	}

	// Pre-allocating all the space for hash list.
	// This also gives the empty leaves a hash of 0
	for i := 0; i < numOfNodes; i++ {
		hashList[i] = make([]byte, hashLength, hashLength)
		if hashList[i] == nil {
			log.Println("Could not allocate memory for hashList.")
			return errors.New("Could not allocate memory for hashList.")
		}
	}

	// Writing each part to file. The tree is not filled yet, an
	// empty tree is written, and the correct values will be written after
	// writing the data blocks.
	bytesWritten, err = newHeader.file.Write(headerBytes)
	if err != nil || bytesWritten != len(headerBytes) {
		log.Println("Could not write the header section.")
		return errors.New("Could not write the header section.")
	}

	bytesWritten, err = newHeader.file.Write(indexBytes)
	if err != nil || bytesWritten != len(indexBytes) {
		log.Println("Could not write the index section.")
		return errors.New("Could not write the index section.")
	}

	// Saving the offset, to seek and overwrite after writing data blocks.
	treeOffset, err = newHeader.file.Seek(0, 1)
	for i := 0; i < numOfNodes; i++ {
		bytesWritten, err = newHeader.file.Write(hashList[i])
		if err != nil || bytesWritten != len(hashList[i]) {
			log.Println("Could not write the tree section.")
			return errors.New("Could not write the tre section.")
		}
	}

	// Writing data blocks, which consist bulk of the log file.
	// The encrypted version of data blocks are written, and so the
	// hash of encrypted blocks are stored in the merkle tree.
	encryptedBlock = new(Block)
	for i, kv := range kvList {
		err = encryptBlock(encryptedBlock, kv.block)
		if err != nil {
			log.Println("Failure in encryption.")
			return errors.New("Failure in encryption.")
		}
		err = hashData(hashList[firstLeafIndex+i], encryptedBlock[:])
		if err != nil {
			log.Println("Failure in calculating hash.")
			return errors.New("Failure in calculating hash.")
		}
		bytesWritten, err = newHeader.file.Write(encryptedBlock[:])
		if err != nil || bytesWritten != len(encryptedBlock) {
			log.Println("Could not write the encrypted block.")
			return errors.New("Could not write the encrypted block.")
		}

	}

	// Populating the middle nodes of th merkleTree from the leaves up
	// to the root.
	for i := firstLeafIndex - 1; i > 0; i-- {
		err = hashData(hashList[i], append(hashList[2*i], hashList[2*i+1]...))
		if err != nil {
			log.Println("Failure in calculating hash.")
			return errors.New("Failure in calculating hash.")
		}
	}

	// Finally signing the merkleRoot.
	err = signMerkleRoot(hashList[0], hashList[1])
	if err != nil {
		log.Println("Failure in signing merkle root.")
		return errors.New("Failure in signing merkle root.")
	}

	// Re-writing the updated tree in the log file.
	_, err = newHeader.file.Seek(treeOffset, 0)
	for i := 0; i < numOfNodes; i++ {
		bytesWritten, err = newHeader.file.Write(hashList[i])
		if err != nil || bytesWritten != len(hashList[i]) {
			log.Println("Could not write the tree section.")
			return errors.New("Could not write the tre section.")
		}
	}

	return nil
}
