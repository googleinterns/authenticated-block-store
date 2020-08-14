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
// data blocks. The keys are 7-byte long integers. i.e. uint64 numbers with
// highest most byte being zero.
// The log manager is supposed to be used in the DataBase. The table gets
// flushed to disk.
// Each log file consists of
//  ----------------
//       HEADER
//      Key-table
//     Merkle Tree
//     Data blocks
//  ----------------
// The header holds the important information about the file and log chain,
// such as merkle root, merkle root of the next log, number of entries.
// The merkle tree of the data blocks is stored to provide authenticity.
// The key table is a sorted list of keys, and the offset of corresponding
// data block.
// The merkle tree is a binary tree with leaves being hash of data blocks, and
// each parent node being hash of its children, all the way to the root.
// Furthermore, we include a signed version of the merkle root for authenticity
// Each hash is a []byte, and the merkle tree itself is list of hash, thus it is
// defined as [][]byte.
// The number of leaves is the first power of 2 greater than or equal to number
// of data blocks. For all the empty leaves (no data block), we put 0 hash.
// We also have the same number of middle nodes (including the signed root)
// Example: For a list of size [5..8], we would have 8 leaves, and 16 hash in
// total.
// Note that the number 8 is also the index of first leaf and each middle
// node [n] is the parent of two nodes/leaves [2n] , [2n+1]
//
// Encrypted root                  [0]
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
	"math"
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

// A version for the log file, to determine the structure.
const defaultHeaderVersion = 1

// Default size for header of the file.
// As for version 1, we keep it simple and static:
// 8 bytes for version (uint64)
// 8 bytes whole header size (uint64)
// 8 bytes key index size (uint64)
// 20 bytes merkle root
// 20 bytes root of next log to help form the chain.
const defaultHeaderSize = 64

// Struct that stores the major information within the header of a log file.
// And other variables that help logManager locate log files.
type logHeader struct {
	headerVersion int
	// Number of keys stored in a log file, defines the size of the key table portion.
	numOfKeys  int
	merkleRoot []byte
	nextRoot   []byte

	// A pointer to the log file itself, kept in memory.
	file *os.File

	// Pointer to next logHeader in chain.
	nextLog *logHeader
}

// Main logManager object, which holds a pointer to the head log file (if any)
// It may have its own distinct name/path. It also includes an increasing
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
// TODO verify the same path/prefix does not exist. instead, it should be able
// to open an existing chain, and keep a handle to each file.
func newLogManager() (*logManager, error) {
	var lm *logManager = new(logManager)
	if lm == nil {
		return nil, errors.New("Could not create the LogManager")
	}
	lm.namePrefix = defaultPrefix
	lm.nextFD = 0

	// Create a random generator.
	if myRand == nil {
		myRand = randomGen()
	}

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

// Function that encrypts a block.
// TODO currently it just copies and lacks encryption.
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

// Function that decrypts a block.
// TODO currently it just copies and lacks decryption.
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
// overwrites the dstHash with the newly created hash.
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

// Function that takes the merkle root and encrypts it for later authentication.
// expects srcHash and dstHash to be of the same fixed length. Overwrites the dstHash.
// TODO right now it lacks encryption.
func signMerkleRoot(dstHash, srcHash []byte) error {
	if srcHash == nil || dstHash == nil || len(srcHash) != hashLength || len(dstHash) != hashLength {
		return errors.New("Sign merkle fail.")
	}
	for i, b := range srcHash {
		dstHash[i] = 255 - b
	}
	return nil
}

// Takes a list of keyVals, and generates an index table byte slice to be
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
// log file, and interprets them as a list of uint64 (key, flags) tuples.
// Extracts a list of keyVals from them (ignores data blocks).
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
	// Write the version of the header.
	binary.LittleEndian.PutUint64(res[0:8], uint64(lh.headerVersion))

	// Write the size of header itself.
	binary.LittleEndian.PutUint64(res[8:16], uint64(headerSize))

	// Write the size of indexTable.
	binary.LittleEndian.PutUint64(res[16:24], uint64(lh.numOfKeys))

	//TODO add merkle root and root of next log

	return res, nil
}

// Generates a merkle tree template. It only needs the number of keys.
// It is supposed to be empty, since it gets populated after block encryptions.
func generateEmptyTree(items int) ([][]byte, error) {
	var hashList [][]byte
	var numOfNodes int

	_, numOfNodes = getTreeSize(items)

	hashList = make([][]byte, numOfNodes, numOfNodes)
	if hashList == nil {
		log.Println("Could not allocate memory for hashList.")
		return nil, errors.New("Could not allocate memory for hashList.")
	}

	// Pre-allocate all the space for hash list.
	// This also gives the empty leaves a hash of 0
	for i := 0; i < numOfNodes; i++ {
		hashList[i] = make([]byte, hashLength, hashLength)
		if hashList[i] == nil {
			log.Println("Could not allocate memory for hashList.")
			return nil, errors.New("Could not allocate memory for hashList.")
		}
	}
	return hashList, nil
}

// A helper function that takes a merkle tree with populated leaves, and
// updates the middle nodes up to the root. Finally it signs the root.
func updateMerkleTree(hashList [][]byte) error {
	var err error
	var firstLeafIndex int

	firstLeafIndex = getFirstLeafIndex(hashList)
	for i := firstLeafIndex - 1; i > 0; i-- {
		err = hashData(hashList[i], append(hashList[2*i], hashList[2*i+1]...))
		if err != nil {
			log.Println("Failure in calculating hash.")
			return errors.New("Failure in calculating hash.")
		}
	}

	// Finally sign the merkle root.
	err = signMerkleRoot(hashList[0], hashList[1])
	if err != nil {
		log.Println("Failure in signing merkle root.")
		return errors.New("Failure in signing merkle root.")
	}
	return nil
}

// Function that takes a logHeader and reads the whole merkle tree from
// corresponding log file, into memory.
// TODO This can be improved by selectively reading only the branches
// in the tree that are going to be needed for authenticating a block.
func readTreeFromFile(lh *logHeader) [][]byte {
	var hashList [][]byte
	var err error
	var bytesRead int
	hashList, err = generateEmptyTree(lh.numOfKeys)
	if err != nil {
		log.Println("Failure in creating tree.")
		return nil
	}

	_, err = lh.file.Seek(getTreeOffset(lh), 0)
	if err != nil {
		log.Println("Failure in seek.")
		return nil
	}

	for i := 0; i < len(hashList); i++ {
		bytesRead, err = lh.file.Read(hashList[i])
		if err != nil || bytesRead != hashLength {
			log.Println("Failure in reading tree.")
			return nil
		}
	}

	return hashList
}

// Function that takes a logHeader and reads a single block from the file.
// Creates a new Block, or returns nil upon error.
func readBlockFromFile(lh *logHeader, index int) *Block {
	var err error
	var bytesRead int
	var tmpData []byte
	var block *Block = new(Block)

	tmpData = make([]byte, blockSize)
	dataOffset := getDataOffset(lh)

	_, err = lh.file.Seek(dataOffset+int64(index*blockSize), 0)
	if err != nil {
		log.Println("Failure in seek.")
		return nil
	}

	bytesRead, err = lh.file.Read(tmpData)
	if err != nil || bytesRead != blockSize {
		log.Println("Failure in reading block.")
		return nil
	}

	for i, b := range tmpData {
		block[i] = b
	}

	return block
}

// A helper function that takes a logHeader and based on the
// number of keys, returns the offset in file for index table.
func getIndexOffset(lh *logHeader) int64 {
	return int64(defaultHeaderSize)
}

// A helper function that takes a logHeader and based on the
// number of keys, returns the offset in file for merkle tree.
func getTreeOffset(lh *logHeader) int64 {
	return int64(defaultHeaderSize + 8*lh.numOfKeys)
}

// A helper function that takes a logHeader and based on the
// number of keys, returns the offset in file for data block[index].
func getDataOffset(lh *logHeader) int64 {
	_, numOfNodes := getTreeSize(lh.numOfKeys)
	return int64(defaultHeaderSize + 8*lh.numOfKeys + hashLength*numOfNodes)
}

// A helper function that calculates how many leaves and total nodes merkle tree
// needs. The number of leaves is the smallest power of two that can contain
// the items.
func getTreeSize(items int) (int, int) {
	var numOfLeaves, numOfNodes int

	numOfLeaves = int(math.Exp2(math.Ceil(math.Log2(float64(items)))))

	// Node[0] will be the encrypted root, root will be Node[1],
	// and leaves are hash of blocks.
	numOfNodes = 2 * numOfLeaves
	return numOfLeaves, numOfNodes
}

// Function that checks a merkle tree root to match to that of the logHeader,
// and make sure it is authenticated.
func validateMerkleRoot(lh *logHeader, hashList [][]byte) bool {
	var tmpHash []byte
	if !compareHash(lh.merkleRoot, hashList[1]) {
		return false
	}

	tmpHash = make([]byte, hashLength)
	signMerkleRoot(tmpHash, hashList[1])
	if !compareHash(tmpHash, hashList[0]) {
		return false
	}

	return true
}

// A helper function that returns the index of first leaf in the merkle Tree
// In a binary tree, it is very simple as the number of leaves and other nodes
// are equal.
func getFirstLeafIndex(hashList [][]byte) int {
	return len(hashList) / 2
}

// A helper function that compares two []byte of hashes for a match.
func compareHash(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true

}

// A function that takes a merkle tree, a block and the index of the block,
// and validates the authenticity of the block by calculating and verifying
// all the hashes from the leaf to the root.
func validateBlock(hashList [][]byte, block *Block, index int) bool {
	var tmpHash []byte
	var err error
	var firstLeafIndex int
	var nodeID int

	tmpHash = make([]byte, hashLength)

	// First calculate the hash of the block.
	err = hashData(tmpHash, block[:])
	if err != nil {
		return false
	}

	// Compare with existing hash in the tree.
	firstLeafIndex = getFirstLeafIndex(hashList)
	if !compareHash(tmpHash, hashList[firstLeafIndex+index]) {
		return false
	}

	// Validate rest of the tree up to the root.
	// In the case of binary tree, ID of the parent = ID of child / 2
	for nodeID = index / 2; nodeID > 0; nodeID /= 2 {
		// Calculating the hash of the node from its children.
		err = hashData(tmpHash, append(hashList[2*nodeID], hashList[2*nodeID+1]...))
		if err != nil {
			return false
		}

		// Compare with existing hash in the tree
		if !compareHash(tmpHash, hashList[nodeID]) {
			return false
		}
	}

	return true
}

// Function that takes a logHeader, checks the file for a target Key.
// If the key is found, creates a keyVal entry and populates it with
// the key and the flags. Returns the keyVal entry and the index of the key
// within the indexTable. Returns nil if the key is not found.
func isKeyInFile(targetKey uint64, lh *logHeader) (*keyVal, int, error) {
	var kv *keyVal
	var err error
	var start, end, index int
	var compactKey, myKey uint64
	var flags byte

	var tmpData []byte
	var bytesRead int

	// Read the key index table to memory for fast search.
	tmpData = make([]byte, 8*lh.numOfKeys)
	_, err = lh.file.Seek(getIndexOffset(lh), 0)
	if err != nil {
		log.Println("Failure in seek.")
		return nil, 0, errors.New("Failure in seek.")
	}

	bytesRead, err = lh.file.Read(tmpData)
	if err != nil || bytesRead != len(tmpData) {
		log.Println("Failure in reading index table.")
		return nil, 0, errors.New("Failure in reading index table.")
	}

	// Search the table using binary search, mask/ignore flags.
	start = 0
	end = lh.numOfKeys - 1

	for start <= end {
		index = start + (end-start)/2
		compactKey = binary.LittleEndian.Uint64(tmpData[8*index : 8*index+8])
		myKey, flags = decodeKeyFlags(compactKey)
		if myKey == targetKey {
			kv = new(keyVal)
			kv.key = myKey
			kv.flags = flags
			return kv, index, nil
		}
		if myKey > targetKey {
			end = index - 1
		} else {
			start = index + 1
		}
	}

	return nil, 0, nil
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
// TODO make sure to take care of removals.
// TODO make sure it moves the head in chain.
func (lm *logManager) write(kvList []*keyVal) error {
	var err error
	var newHeader *logHeader
	var headerBytes, indexBytes []byte
	var bytesWritten, copiedBytes int
	var emptyBytes int64
	var encryptedBlock *Block
	var hashList [][]byte
	var firstLeafIndex int

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
	// The new log is created and written, but the head pointer will be
	// updated only after verification of the new file to prevent
	// corruption of head.

	// Make sure a NEW file is created.
	for checkFileExists(lm.generateName()) {
		lm.nextFD++
	}
	newHeader.file, err = os.Create(lm.generateName())
	if err != nil {
		log.Println("Could not create log file.")
		return errors.New("Could not create log file.")
	}
	lm.nextFD++
	// The file does not get closed. It is kept open to access it for reads
	// and other methods. It only gets created, written, and synced.

	// Generate the header, key index table, and merkle tree in memory.
	headerBytes, err = generateHeader(newHeader)
	if err != nil {
		return errors.New("Could not generate header.")
	}

	indexBytes, err = generateIndex(kvList)
	if err != nil {
		return errors.New("Could not generate index table.")
	}

	hashList, err = generateEmptyTree(len(kvList))
	firstLeafIndex = getFirstLeafIndex(hashList)
	if err != nil {
		return errors.New("Could not generate empty tree.")
	}

	// Skip header, index, tree parts in the file to be filled later.
	emptyBytes = int64(len(headerBytes) + len(indexBytes) + len(hashList)*hashLength)
	err = newHeader.file.Truncate(emptyBytes)
	if err != nil {
		log.Println("Could not truncate file to size.")
		return errors.New("Could not truncate file to size.")
	}
	_, err = newHeader.file.Seek(emptyBytes, 0)
	if err != nil {
		log.Println("Failure in seek.")
		return errors.New("Failure in seek.")
	}

	// Write data blocks, which consist bulk of the log file.
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

	// Populate the middle nodes of the merkle tree starting from the
	// leaves up to the root.
	err = updateMerkleTree(hashList)
	if err != nil {
		log.Println("Failure in updating merkle tree.")
		return errors.New("Failure in updating merkle tree.")
	}

	// Update the merkle root of this logHeader with the calculated root.
	newHeader.merkleRoot = make([]byte, hashLength)
	copiedBytes = copy(newHeader.merkleRoot, hashList[1])
	if copiedBytes != hashLength {
		log.Println("Could not save the merkle root.")
		return errors.New("Could not save the merkle root.")
	}

	// Write the correct header, index table, and updated tree in the log file.
	_, err = newHeader.file.Seek(0, 0)
	if err != nil {
		log.Println("Failure in rewind.")
		return errors.New("Failure in rewind.")
	}

	// Write header.
	bytesWritten, err = newHeader.file.Write(headerBytes)
	if err != nil || bytesWritten != len(headerBytes) {
		log.Println("Could not write the header section.")
		return errors.New("Could not write the header section.")
	}

	// Write index table.
	bytesWritten, err = newHeader.file.Write(indexBytes)
	if err != nil || bytesWritten != len(indexBytes) {
		log.Println("Could not write the index section.")
		return errors.New("Could not write the index section.")
	}

	// Write merkle tree.
	for i := 0; i < len(hashList); i++ {
		bytesWritten, err = newHeader.file.Write(hashList[i])
		if err != nil || bytesWritten != len(hashList[i]) {
			log.Println("Could not write the tree section.")
			return errors.New("Could not write the tre section.")
		}
	}

	// Force flush the system buffer.
	newHeader.file.Sync()

	lm.headLog = newHeader
	return nil
}

// Function that tries to read a key from the log files.
// Starts from the head and moves down the chain, looking in each file
// for the key.  If it does not find the key, returns nil.
// If a key is written as removed, it will be returned with the removed flag.
// The caller can then check it.
func (lm *logManager) read(keyIn uint64) (*keyVal, error) {
	var err error
	var lh *logHeader
	var targetKey uint64
	var ok bool
	var index int
	var kv *keyVal
	var hashList [][]byte

	targetKey, ok = CleanKey(keyIn)
	if !ok {
		log.Println("Key out of range.")
		return nil, errors.New("Key out of range.")
	}

	// Iterate over the chain
	for lh = lm.headLog; lh != nil; lh = lh.nextLog {
		kv, index, err = isKeyInFile(targetKey, lh)
		if err != nil {
			log.Println("Failure in checking file for key.")
			return nil, errors.New("Failure in checking file for key.")
		}
		// The key is found.
		if kv != nil {
			break
		}
	}
	if kv == nil {
		return nil, nil
	}

	// Both the data block, and the merkle tree (for validation) is needed.
	// First read the tree (to seek forward only).
	hashList = readTreeFromFile(lh)
	if hashList == nil {
		log.Println("Failure in reading tree from file.")
		return nil, errors.New("Failure in reading tree from file.")
	}

	// Check the merkle root first.
	ok = validateMerkleRoot(lh, hashList)
	if !ok {
		log.Println("Could not validate the merkle root.")
		return nil, errors.New("Could not validate the merkle root.")

	}

	// Read the target block.
	kv.block = readBlockFromFile(lh, index)
	if kv.block == nil {
		log.Println("Failure in reading block from file.")
		return nil, errors.New("Failure in reading block from file.")
	}

	// Finally, validate the block in the tree.
	ok = validateBlock(hashList, kv.block, index)
	if !ok {
		log.Println("Could not validate the block in the merkle tree.")
		return nil, errors.New("Could not validate the block in the merkle tree.")
	}

	return kv, nil

}
