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
// The merkle tree is a binary tree with leaves being hash of data (block|key|flags),
// and each parent node being hash of its children, all the way to the root.
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
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

// The number of log files before a merge is called.
const maxLogFiles = 5

// Depending on the type of hash, this defines the length of hash.
const hashLength = 20

// Used in file names, a digit will be injected in between.
const defaultPath = "./dirLogs/"
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
	namePath   string
	namePrefix string
	// A file descriptor number that increases, used in file names.
	nextFD int
}

// Constructs a log manager and initialize the members.
func newLogManager(path string, prefix string) (*logManager, error) {
	var lm *logManager = new(logManager)
	var err error
	if lm == nil {
		log.Println("Could not create the LogManager")
		return nil, errors.New("Could not create the LogManager")
	}
	path, prefix = cleanPathPrefix(path, prefix)

	lm.namePath = path
	lm.namePrefix = prefix

	lm.nextFD = 0

	// Make sure the path exists or create it.
	_ = os.Mkdir(path, 0755)

	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		log.Println("Could not create directory.")
		return nil, errors.New("Could not create directory.")
	}

	// Create a random generator.
	if myRand == nil {
		myRand = randomGen()
	}

	return lm, nil
}

// A helper function that makes sure the path and prefix are valid.
// Returns default strings if they are not.
func cleanPathPrefix(path string, prefix string) (string, string) {
	if len(path) == 0 {
		path = defaultPath
	}
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	if len(prefix) == 0 {
		prefix = defaultPrefix
	}
	// TODO other sanity/validity checks on path/prefix.
	return path, prefix
}

// A function that checks for existing log files, and returns a list of them
// without any specific order.
func readFileHeaders(lm *logManager) ([]*logHeader, error) {
	var list []*logHeader
	var tmpHeader *logHeader
	var files []os.FileInfo
	var err error

	files, err = ioutil.ReadDir(lm.namePath)
	if err != nil {
		log.Println("Could not stat files in path.")
		return nil, errors.New("Could not stat files in path. -> " + err.Error())
	}

	for _, f := range files {
		if strings.HasPrefix(f.Name(), lm.namePrefix) {
			tmpHeader, err = extractHeader(lm.namePath + f.Name())
			if err != nil {
				log.Println("Error in reading file.")
				return nil, errors.New("Error in reading file. ->" + err.Error())
			}
			if tmpHeader != nil {
				list = append(list, tmpHeader)
			}
		}
	}
	return list, nil
}

// Function that reads a file and tries to extract a header out of the file.
// TODO skip non header files with no errors.
func extractHeader(fullPath string) (*logHeader, error) {
	var newHeader *logHeader
	var err error
	var file *os.File
	var bytes []byte
	var bytesRead int
	var headerSize int = defaultHeaderSize

	file, err = os.Open(fullPath)
	if err != nil {
		log.Println("Could not open file.")
		return nil, errors.New("Could not open file. -> " + err.Error())
	}

	// Fist assign a new header file.
	newHeader = new(logHeader)
	if newHeader == nil {
		log.Println("Could not allocate header.")
		return nil, errors.New("Could not allocate header.")
	}

	bytes = make([]byte, headerSize, headerSize)
	bytesRead, err = file.Read(bytes)

	if err != nil || bytesRead < headerSize {
		log.Println("Could not read header section.")
		return nil, errors.New("Could not read header section.")
	}

	newHeader.headerVersion = int(binary.LittleEndian.Uint64(bytes[0:8]))
	if newHeader.headerVersion > defaultHeaderVersion {
		log.Println("Wrong header version.")
		return nil, errors.New("Wrong header version.")

	}
	if int(binary.LittleEndian.Uint64(bytes[8:16])) != headerSize {
		log.Println("Wrong header size.")
		return nil, errors.New("Wrong header size.")
	}
	newHeader.numOfKeys = int(binary.LittleEndian.Uint64(bytes[16:24]))

	newHeader.merkleRoot = make([]byte, hashLength, hashLength)
	newHeader.nextRoot = make([]byte, hashLength, hashLength)

	copy(newHeader.merkleRoot, bytes[24:24+hashLength])
	copy(newHeader.nextRoot, bytes[24+hashLength:24+2*hashLength])

	newHeader.file = file

	return newHeader, nil
}

// A helper function that checks whether a file exists.
func checkFileExists(name string) bool {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// Function that encrypts a slice of data.
// TODO currently it lacks encryption.
func encryptData(dstBlock, srcBlock []byte) error {
	if srcBlock == nil || dstBlock == nil || len(srcBlock) != len(dstBlock) {
		log.Println("Block encryption fail.")
		return errors.New("Block encryption fail.")
	}
	for i, v := range srcBlock {
		dstBlock[i] = 255 - v
	}
	return nil
}

// Function that decrypts a slice of data.
// TODO currently it lacks decryption.
func decryptData(dstBlock, srcBlock []byte) error {
	if srcBlock == nil || dstBlock == nil || len(srcBlock) != len(dstBlock) {
		log.Println("Block decryption fail.")
		return errors.New("Block decryption fail.")
	}
	for i, v := range srcBlock {
		dstBlock[i] = 255 - v
	}
	return nil
}

// Hash function that takes srcBytes of arbitrary size, and dstHash
// with a fixed, expected size. Calculates the hash of srcBytes, then
// overwrites the dstHash with the newly created hash.
func hashData(dstHash, srcBytes []byte) error {
	if srcBytes == nil || dstHash == nil || len(dstHash) != hashLength {
		log.Println("Hashing fail.")
		return errors.New("Hashing fail.")
	}

	h := sha1.New()
	h.Write(srcBytes)
	s := h.Sum(nil)

	if len(s) != hashLength {
		log.Println("Hash length fail.")
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
		log.Println("Sign merkle fail.")
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
// The result is encrypted for protection against modification.
func generateIndex(kvList []*keyVal) ([]byte, error) {
	var plain, encrypted []byte
	var tmp uint64
	var maxKey uint64 = 0
	var err error

	plain = make([]byte, 8*len(kvList), 8*len(kvList))
	encrypted = make([]byte, 8*len(kvList), 8*len(kvList))
	if plain == nil || encrypted == nil {
		log.Println("Could not allcoate memory for index table.")
		return nil, errors.New("Could not allcoate memory for index table.")
	}

	for i, kv := range kvList {
		if kv.key < maxKey {
			log.Println("Input KeyVals are not sorted.")
			return nil, errors.New("Input KeyVals are not sorted.")
		}
		maxKey = kv.key
		tmp = encodeKeyFlags(kv.key, kv.flags)
		binary.LittleEndian.PutUint64(plain[i*8:i*8+8], tmp)
	}

	err = encryptData(encrypted, plain)
	if err != nil {
		log.Println("Failure in encryption. -> " + err.Error())
		return nil, errors.New("Failure in encryption. -> " + err.Error())
	}
	return encrypted, nil
}

// Counter part of generateIndex(), takes the index table bytes from the
// log file, and interprets them as a list of uint64 (key, flags) tuples.
// Extracts a list of keyVals from them (ignores data blocks).
func decomposeIndex(srcEncrypted []byte) ([]*keyVal, error) {
	var count int
	var dst []*keyVal
	var kv *keyVal
	var tmp uint64
	var srcPlain []byte
	var err error

	if srcEncrypted == nil || len(srcEncrypted)%8 != 0 {
		log.Println("Error in input.")
		return nil, errors.New("Error in input.")
	}

	srcPlain = make([]byte, len(srcEncrypted))
	if srcPlain == nil {
		log.Println("Could not allocate memory.")
		return nil, errors.New("Could not allocate memory.")
	}
	err = decryptData(srcPlain, srcEncrypted)
	if err != nil {
		log.Println("Failure in decryption. -> " + err.Error())
		return nil, errors.New("Failure in decryption. -> " + err.Error())
	}

	count = len(srcPlain) / 8
	dst = make([]*keyVal, count)
	if dst == nil {
		log.Println("Could not allocate memory.")
		return nil, errors.New("Could not allocate memory.")
	}

	for i := 0; i < count; i++ {
		kv = new(keyVal)
		if kv == nil {
			log.Println("Could not allocate memory.")
			return nil, errors.New("Could not allocate memory.")
		}
		tmp = binary.LittleEndian.Uint64(srcPlain[i*8 : i*8+8])
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

	// Write merkle root and root of next log
	copy(res[24:24+hashLength], lh.merkleRoot)
	copy(res[24+hashLength:24+2*hashLength], lh.nextRoot)

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
	var combinedHash []byte

	combinedHash = make([]byte, 2*hashLength, 2*hashLength)

	firstLeafIndex = getFirstLeafIndex(hashList)
	for i := firstLeafIndex - 1; i > 0; i-- {
		copy(combinedHash[0:hashLength], hashList[2*i])
		copy(combinedHash[hashLength:2*hashLength], hashList[2*i+1])
		err = hashData(hashList[i], combinedHash)

		if err != nil {
			log.Println("Failure in calculating hash. -> " + err.Error())
			return errors.New("Failure in calculating hash. -> " + err.Error())
		}
	}

	// Finally sign the merkle root.
	err = signMerkleRoot(hashList[0], hashList[1])
	if err != nil {
		log.Println("Failure in signing merkle root. -> " + err.Error())
		return errors.New("Failure in signing merkle root. -> " + err.Error())
	}
	return nil
}

// Function that takes a logHeader and reads the whole merkle tree from
// corresponding log file, into memory.
// TODO This can be improved by selectively reading only the branches
// in the tree that are going to be needed for authenticating a block.
// For each of the Node's parents up to the root, include both children.
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
	if !compareByteSlice(lh.merkleRoot, hashList[1]) {
		return false
	}

	tmpHash = make([]byte, hashLength)
	signMerkleRoot(tmpHash, hashList[1])
	if !compareByteSlice(tmpHash, hashList[0]) {
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

// A helper function that compares two []byte of data for a match.
func compareByteSlice(a, b []byte) bool {
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

// A function that takes a merkle tree, a keyVal and the index of the keyVal,
// and validates the authenticity of the keyVal by calculating and verifying
// all the hashes from the leaf to the root.
func validateBlock(hashList [][]byte, kv *keyVal, index int) bool {
	var tmpHash, combinedHash []byte
	var err error
	var firstLeafIndex int
	var nodeID int
	var toBeHashed []byte

	tmpHash = make([]byte, hashLength, hashLength)
	combinedHash = make([]byte, 2*hashLength, 2*hashLength)

	// First calculate the hash of the keyVal (block|key|flags)
	toBeHashed = packBlockKeyFlags(kv)
	err = hashData(tmpHash, toBeHashed)
	if err != nil {
		return false
	}

	// Compare with existing hash in the tree.
	firstLeafIndex = getFirstLeafIndex(hashList)
	if !compareByteSlice(tmpHash, hashList[firstLeafIndex+index]) {
		return false
	}

	// Validate rest of the tree up to the root.
	// In the case of binary tree, ID of the parent = ID of child / 2
	for nodeID = index / 2; nodeID > 0; nodeID /= 2 {
		// Calculating the hash of the node from its children.
		copy(combinedHash[0:hashLength], hashList[2*nodeID])
		copy(combinedHash[hashLength:2*hashLength], hashList[2*nodeID+1])
		err = hashData(tmpHash, combinedHash)

		if err != nil {
			return false
		}

		// Compare with existing hash in the tree
		if !compareByteSlice(tmpHash, hashList[nodeID]) {
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
	var myKey uint64
	var flags byte

	var tmpData []byte
	var bytesRead int
	var kvList []*keyVal

	// Read the key index table to memory for fast search.
	tmpData = make([]byte, 8*lh.numOfKeys)
	_, err = lh.file.Seek(getIndexOffset(lh), 0)
	if err != nil {
		log.Println("Failure in seek. -> " + err.Error())
		return nil, 0, errors.New("Failure in seek. -> " + err.Error())
	}

	bytesRead, err = lh.file.Read(tmpData)
	if err != nil || bytesRead != len(tmpData) {
		log.Println("Failure in reading index table. -> " + err.Error())
		return nil, 0, errors.New("Failure in reading index table. -> " + err.Error())
	}

	kvList, err = decomposeIndex(tmpData)
	if err != nil {
		log.Println("Could not decompose index table. -> " + err.Error())
		return nil, 0, errors.New("Could not decompose index table. -> " + err.Error())
	}

	// Search the table using binary search, mask/ignore flags.
	start = 0
	end = lh.numOfKeys - 1

	for start <= end {
		index = start + (end-start)/2
		myKey, flags = kvList[index].key, kvList[index].flags
		if kvList[index].key == targetKey {
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
	return lm.namePath + lm.namePrefix + strconv.Itoa(lm.nextFD) + defaultSuffix
}

// Writes a log file at the head.
// The input is a slice of keyVals that MUST be sorted by key.
// During write, the flags are appended to key (1 byte flags + 7 bytes key)
func (lm *logManager) write(kvList []*keyVal) error {
	var err error
	var newHeader *logHeader
	var headerBytes, indexBytes []byte
	var bytesWritten, copiedBytes int
	var emptyBytes int64
	var encryptedBlock *Block
	var hashList [][]byte
	var firstLeafIndex int
	var toBeHashed []byte

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
	} else {
		newHeader.nextRoot = make([]byte, hashLength, hashLength)

	}
	// The new log is created and written, but the head pointer will be
	// updated only after completion of the new file to prevent
	// corruption of the head.

	// Make sure a NEW file is created.
	for checkFileExists(lm.generateName()) {
		lm.nextFD++
	}
	newHeader.file, err = os.Create(lm.generateName())
	if err != nil {
		log.Println("Could not create log file. -> " + err.Error())
		return errors.New("Could not create log file. -> " + err.Error())
	}
	lm.nextFD++
	// The file does not get closed. It is kept open to access it for reads
	// and other methods. It only gets created, written, and synced.

	// Generate the primitive header. The merkle root will be generated later
	// and the header will be updated.

	headerBytes, err = generateHeader(newHeader)
	if err != nil {
		log.Println("Could not generate header. -> " + err.Error())
		return errors.New("Could not generate header. -> " + err.Error())
	}

	// Generate key index table, and merkle tree in memory.
	indexBytes, err = generateIndex(kvList)
	if err != nil {
		log.Println("Could not generate index table. -> " + err.Error())
		return errors.New("Could not generate index table. -> " + err.Error())
	}

	hashList, err = generateEmptyTree(len(kvList))
	firstLeafIndex = getFirstLeafIndex(hashList)
	if err != nil {
		log.Println("Could not generate empty tree. -> " + err.Error())
		return errors.New("Could not generate empty tree. -> " + err.Error())
	}

	// Skip header, index, tree parts in the file to be filled later.
	emptyBytes = int64(len(headerBytes) + len(indexBytes) + len(hashList)*hashLength)
	err = newHeader.file.Truncate(emptyBytes)
	if err != nil {
		log.Println("Could not truncate file to size. -> " + err.Error())
		return errors.New("Could not truncate file to size. -> " + err.Error())
	}
	_, err = newHeader.file.Seek(emptyBytes, 0)
	if err != nil {
		log.Println("Failure in seek. -> " + err.Error())
		return errors.New("Failure in seek. -> " + err.Error())
	}

	// Write data blocks, which consist bulk of the log file.
	// The encrypted version of data blocks are written, and the
	// hash of plain blocks are stored in the merkle tree.
	encryptedBlock = new(Block)
	for i, kv := range kvList {
		err = encryptData(encryptedBlock[:], kv.block[:])
		if err != nil {
			log.Println("Failure in encryption. -> " + err.Error())
			return errors.New("Failure in encryption. -> " + err.Error())
		}
		toBeHashed = packBlockKeyFlags(kv)
		err = hashData(hashList[firstLeafIndex+i], toBeHashed)
		if err != nil {
			log.Println("Failure in calculating hash. -> " + err.Error())
			return errors.New("Failure in calculating hash. -> " + err.Error())
		}
		bytesWritten, err = newHeader.file.Write(encryptedBlock[:])
		if err != nil || bytesWritten != len(encryptedBlock) {
			log.Println("Could not write the encrypted block. -> " + err.Error())
			return errors.New("Could not write the encrypted block. -> " + err.Error())
		}

	}

	// Populate the middle nodes of the merkle tree starting from the
	// leaves up to the root.
	err = updateMerkleTree(hashList)
	if err != nil {
		log.Println("Failure in updating merkle tree. -> " + err.Error())
		return errors.New("Failure in updating merkle tree. -> " + err.Error())
	}

	// Update the merkle root of this logHeader with the calculated root.
	newHeader.merkleRoot = make([]byte, hashLength)
	copiedBytes = copy(newHeader.merkleRoot, hashList[1])
	if copiedBytes != hashLength {
		log.Println("Could not save the merkle root. -> ")
		return errors.New("Could not save the merkle root. -> ")
	}

	// Update the headerBytes with correct merkleRoot.
	headerBytes, err = generateHeader(newHeader)
	if err != nil {
		log.Println("Could not update header. -> " + err.Error())
		return errors.New("Could not update header. -> " + err.Error())
	}

	// Write the correct header, index table, and updated tree in the log file.
	_, err = newHeader.file.Seek(0, 0)
	if err != nil {
		log.Println("Failure in rewind. -> " + err.Error())
		return errors.New("Failure in rewind. -> " + err.Error())
	}

	// Write header.
	bytesWritten, err = newHeader.file.Write(headerBytes)
	if err != nil || bytesWritten != len(headerBytes) {
		log.Println("Could not write the header section. -> " + err.Error())
		return errors.New("Could not write the header section. -> " + err.Error())
	}

	// Write index table.
	bytesWritten, err = newHeader.file.Write(indexBytes)
	if err != nil || bytesWritten != len(indexBytes) {
		log.Println("Could not write the index section. -> " + err.Error())
		return errors.New("Could not write the index section. -> " + err.Error())
	}

	// Write merkle tree.
	for i := 0; i < len(hashList); i++ {
		bytesWritten, err = newHeader.file.Write(hashList[i])
		if err != nil || bytesWritten != len(hashList[i]) {
			log.Println("Could not write the tree section. -> " + err.Error())
			return errors.New("Could not write the tre section. -> " + err.Error())
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
	var tmpBlock *Block

	targetKey, ok = CleanKey(keyIn)
	if !ok {
		log.Println("Key out of range.")
		return nil, errors.New("Key out of range.")
	}

	// Iterate over the chain
	for lh = lm.headLog; lh != nil; lh = lh.nextLog {
		kv, index, err = isKeyInFile(targetKey, lh)
		if err != nil {
			log.Println("Failure in checking file for key. -> " + err.Error())
			return nil, errors.New("Failure in checking file for key. -> " + err.Error())
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

	// Check the merkle root.
	ok = validateMerkleRoot(lh, hashList)
	if !ok {
		log.Println("Could not validate the merkle root.")
		return nil, errors.New("Could not validate the merkle root.")

	}

	// Read the target block.
	tmpBlock = readBlockFromFile(lh, index)
	if tmpBlock == nil {
		log.Println("Failure in reading block from file.")
		return nil, errors.New("Failure in reading block from file.")
	}

	// Decrypt the block.
	kv.block = new(Block)
	if kv.block == nil {
		log.Println("Could not allocate memory.")
		return nil, errors.New("Could not allocate memory.")
	}

	err = decryptData(kv.block[:], tmpBlock[:])
	if err != nil {
		log.Println("Failure in decryption. -> " + err.Error())
		return nil, errors.New("Failure in decryption. -> " + err.Error())
	}

	// Validate the kv in the tree.
	ok = validateBlock(hashList, kv, index)
	if !ok {
		log.Println("Could not validate the keyVal in the merkle tree.")
		return nil, errors.New("Could not validate the keyVal in the merkle tree.")
	}

	return kv, nil

}

// Closes all the handles to the files of a logManager.
// Starts from the head and moves down the chain
// Overwrites the object's head pointer with nil
func (lm *logManager) close() error {
	var err error
	var lh *logHeader
	// Iterate over the chain
	for lh = lm.headLog; lh != nil; lh = lh.nextLog {
		err = lh.file.Close()
		if err != nil {
			log.Println("Failure in closing file. -> " + err.Error())
			return errors.New("Failure in closing file. -> " + err.Error())
		}
	}
	lm.headLog = nil
	return nil
}

// Tries to open an existing chain of log files.
func (lm *logManager) open(path string, prefix string) error {
	var err error
	var chainHead, chainTail *logHeader
	var headerList []*logHeader
	var tmpFD, maxFD int
	var s string

	lm.namePath, lm.namePrefix = cleanPathPrefix(path, prefix)

	headerList, err = readFileHeaders(lm)

	if err != nil {
		log.Println("Error in finding log files.")
		return errors.New("Error in finding finding log files. -> " + err.Error())
	}
	if headerList == nil {
		log.Println("No log files found.")
		return errors.New("No log files found")
	}

	chainHead = headerList[0]
	chainTail = chainHead

	// Simply going over all the list, N times, to form the link.
	// Number of files N is expected to be small.
	// Every loop adding [at least] one link to head or tail.
	for i := 0; i < len(headerList); i++ {
		for j := 0; j < len(headerList); j++ {
			if compareByteSlice(headerList[j].nextRoot, chainHead.merkleRoot) == true {
				headerList[j].nextLog = chainHead
				chainHead = headerList[j]
			} else if compareByteSlice(chainTail.nextRoot, headerList[j].merkleRoot) == true {
				chainTail.nextLog = headerList[j]
				chainTail = headerList[j]
			}

		}
		// Updating the maximum FD number in file names.
		s = headerList[i].file.Name()
		tmpFD, _ = strconv.Atoi(s[len(lm.namePath+lm.namePrefix) : len(s)-4])
		if tmpFD > maxFD {
			maxFD = tmpFD
		}
	}

	lm.nextFD = maxFD
	lm.headLog = chainHead

	return nil
}

// merge() Merges multiple log files into a single one.
// Keeps only the most updated version for each key.
// Discards the keys that have been marked for removal.
// A struct that helps merge() is defined to keep track of a keyVal,
// the log file that it belongs to, and its index within that log file.
// TODO Add ability to not necessarily all of them, but have some
// flexibility. E.g. only merge last N log files.
type mergeHelper struct {
	kv         *keyVal
	logIndex   int
	tableIndex int
}

func (lm *logManager) merge() error {
	var err error
	var lh, newHeader *logHeader
	var logList []*logHeader
	var numOfFiles int
	var kvList [][]*keyVal
	var hashList [][][]byte
	var tmpData []byte
	var bytesRead int
	var listIndex []int
	var ok bool
	var totalKeys int
	var minSel int
	var lastKey, minKey uint64
	var headerBytes, indexBytes []byte
	var bytesWritten, copiedBytes int
	var emptyBytes int64
	var encryptedBlock *Block
	var outHashList [][]byte
	var outKVList []*keyVal
	var firstLeafIndex int
	var toBeHashed []byte

	// OutList contains the list of keyVals to be written, sorted by key.
	// But it does not contain the data block. Instead, the struct holds
	// a pointer to the correct the log file that contains the data block.
	// This way we do not load all the data blocks in memory, and load
	// them one by one from the log files instead.
	var outList []*mergeHelper
	var tmpHelper *mergeHelper

	// Iterate over the chain. logList[0] is the head.
	for lh = lm.headLog; lh != nil; lh = lh.nextLog {
		logList = append(logList, lh)
	}
	numOfFiles = len(logList)
	kvList = make([][]*keyVal, numOfFiles)
	hashList = make([][][]byte, numOfFiles)

	// Read the key index table to memory from each log file
	for i := 0; i < numOfFiles; i++ {
		tmpData = make([]byte, 8*logList[i].numOfKeys)
		totalKeys = totalKeys + logList[i].numOfKeys

		_, err = logList[i].file.Seek(getIndexOffset(logList[i]), 0)
		if err != nil {
			log.Println("Failure in seek. -> " + err.Error())
			return errors.New("Failure in seek. -> " + err.Error())
		}

		bytesRead, err = logList[i].file.Read(tmpData)
		if err != nil || bytesRead != len(tmpData) {
			log.Println("Failure in reading index table. -> " + err.Error())
			return errors.New("Failure in reading index table. -> " + err.Error())
		}

		kvList[i], err = decomposeIndex(tmpData)
		if err != nil {
			log.Println("Could not decompose index table. -> " + err.Error())
			return errors.New("Could not decompose index table. -> " + err.Error())
		}

		// First read the merkle tree
		hashList[i] = readTreeFromFile(logList[i])
		if hashList[i] == nil {
			log.Println("Failure in reading tree from file.")
			return errors.New("Failure in reading tree from file.")
		}

		// Check the merkle root.
		ok = validateMerkleRoot(logList[i], hashList[i])
		if !ok {
			log.Println("Could not validate the merkle root.")
			return errors.New("Could not validate the merkle root.")

		}
	}

	listIndex = make([]int, numOfFiles)

	// Get a sorted struct of mergeHelper.
	// This allows to iterate over all of them, read the data block from correct log file,
	// decrypt and authenticate the block, and write it directly to log file.
	// The datablocks, key, flags, and so HASH of leaves won't change. (right?)

	lastKey = maxKey + 1
	for i := 0; i < totalKeys; i++ {
		// First, select the minimum key and file.
		// Priority is with newer files.
		minKey = maxKey + 1
		for j := 0; j < numOfFiles; j++ {
			// Check to see if there is anything left from this file.
			if listIndex[j] >= len(kvList[j]) {

				continue
			}
			// Note that the comparison should be > and not >=
			// so an older file is not picked in case of a tie.
			if minKey > kvList[j][listIndex[j]].key {
				minKey = kvList[j][listIndex[j]].key
				minSel = j
			}
		}

		if minKey > lastKey || lastKey == maxKey+1 {
			lastKey = minKey

			tmpHelper = new(mergeHelper)
			tmpHelper.kv = kvList[minSel][listIndex[minSel]]
			tmpHelper.logIndex = minSel
			tmpHelper.tableIndex = listIndex[minSel]
			// We onlu need to write this item if it is not marked as removed.
			if tmpHelper.kv.flags&flagRemove != flagRemove {
				outList = append(outList, tmpHelper)
			}
		} else if minKey == lastKey {
			// Similar key, but with older data. Skip.
		} else {
			// Should never happen!
			return errors.New("Failure during merging.")
		}
		listIndex[minSel]++
	}

	// Fist assign a new header file.
	newHeader = new(logHeader)
	if newHeader == nil {
		log.Println("Could not allocate header.")
		return errors.New("Could not allocate header.")
	}
	newHeader.headerVersion = defaultHeaderVersion
	newHeader.numOfKeys = len(outList)

	// Explicitly breaking the chain as we are merging all log files.
	// TODO if not all log files are merged, this should link to the rest.
	newHeader.nextRoot = make([]byte, hashLength, hashLength)

	// The new log is created and written, but the head pointer will be
	// updated only after completion of the new file to prevent
	// corruption of the head.

	// Make sure a NEW file is created.
	for checkFileExists(lm.generateName()) {
		lm.nextFD++
	}
	newHeader.file, err = os.Create(lm.generateName())
	if err != nil {
		log.Println("Could not create log file. -> " + err.Error())
		return errors.New("Could not create log file. -> " + err.Error())
	}
	lm.nextFD++
	// The file does not get closed. It is kept open to access it for reads
	// and other methods. It only gets created, written, and synced.

	// Generate the primitive header. The merkle root will be generated later
	// and the header will be updated.
	headerBytes, err = generateHeader(newHeader)
	if err != nil {
		log.Println("Could not generate header. -> " + err.Error())
		return errors.New("Could not generate header. -> " + err.Error())
	}

	outKVList = make([]*keyVal, len(outList))
	for j := 0; j < len(outList); j++ {
		outKVList[j] = outList[j].kv
	}
	// Generate key index table, and merkle tree in memory.
	indexBytes, err = generateIndex(outKVList)
	if err != nil {
		log.Println("Could not generate index table. -> " + err.Error())
		return errors.New("Could not generate index table. -> " + err.Error())
	}

	outHashList, err = generateEmptyTree(len(outList))
	firstLeafIndex = getFirstLeafIndex(outHashList)
	if err != nil {
		log.Println("Could not generate empty tree. -> " + err.Error())
		return errors.New("Could not generate empty tree. -> " + err.Error())
	}

	// Skip header, index, tree parts in the file to be filled later.
	emptyBytes = int64(len(headerBytes) + len(indexBytes) + len(outHashList)*hashLength)
	err = newHeader.file.Truncate(emptyBytes)
	if err != nil {
		log.Println("Could not truncate file to size. -> " + err.Error())
		return errors.New("Could not truncate file to size. -> " + err.Error())
	}
	_, err = newHeader.file.Seek(emptyBytes, 0)
	if err != nil {
		log.Println("Failure in seek. -> " + err.Error())
		return errors.New("Failure in seek. -> " + err.Error())
	}

	// Write data blocks, which consist bulk of the log file.
	// The encrypted version of data blocks are written, and the
	// plain hash of blocks are stored in the merkle tree.

	for i, item := range outList {

		// Read the target block.
		encryptedBlock = readBlockFromFile(logList[item.logIndex], item.tableIndex)
		if encryptedBlock == nil {
			log.Println("Failure in reading block from file.")
			return errors.New("Failure in reading block from file.")
		}

		// Decrypt the block.
		item.kv.block = new(Block)
		if item.kv.block == nil {
			log.Println("Could not allocate memory.")
			return errors.New("Could not allocate memory.")
		}

		err = decryptData(item.kv.block[:], encryptedBlock[:])
		if err != nil {
			log.Println("Failure in decryption. -> " + err.Error())
			return errors.New("Failure in decryption. -> " + err.Error())
		}

		// Validate the kv in the tree.
		ok = validateBlock(hashList[item.logIndex], item.kv, item.tableIndex)
		if !ok {
			log.Println("Could not validate the keyVal in the merkle tree.")
			return errors.New("Could not validate the keyVal in the merkle tree.")
		}

		toBeHashed = packBlockKeyFlags(item.kv)
		err = hashData(outHashList[firstLeafIndex+i], toBeHashed)
		if err != nil {
			log.Println("Failure in calculating hash. -> " + err.Error())
			return errors.New("Failure in calculating hash. -> " + err.Error())
		}
		bytesWritten, err = newHeader.file.Write(encryptedBlock[:])
		if err != nil || bytesWritten != len(encryptedBlock) {
			log.Println("Could not write the encrypted block. -> " + err.Error())
			return errors.New("Could not write the encrypted block. -> " + err.Error())
		}

	}

	// Populate the middle nodes of the merkle tree starting from the
	// leaves up to the root.
	err = updateMerkleTree(outHashList)
	if err != nil {
		log.Println("Failure in updating merkle tree. -> " + err.Error())
		return errors.New("Failure in updating merkle tree. -> " + err.Error())
	}

	// Update the merkle root of this logHeader with the calculated root.
	newHeader.merkleRoot = make([]byte, hashLength)
	copiedBytes = copy(newHeader.merkleRoot, outHashList[1])
	if copiedBytes != hashLength {
		log.Println("Could not save the merkle root. -> ")
		return errors.New("Could not save the merkle root. -> ")
	}

	// Update the headerBytes with correct merkleRoot.
	headerBytes, err = generateHeader(newHeader)
	if err != nil {
		log.Println("Could not update header. -> " + err.Error())
		return errors.New("Could not update header. -> " + err.Error())
	}

	// Write the correct header, index table, and updated tree in the log file.
	_, err = newHeader.file.Seek(0, 0)
	if err != nil {
		log.Println("Failure in rewind. -> " + err.Error())
		return errors.New("Failure in rewind. -> " + err.Error())
	}

	// Write header.
	bytesWritten, err = newHeader.file.Write(headerBytes)
	if err != nil || bytesWritten != len(headerBytes) {
		log.Println("Could not write the header section. -> " + err.Error())
		return errors.New("Could not write the header section. -> " + err.Error())
	}

	// Write index table.
	bytesWritten, err = newHeader.file.Write(indexBytes)
	if err != nil || bytesWritten != len(indexBytes) {
		log.Println("Could not write the index section. -> " + err.Error())
		return errors.New("Could not write the index section. -> " + err.Error())
	}

	// Write merkle tree.
	for i := 0; i < len(outHashList); i++ {
		bytesWritten, err = newHeader.file.Write(outHashList[i])
		if err != nil || bytesWritten != len(outHashList[i]) {
			log.Println("Could not write the tree section. -> " + err.Error())
			return errors.New("Could not write the tre section. -> " + err.Error())
		}
	}

	// Force flush the system buffer.
	newHeader.file.Sync()

	lm.headLog = newHeader

	// Remove older files.
	for _, lh := range logList {
		lh.file.Close()
		os.Remove(lh.file.Name())
	}

	return nil
}
