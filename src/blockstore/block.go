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

// This file implements block primitives.

import (
	"math/rand"
	"time"
)

// Size of a Block can be arbitrary. It is Intended to be 4096 to fit in a 4KB page.
const blockSize = 4096

type Block [blockSize]byte

// Helper function that generates a random block of data.
func GetRandomBlock() *Block {
	var newBlock *Block = new(Block)
	myRand := randomGen()
	for index, _ := range newBlock {
		newBlock[index] = byte(myRand.Intn(256))
	}
	return newBlock
}

func randomGen() *rand.Rand {
	seed := rand.NewSource(time.Now().UnixNano())
	random := rand.New(seed)
	return random
}
