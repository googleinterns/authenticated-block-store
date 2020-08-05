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

// This simply tests if a random block is generated with the correct size.
func TestRandomBlock(t *testing.T) {
	var testBlock *Block
	testBlock = GetRandomBlock()

	if testBlock == nil {
		t.Error("Block was not generated.")
	}
	log.Printf("Block -- TestRandomBlock -- Random block generated with size = %d", len(testBlock))

	if len(testBlock) != blockSize {
		t.Error("Block was generated with wrong size.")
	}
}
