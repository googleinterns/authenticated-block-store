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

// This simply tests if a wrong key is rejected/cleaned correctly or not.
func check(t *testing.T, v1 uint64, expect bool) {
	var v2 uint64
	var ok bool
	log.Printf("Key -- TestCleanKey -- Testing with key: 0x%016X (expecting: %t)\n", v1, expect)
	v2, ok = CleanKey(v1)
	if expect == true {
		if !ok || v1 != v2 {
			t.Errorf("Error happened at key=%X , CleanKey(key)=%X\n", v1, v2)
		}
	} else {
		if ok || v1 == v2 {
			t.Errorf("Error happened at key=%X , CleanKey(key)=%X\n", v1, v2)
		}
	}
}

// Checks different keys. Starting from 0, 1, then shifts it by 4 bits until
// reaching the bound. Then goes further and makes sure CleanKey rejects.
func TestCleanKey(t *testing.T) {
	var key uint64

	check(t, 0, true)

	for key = 1; ; key *= 16 {
		if key > maxKey {
			break
		}
		check(t, key, true)
	}
	check(t, maxKey, true)
	check(t, maxKey+1, false)

}
