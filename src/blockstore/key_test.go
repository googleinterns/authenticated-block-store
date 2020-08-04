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
