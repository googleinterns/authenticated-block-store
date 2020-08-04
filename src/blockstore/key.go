package blockstore

// This file implements key primitives.

// The key is 7 least significcant bytes.
// The highmost byte is reserved for flags.
const flagMask = uint64(0xFF) << 56
const keyMask = ^(uint64(0xFF) << 56)
const maxKey = ^(uint64(0xFF) << 56)

// Making sure the highest byte is zeroed.
func CleanKey(keyIn uint64) (uint64, bool) {
	return keyIn & keyMask, (keyIn >> 56) <= 0
}
