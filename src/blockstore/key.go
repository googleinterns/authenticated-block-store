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
