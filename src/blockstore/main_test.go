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
	"os"
	"testing"
)

// The initializer for testing.
// It creates a random generator for the test functions.
// It also creates a directory for test log files. if this directory exists,
// it removes previous files.
func TestMain(m *testing.M) {
	if myRand == nil {
		myRand = randomGen()
	}

	// Remove existing test log files.
	os.RemoveAll(dirTest)

	// Create a directory for new files.
	os.Mkdir(dirTest, 0755)

	stat := m.Run()
	os.Exit(stat)
}
