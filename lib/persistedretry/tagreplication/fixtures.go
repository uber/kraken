// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package tagreplication

import (
	"fmt"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/randutil"
)

// TaskFixture creates a fixture of tagreplication.Task.
func TaskFixture() *Task {
	tag := core.TagFixture()
	d := core.DigestFixture()
	dest := fmt.Sprintf("build-index-%s", randutil.Hex(8))
	return NewTask(tag, d, core.DigestListFixture(3), dest, 0)
}
