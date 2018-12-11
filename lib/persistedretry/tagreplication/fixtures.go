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
