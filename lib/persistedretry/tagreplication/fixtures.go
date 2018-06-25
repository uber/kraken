package tagreplication

import (
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/randutil"
)

// TaskFixture creates a fixture of tagreplication.Task.
func TaskFixture() *Task {
	tag := core.TagFixture()
	d := core.DigestFixture()
	dest := fmt.Sprintf("build-index-%s", randutil.Hex(8))
	return NewTask(tag, d, core.DigestListFixture(3), dest)
}
