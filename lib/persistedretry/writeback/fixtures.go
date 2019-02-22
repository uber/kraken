package writeback

import (
	"fmt"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/randutil"
)

// TaskFixture returns a randomly generated Task for testing purposes.
func TaskFixture() *Task {
	return NewTask(
		fmt.Sprintf("namespace-%s", randutil.Hex(8)),
		core.DigestFixture().Hex(), 0)
}
