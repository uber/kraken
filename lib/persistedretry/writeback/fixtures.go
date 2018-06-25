package writeback

import (
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/randutil"
)

// TaskFixture returns a randomly generated Task for testing purposes.
func TaskFixture() *Task {
	return NewTask(
		fmt.Sprintf("namespace-%s", randutil.Hex(8)),
		core.DigestFixture().Hex())
}
