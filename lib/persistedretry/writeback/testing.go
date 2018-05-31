package writeback

import (
	"reflect"
	"time"
)

// TaskMatcher is a gomock Matcher which matches two tasks.
type TaskMatcher struct {
	task Task
}

// MatchTask returns a new TaskMatcher
func MatchTask(task *Task) *TaskMatcher {
	return &TaskMatcher{*task}
}

// Matches compares two tasks. It ignores checking for time.
func (m *TaskMatcher) Matches(x interface{}) bool {
	expected := m.task
	result := *(x.(*Task))

	expected.CreatedAt = time.Time{}
	result.CreatedAt = time.Time{}
	expected.LastAttempt = time.Time{}
	result.LastAttempt = time.Time{}

	return reflect.DeepEqual(expected, result)
}

// String returns the name of the matcher.
func (m *TaskMatcher) String() string {
	return "TaskMatcher"
}
