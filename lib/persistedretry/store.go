package persistedretry

// Task defines an interface for task used in taskstore and workerpool.
type Task interface {
	Run() error
}

// Store contains tasks.
type Store interface {
	GetFailed() ([]Task, error)
	GetPending() ([]Task, error)
	MarkPending(Task) error
	MarkFailed(Task) error
	MarkDone(Task) error
}
