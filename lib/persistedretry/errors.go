package persistedretry

import "errors"

// Store errors.
var (
	ErrTaskExists   = errors.New("task already exists in store")
	ErrTaskNotFound = errors.New("task not found")
)
