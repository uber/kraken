package memsize

// Defines the number of bytes in each unit.
const (
	B uint64 = 1 << (10 * iota)
	KB
	MB
	GB
	TB
)
