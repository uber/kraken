package randutil

import (
	"fmt"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Text returns randomly generated alphanumeric text of length n.
func Text(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		c := chars[rand.Intn(len(chars))]
		b[i] = byte(c)
	}
	return b
}

// IP returns a randomly generated ip address.
func IP() string {
	return fmt.Sprintf(
		"%d.%d.%d.%d",
		rand.Intn(256),
		rand.Intn(256),
		rand.Intn(256),
		rand.Intn(256))
}

// Port returns a randomly generated port.
func Port() int {
	return rand.Intn(65535) + 1
}
