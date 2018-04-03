package randutil

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func choose(n uint64, choices string) []byte {
	b := make([]byte, n)
	for i := range b {
		c := choices[rand.Intn(len(choices))]
		b[i] = byte(c)
	}
	return b
}

const text = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Text returns randomly generated alphanumeric text of length n.
func Text(n uint64) []byte {
	return choose(n, text)
}

// Blob returns randomly generated blob data of length n.
func Blob(n uint64) []byte {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	lr := io.LimitReader(r, int64(n))
	b, _ := ioutil.ReadAll(lr)

	return b
}

const hex = "0123456789abcdef"

// Hex returns randomly generated hexadecimal string of length n.
func Hex(n uint64) string {
	return string(choose(n, hex))
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

// ShuffleInts shuffles the values of xs in place.
func ShuffleInts(xs []int) {
	for i := range xs {
		j := rand.Intn(i + 1)
		xs[i], xs[j] = xs[j], xs[i]
	}
}

// ShuffleInt64s shuffles the values of xs in place.
func ShuffleInt64s(xs []int64) {
	for i := range xs {
		j := rand.Intn(i + 1)
		xs[i], xs[j] = xs[j], xs[i]
	}
}

// Bools returns a list of randomly generated bools of length n.
func Bools(n int) []bool {
	b := make([]bool, n)
	for i := range b {
		b[i] = rand.Intn(2) == 1
	}
	return b
}
