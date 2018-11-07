package memsize

import (
	"fmt"
)

// Defines number of bits in each bit unit.
const (
	bit uint64 = 1 << (10 * iota)
	Kbit
	Mbit
	Gbit
	Tbit
)

// Defines number of bytes in each byte unit.
const (
	B uint64 = 1 << (10 * iota)
	KB
	MB
	GB
	TB
)

type unit struct {
	val uint64
	str string
}

func format(units []unit, n uint64) (string, bool) {
	for _, u := range units {
		if n >= u.val {
			f := float64(n) / float64(u.val)
			return fmt.Sprintf("%.2f%s", f, u.str), true
		}
	}
	return "", false
}

// Format returns a human readable representation for the given number of bytes.
func Format(bytes uint64) string {
	units := []unit{
		{TB, "TB"},
		{GB, "GB"},
		{MB, "MB"},
		{KB, "KB"},
		{B, "B"},
	}
	s, ok := format(units, bytes)
	if !ok {
		s = "0B"
	}
	return s
}

// BitFormat returns a human readable representation for the given number of bits.
func BitFormat(bits uint64) string {
	units := []unit{
		{Tbit, "Tbit"},
		{Gbit, "Gbit"},
		{Mbit, "Mbit"},
		{Kbit, "Kbit"},
		{1, "bit"},
	}
	s, ok := format(units, bits)
	if !ok {
		s = "0bit"
	}
	return s
}
