package storage

import "bytes"

// Bitfield contains the completeness status for all pieces
type Bitfield []bool

// FormatBitfield returns a string represention of the bitfield
func (b Bitfield) String() string {
	buf := new(bytes.Buffer)
	for _, v := range b {
		if v {
			buf.WriteRune('1')
		} else {
			buf.WriteRune('0')
		}
	}
	return buf.String()
}
