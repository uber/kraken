package scheduler

import "bytes"

func formatBitfield(bitfield []bool) string {
	b := new(bytes.Buffer)
	for _, v := range bitfield {
		if v {
			b.WriteRune('1')
		} else {
			b.WriteRune('0')
		}
	}
	return b.String()
}
