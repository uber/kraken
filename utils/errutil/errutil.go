package errutil

import "bytes"

// MultiError defines a list of multiple errors.
type MultiError []error

func (e MultiError) Error() string {
	var b bytes.Buffer
	for i, err := range e {
		b.WriteString(err.Error())
		if i < len(e)-1 {
			b.WriteString(", ")
		}
	}
	return b.String()
}
