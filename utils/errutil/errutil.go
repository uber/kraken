package errutil

import "bytes"

// MultiError defines a list of multiple errors. Useful for type assertions and
// inspecting individual errors.
//
// XXX: Don't initialize errors as MultiError unless you know what you're doing!
// See https://golang.org/doc/faq#nil_error for more details.
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

// Join converts errs into an error interface.
func Join(errs []error) error {
	if errs == nil {
		return nil
	}
	return MultiError(errs)
}
