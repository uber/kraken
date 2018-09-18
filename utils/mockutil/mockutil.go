package mockutil

import "regexp"

// RegexMatcher is a gomock Matcher which matches strings against some
// given regex.
type RegexMatcher struct {
	expected *regexp.Regexp
}

// MatchRegex returns a new RegexMatcher which matches the given regex.
func MatchRegex(expr string) *RegexMatcher {
	return &RegexMatcher{regexp.MustCompile(expr)}
}

// Matches returns true if x is a string which matches the expected regex.
func (m *RegexMatcher) Matches(x interface{}) bool {
	s, ok := x.(string)
	if !ok {
		return false
	}
	return m.expected.MatchString(s)
}

func (m *RegexMatcher) String() string {
	return m.expected.String()
}
