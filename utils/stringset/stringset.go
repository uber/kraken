package stringset

// Set is a nifty little wrapper for common set operations on a map. Because it
// is equivalent to a map, make/range/len will still work with Set.
type Set map[string]struct{}

// FromSlice converts a slice of strings into a Set.
func FromSlice(xs []string) Set {
	s := make(Set)
	for _, x := range xs {
		s.Add(x)
	}
	return s
}

// Add adds x to s.
func (s Set) Add(x string) {
	s[x] = struct{}{}
}

// Remove removes x from s.
func (s Set) Remove(x string) {
	delete(s, x)
}

// Has returns true if x is in s.
func (s Set) Has(x string) bool {
	_, ok := s[x]
	return ok
}

// Equal returns whether s1 and s2 contain the same elements.
func Equal(s1 Set, s2 Set) bool {
	if len(s1) != len(s2) {
		return false
	}
	for x := range s1 {
		if !s2.Has(x) {
			return false
		}
	}
	return true
}
