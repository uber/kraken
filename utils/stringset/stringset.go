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

// New creates a new Set with xs.
func New(xs ...string) Set {
	return FromSlice(xs)
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

// Sub returns a new set which is the result of s minus s2.
func (s Set) Sub(s2 Set) Set {
	result := make(Set)
	for x := range s {
		if !s2.Has(x) {
			result.Add(x)
		}
	}
	return result
}

// ToSlice converts s to a slice.
func (s Set) ToSlice() []string {
	var xs []string
	for x := range s {
		xs = append(xs, x)
	}
	return xs
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
