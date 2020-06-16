## Code style guidelines

This project adheres to the official Go style guidelines: https://github.com/golang/go/wiki/CodeReviewComments

Some things we are very picky about:

#### Variable naming

Please read: https://talks.golang.org/2014/names.slide#1

#### Comments

All comments, regardless of location, must have proper grammar, including proper capitalization and punctuation.

```go
// this is a bad comment with bad grammar

// This is a good comment with good grammar.
```

Avoid comments that state the obvious or repeat code logic. Comments can easily get out of sync with
the code and can mislead rather than help.

#### Line length

All lines of code should be kept under 100 characters.

Comments should be kept under 80 characters.

#### Breaking up long lines

Long function signatures should be broken up like so:

```go
func Foo(
    bar int,
    baz bool,
    blah []int) (string, error) {
  
    ...
}
```
 
And callsites:
 
```go
// If it fits on a 2nd line:
x, err := Foo(
    1, false, []int{1, 2, 3})
    
// If it doesn't:
y, err := Foo(
    1,
    false,
    []int{1, 2, 3, ...})
```

#### Whitespace

Be conservative with adding blank lines between blocks of code. Avoid cluttering vertical screen space
with blank lines when the code reads just fine without them.

Bad:

```go
func (s *State) DeletePending(peerID core.PeerID, h core.InfoHash) {
	k := connKey{peerID, h}
  
	if !s.pending[k] {
		return
	}
  
	delete(s.pending, k)
  
	s.capacity[k.infoHash]++

	s.log("peer", peerID, "hash", h).Infof(
		"Deleted pending conn, capacity now at %d", s.capacity[k.infoHash])
}
```

Good:

```go
func (s *State) DeletePending(peerID core.PeerID, h core.InfoHash) {
	k := connKey{peerID, h}
	if !s.pending[k] {
		return
	}
	delete(s.pending, k)
	s.capacity[k.infoHash]++

	s.log("peer", peerID, "hash", h).Infof(
		"Deleted pending conn, capacity now at %d", s.capacity[k.infoHash])
}
```
