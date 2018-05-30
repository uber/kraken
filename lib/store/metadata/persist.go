package metadata

import (
	"regexp"
	"strconv"
)

const _persistSuffix = "_persist"

func init() {
	Register(regexp.MustCompile(_persistSuffix), &persistFactory{})
}

type persistFactory struct{}

func (f persistFactory) Create(suffix string) Metadata {
	return &Persist{}
}

// Persist marks whether a blob should be persisted.
type Persist struct {
	Value bool
}

// NewPersist creates a new Persist, where true means the blob
// should be persisted, and false means the blob is safe to delete.
func NewPersist(v bool) *Persist {
	return &Persist{v}
}

// GetSuffix returns a static suffix.
func (m *Persist) GetSuffix() string {
	return _persistSuffix
}

// Movable is true.
func (m *Persist) Movable() bool {
	return true
}

// Serialize converts m to bytes.
func (m *Persist) Serialize() ([]byte, error) {
	return []byte(strconv.FormatBool(m.Value)), nil
}

// Deserialize loads b into m.
func (m *Persist) Deserialize(b []byte) error {
	v, err := strconv.ParseBool(string(b))
	if err != nil {
		return err
	}
	m.Value = v
	return nil
}
