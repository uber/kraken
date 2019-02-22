package writeback

// NameQuery queries writeback tasks which match a name.
type NameQuery struct {
	name string
}

// NewNameQuery returns a new NameQuery.
func NewNameQuery(name string) *NameQuery {
	return &NameQuery{name}
}
