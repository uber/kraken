package hostlist

// Fixture returns a static list of names for testing purposes. Defaults to port 80.
func Fixture(names ...string) List {
	l, err := New(Config{Static: names})
	if err != nil {
		panic(err)
	}
	return l
}
