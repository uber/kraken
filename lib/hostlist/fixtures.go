package hostlist

// Fixture returns a static list of addrs for testing purposes.
func Fixture(addrs ...string) List {
	l, err := New(Config{Static: addrs})
	if err != nil {
		panic(err)
	}
	return l
}
