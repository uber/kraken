package backend

// ManagerFixture returns a Manager with no clients for testing purposes.
func ManagerFixture() *Manager {
	m, err := NewManager(nil, AuthConfig{})
	if err != nil {
		panic(err)
	}
	return m
}
