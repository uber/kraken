package sqlbackend

// Config is used to initialize the SQL Backend Client
type Config struct {
	DebugLogging     bool   `yaml:"debug_logging"`
	Dialect          string `yaml:"dialect"`
	ConnectionString string `yaml:"connection_string"`
	Username         string `yaml:"username"`
}

// UserAuthConfig defines authentication configuration overlayed by Langley/Vault.
// Each key is the iam username of the credentials.
type UserAuthConfig map[string]AuthConfig

// SQL is a struct that holds credentials. This is declared here to make testing easier
type SQL struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// AuthConfig matches Langley format.
type AuthConfig struct {
	SQL SQL `yaml:"sql"`
}
