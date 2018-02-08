package networkevent

// Config defines network event configuration.
type Config struct {
	LogPath string `yaml:"log_path"`
	Enabled bool   `yaml:"enabled"`
}
