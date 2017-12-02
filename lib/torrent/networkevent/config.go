package networkevent

// Config defines network event configuration.
type Config struct {
	KafkaTopic string `yaml:"kafka_topic"`
	LogPath    string `yaml:"log_path"`
	Enabled    bool   `yaml:"enabled"`
}
