package networkevent

// Config defines network event configuration.
type Config struct {
	KafkaTopic string `yaml:"kafka_topic"`
	Enabled    bool   `yaml:"enabled"`
}
