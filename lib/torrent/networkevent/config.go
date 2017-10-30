package networkevent

// Config defines network event configuration.
type Config struct {
	KafkaTopic string `yaml:"kafka_topic"`
	Enable     bool   `yaml:"enable"`
}
