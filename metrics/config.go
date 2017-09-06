package metrics

// Config defines metrics configuration.
type Config struct {
	Backend string        `yaml:"type"`
	Statsd  StatsdConfig  `yaml:"statsd"`
	Default DefaultConfig `yaml:"default"`
}

// StatsdConfig defines statsd configuration.
type StatsdConfig struct {
	HostPort string `yaml:"host_port"`
	Prefix   string `yaml:"prefix"`
}

// DefaultConfig defines default configuration.
type DefaultConfig struct{}
