package metrics

// Config defines metrics configuration.
type Config struct {
	Backend string       `yaml:"backend"`
	Statsd  StatsdConfig `yaml:"statsd"`
	M3      M3Config     `yaml:"m3"`
}

// StatsdConfig defines statsd configuration.
type StatsdConfig struct {
	HostPort string `yaml:"host_port"`
	Prefix   string `yaml:"prefix"`
}

// M3Config defines m3 configuration.
type M3Config struct {
	HostPort string `yaml:"host_port"`
	Service  string `yaml:"service"`
	Env      string `yaml:"env"`
}
