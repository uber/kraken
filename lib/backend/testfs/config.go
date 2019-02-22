package testfs

// Config defines Client configuration.
type Config struct {
	Addr     string `yaml:"addr"`
	Root     string `yaml:"root"`
	NamePath string `yaml:"name_path"`
}
