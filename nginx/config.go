package nginx

// Config defines nginx configuration.
type Config struct {
	Name     string `yaml:"name"`
	Template string `yaml:"template"`
}
