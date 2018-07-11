package nginx

// Config defines nginx configuration.
type Config struct {
	// Template is the name of config template to populate, relative to the
	// ./nginx/config directory.
	Template string `yaml:"template"`
}
