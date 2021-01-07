package shadowbackend

// Config is used to initialize the Shadow client
type Config struct {
	ActiveClientConfig map[string]interface{} `yaml:"active_backend"`
	ShadowClientConfig map[string]interface{} `yaml:"shadow_backend"`
}
