package peerhandoutpolicy

// Config defines configuration for the peer handout policy.
type Config struct {
	Priority string `yaml:"priority"`
	Sampling string `yaml:"sampling"`
}
