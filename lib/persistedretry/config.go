package persistedretry

import "time"

// Config defines Manager configuration.
type Config struct {
	NumWorkers        int           `yaml:"num_workers"`
	NumRetryWorkers   int           `yaml:"num_retry_workers"`
	TaskChanSize      int           `yaml:"task_chan_size"`
	RetryChanSize     int           `yaml:"retry_chan_size"`
	TaskInterval      time.Duration `yaml:"task_interval"`
	RetryInterval     time.Duration `yaml:"retry_interval"`
	RetryTaskInterval time.Duration `yaml:"retry_task_interval"`

	// Flags that zero-value channel sizes should not have defaults applied.
	Testing bool
}

func (c Config) applyDefaults() Config {
	if c.NumWorkers == 0 {
		c.NumWorkers = 6
	}
	if c.NumRetryWorkers == 0 {
		c.NumRetryWorkers = 1
	}
	if c.TaskInterval == 0 {
		c.TaskInterval = 10 * time.Millisecond
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = 5 * time.Minute
	}
	if c.RetryTaskInterval == 0 {
		c.RetryTaskInterval = 5 * time.Second
	}
	if !c.Testing {
		if c.TaskChanSize == 0 {
			c.TaskChanSize = 1000
		}
		if c.RetryChanSize == 0 {
			c.RetryChanSize = 1000
		}
	}
	return c
}
