package nginx

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"
)

const (
	// Assumes CWD is set to the project root.
	_configDir = "./nginx/config"

	_genDir = "/tmp/nginx"
)

func abspath(name string) (string, error) {
	return filepath.Abs(filepath.Join(_configDir, name))
}

// Config defines nginx configuration.
type Config struct {
	Name     string `yaml:"name"`
	CacheDir string `yaml:"cache_dir"`
	LogDir   string `yaml:"log_dir"`
}

func (c *Config) inject(params map[string]interface{}) error {
	for _, s := range []string{"cache_dir", "log_dir"} {
		if _, ok := params[s]; ok {
			return fmt.Errorf("invalid params: %s is reserved", s)
		}
	}
	params["cache_dir"] = c.CacheDir
	params["log_dir"] = c.LogDir
	return nil
}

// Run injects params into an nginx configuration template and runs it.
func Run(config Config, params map[string]interface{}) error {
	if config.Name == "" {
		return errors.New("invalid config: name required")
	}
	if config.CacheDir == "" {
		return errors.New("invalid config: cache_dir required")
	}
	if config.LogDir == "" {
		return errors.New("invalid config: log_dir required")
	}

	if err := os.MkdirAll(config.CacheDir, 0775); err != nil {
		return err
	}

	if err := config.inject(params); err != nil {
		return err
	}

	site, err := populateTemplate(config.Name, params)
	if err != nil {
		return fmt.Errorf("populate site: %s", err)
	}
	src, err := populateTemplate("base", map[string]interface{}{
		"site": string(site),
	})
	if err != nil {
		return fmt.Errorf("populate base: %s", err)
	}

	if err := os.MkdirAll(_genDir, 0775); err != nil {
		return err
	}
	conf := filepath.Join(_genDir, config.Name)
	if err := ioutil.WriteFile(conf, src, 0755); err != nil {
		return fmt.Errorf("write src: %s", err)
	}

	cmd := exec.Command(
		"/usr/sbin/nginx",
		"-g", "daemon off;",
		"-c", conf)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func populateTemplate(name string, args map[string]interface{}) ([]byte, error) {
	p, err := abspath(name + ".tmpl")
	if err != nil {
		return nil, err
	}
	b, err := ioutil.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("read: %s", err)
	}
	t, err := template.New("nginx").Parse(string(b))
	if err != nil {
		return nil, fmt.Errorf("parse: %s", err)
	}
	out := &bytes.Buffer{}
	if err := t.Execute(out, args); err != nil {
		return nil, fmt.Errorf("exec: %s", err)
	}
	return out.Bytes(), nil
}

// GetServer returns a string for an nginx server directive value.
func GetServer(net, addr string) string {
	if net == "unix" {
		return "unix:" + addr
	}
	return addr
}
