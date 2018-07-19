package nginx

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"
)

// Assumes CWD is set to the project root.
const _configDir = "./nginx/config"

func abspath(name string) (string, error) {
	return filepath.Abs(filepath.Join(_configDir, name))
}

// Config defines nginx configuration.
type Config struct {
	Name     string `yaml:"name"`
	CacheDir string `yaml:"cache_dir"`
}

// Run runs nginx configuration.
func Run(config Config, port int) error {
	if err := os.MkdirAll(config.CacheDir, 0775); err != nil {
		return err
	}

	site, err := populateTemplate(config.Name, map[string]interface{}{
		"cache_dir": config.CacheDir,
		"port":      port,
	})
	if err != nil {
		return fmt.Errorf("populate site: %s", err)
	}
	src, err := populateTemplate("base", map[string]interface{}{
		"site": string(site),
	})
	if err != nil {
		return fmt.Errorf("populate base: %s", err)
	}

	conf := filepath.Join("/etc/nginx", config.Name)
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
