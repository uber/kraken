package nginx

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
)

// Assumes CWD is set to the project root.
const _configDir = "./nginx/config"

func abspath(name string) (string, error) {
	return filepath.Abs(filepath.Join(_configDir, name))
}

// Run runs nginx configuration. All configuration
func Run(config Config, args map[string]interface{}) error {
	tmpl, err := abspath(config.Template)
	if err != nil {
		return fmt.Errorf("template path: %s", err)
	}
	src, err := populateTemplate(tmpl, args)
	if err != nil {
		return fmt.Errorf("template: %s", err)
	}
	name := strings.TrimSuffix(config.Template, filepath.Ext(tmpl))

	for _, d := range []string{"/etc/nginx/sites-available", "/etc/nginx/sites-enabled"} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return err
		}
		if err := ioutil.WriteFile(filepath.Join(d, name), src, 0755); err != nil {
			return fmt.Errorf("write file: %s", err)
		}
	}

	defaultConfig, err := abspath("default")
	if err != nil {
		return fmt.Errorf("default config path: %s", err)
	}

	cmd := exec.Command(
		"/usr/sbin/nginx",
		"-g", "daemon off;",
		"-c", defaultConfig)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func populateTemplate(tmpl string, args map[string]interface{}) ([]byte, error) {
	b, err := ioutil.ReadFile(tmpl)
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
