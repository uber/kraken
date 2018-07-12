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

const (
	_sitesAvailable = "/etc/nginx/sites-available"
	_sitesEnabled   = "/etc/nginx/sites-enabled"
)

func abspath(name string) (string, error) {
	return filepath.Abs(filepath.Join(_configDir, name))
}

// Run runs nginx configuration. templateName is relative to _configDir.
func Run(templateName string, args map[string]interface{}) error {
	templatePath, err := abspath(templateName)
	if err != nil {
		return fmt.Errorf("template path: %s", err)
	}
	src, err := populateTemplate(templatePath, args)
	if err != nil {
		return fmt.Errorf("template: %s", err)
	}
	name := strings.TrimSuffix(templateName, filepath.Ext(templateName))

	if err := addToSites(name, src); err != nil {
		return err
	}

	config, err := abspath("default")
	if err != nil {
		return fmt.Errorf("default config path: %s", err)
	}

	cmd := exec.Command(
		"/usr/sbin/nginx",
		"-g", "daemon off;",
		"-c", config)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func populateTemplate(templatePath string, args map[string]interface{}) ([]byte, error) {
	b, err := ioutil.ReadFile(templatePath)
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

func addToSites(name string, src []byte) error {
	available := filepath.Join(_sitesAvailable, name)
	if err := os.MkdirAll(_sitesAvailable, 0755); err != nil {
		return err
	}
	if err := ioutil.WriteFile(available, src, 0755); err != nil {
		return fmt.Errorf("write file: %s", err)
	}

	enabled := filepath.Join(_sitesEnabled, name)
	if err := os.MkdirAll(_sitesEnabled, 0755); err != nil {
		return err
	}
	return os.Symlink(available, enabled)
}
