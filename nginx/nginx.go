package nginx

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/alecthomas/template"
)

// Run runs nginx configuration.
func Run(config Config, args map[string]interface{}) error {
	src, err := populateTemplate(config.Template, args)
	if err != nil {
		return fmt.Errorf("template: %s", err)
	}
	for _, d := range []string{"/etc/nginx/sites-available", "/etc/nginx/sites-enabled"} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return err
		}
		if err := ioutil.WriteFile(filepath.Join(d, config.Name), src, 0755); err != nil {
			return fmt.Errorf("write file: %s", err)
		}
	}
	cmd := exec.Command("/usr/sbin/nginx", "-g", "daemon off;")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func populateTemplate(tmplFilename string, args map[string]interface{}) ([]byte, error) {
	b, err := ioutil.ReadFile(tmplFilename)
	if err != nil {
		return nil, fmt.Errorf("read: %s", err)
	}
	tmpl, err := template.New("nginx").Parse(string(b))
	if err != nil {
		return nil, fmt.Errorf("parse: %s", err)
	}
	out := &bytes.Buffer{}
	if err := tmpl.Execute(out, args); err != nil {
		return nil, fmt.Errorf("exec: %s", err)
	}
	return out.Bytes(), nil
}
