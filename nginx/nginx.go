package nginx

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"text/template"

	"github.com/uber/kraken/nginx/config"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"
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
	Root         bool   `yaml:"root"`
	Name         string `yaml:"name"`
	TemplatePath string `yaml:"template_path"`
	CacheDir     string `yaml:"cache_dir"`
	LogDir       string `yaml:"log_dir"`

	tls httputil.TLSConfig
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

// GetTemplate returns the template content.
func (c *Config) getTemplate() (string, error) {
	if c.TemplatePath != "" {
		b, err := ioutil.ReadFile(c.TemplatePath)
		if err != nil {
			return "", fmt.Errorf("read template: %s", err)
		}
		return string(b), nil
	}
	tmpl, err := config.GetDefaultTemplate(c.Name)
	if err != nil {
		return "", fmt.Errorf("get default template: %s", err)
	}
	return tmpl, nil
}

// Build builds nginx config.
func (c *Config) Build(params map[string]interface{}) ([]byte, error) {
	tmpl, err := c.getTemplate()
	if err != nil {
		return nil, fmt.Errorf("get template: %s", err)
	}
	site, err := populateTemplate(tmpl, params)
	if err != nil {
		return nil, fmt.Errorf("populate template: %s", err)
	}

	// Build nginx config with base template and component specific template.
	tmpl, err = config.GetDefaultTemplate("base")
	if err != nil {
		return nil, fmt.Errorf("get default base template: %s", err)
	}
	src, err := populateTemplate(tmpl, map[string]interface{}{
		"site":                string(site),
		"ssl_enabled":         !c.tls.CA.Disabled,
		"ssl_certificate":     c.tls.CA.Cert.Path,
		"ssl_certificate_key": c.tls.CA.Key.Path,
		"ssl_password_file":   c.tls.CA.Passphrase.Path,
	})
	if err != nil {
		return nil, fmt.Errorf("populate base: %s", err)
	}
	return src, nil
}

// Option allows setting optional nginx configuration.
type Option func(*Config)

// WithTLS configures nginx configuration with tls.
func WithTLS(tls httputil.TLSConfig) Option {
	return func(c *Config) { c.tls = tls }
}

// Run injects params into an nginx configuration template and runs it.
func Run(config Config, params map[string]interface{}, opts ...Option) error {
	if config.Name == "" && config.TemplatePath == "" {
		return errors.New("invalid config: name or template_path required")
	}
	if config.CacheDir == "" {
		return errors.New("invalid config: cache_dir required")
	}
	if config.LogDir == "" {
		return errors.New("invalid config: log_dir required")
	}
	for _, opt := range opts {
		opt(&config)
	}
	if config.tls.CA.Disabled {
		log.Warn("Server TLS is disabled")
	} else {
		for _, f := range []string{
			config.tls.CA.Cert.Path,
			config.tls.CA.Key.Path,
			config.tls.CA.Passphrase.Path,
		} {
			if _, err := os.Stat(f); err != nil {
				return fmt.Errorf("invalid TLS config: %s", err)
			}
		}
	}

	if err := os.MkdirAll(config.CacheDir, 0775); err != nil {
		return err
	}

	if err := config.inject(params); err != nil {
		return err
	}

	src, err := config.Build(params)
	if err != nil {
		return fmt.Errorf("build nginx config: %s", err)
	}

	if err := os.MkdirAll(_genDir, 0775); err != nil {
		return err
	}
	conf := filepath.Join(_genDir, config.Name)
	if err := ioutil.WriteFile(conf, src, 0755); err != nil {
		return fmt.Errorf("write src: %s", err)
	}

	stdoutLog := path.Join(config.LogDir, "nginx-stdout.log")
	stdout, err := os.OpenFile(stdoutLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open stdout log: %s", err)
	}

	args := []string{"/usr/sbin/nginx", "-g", "daemon off;", "-c", conf}
	if config.Root {
		args = append([]string{"sudo"}, args...)
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = stdout
	cmd.Stderr = stdout
	return cmd.Run()
}

func populateTemplate(tmpl string, args map[string]interface{}) ([]byte, error) {
	t, err := template.New("nginx").Parse(tmpl)
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
