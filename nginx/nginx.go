// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
	_genDir = "/tmp/nginx"
)

var _clientCABundle = path.Join(_genDir, "ca.crt")

// Config defines nginx configuration.
type Config struct {
	Binary string `yaml:"binary"`

	Root bool `yaml:"root"`

	// Name defines the default nginx template for each component.
	Name string `yaml:"name"`

	// TemplatePath takes precedence over Name, overwrites default template.
	TemplatePath string `yaml:"template_path"`

	CacheDir string `yaml:"cache_dir"`

	LogDir string `yaml:"log_dir"`

	// Optional log path overrides.
	StdoutLogPath string `yaml:"stdout_log_path"`
	AccessLogPath string `yaml:"access_log_path"`
	ErrorLogPath  string `yaml:"error_log_path"`

	tls httputil.TLSConfig
}

func (c *Config) applyDefaults() error {
	if c.Binary == "" {
		c.Binary = "/usr/sbin/nginx"
	}
	if c.StdoutLogPath == "" {
		if c.LogDir == "" {
			return errors.New("one of log_dir or stdout_log_path must be set")
		}
		c.StdoutLogPath = filepath.Join(c.LogDir, "nginx-stdout.log")
	}
	if c.AccessLogPath == "" {
		if c.LogDir == "" {
			return errors.New("one of log_dir or access_log_path must be set")
		}
		c.AccessLogPath = filepath.Join(c.LogDir, "nginx-access.log")
	}
	if c.ErrorLogPath == "" {
		if c.LogDir == "" {
			return errors.New("one of log_dir or error_log_path must be set")
		}
		c.ErrorLogPath = filepath.Join(c.LogDir, "nginx-error.log")
	}
	return nil
}

func (c *Config) inject(params map[string]interface{}) error {
	for _, s := range []string{"cache_dir", "access_log_path", "error_log_path"} {
		if _, ok := params[s]; ok {
			return fmt.Errorf("invalid params: %s is reserved", s)
		}
	}
	params["cache_dir"] = c.CacheDir
	params["access_log_path"] = c.AccessLogPath
	params["error_log_path"] = c.ErrorLogPath
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
	if _, ok := params["client_verification"]; !ok {
		params["client_verification"] = config.DefaultClientVerification
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
		"site":                   string(site),
		"ssl_enabled":            !c.tls.Server.Disabled,
		"ssl_certificate":        c.tls.Server.Cert.Path,
		"ssl_certificate_key":    c.tls.Server.Key.Path,
		"ssl_password_file":      c.tls.Server.Passphrase.Path,
		"ssl_client_certificate": _clientCABundle,
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
	if err := config.applyDefaults(); err != nil {
		return fmt.Errorf("invalid config: %s", err)
	}
	if config.Name == "" && config.TemplatePath == "" {
		return errors.New("invalid config: name or template_path required")
	}
	if config.CacheDir == "" {
		return errors.New("invalid config: cache_dir required")
	}
	for _, opt := range opts {
		opt(&config)
	}

	// Create root directory for generated files for nginx.
	if err := os.MkdirAll(_genDir, 0775); err != nil {
		return err
	}

	if config.tls.Server.Disabled {
		log.Warn("Server TLS is disabled")
	} else {
		for _, s := range append(
			config.tls.CAs,
			config.tls.Server.Cert,
			config.tls.Server.Key,
			config.tls.Server.Passphrase) {
			if _, err := os.Stat(s.Path); err != nil {
				return fmt.Errorf("invalid TLS config: %s", err)
			}
		}

		// Concat all ca files into bundle.
		cabundle, err := os.Create(_clientCABundle)
		if err != nil {
			return fmt.Errorf("create cabundle: %s", err)
		}
		if err := config.tls.WriteCABundle(cabundle); err != nil {
			return fmt.Errorf("write cabundle: %s", err)
		}
		cabundle.Close()
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

	conf := filepath.Join(_genDir, config.Name)
	if err := ioutil.WriteFile(conf, src, 0755); err != nil {
		return fmt.Errorf("write src: %s", err)
	}

	stdout, err := os.OpenFile(config.StdoutLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open stdout log: %s", err)
	}

	args := []string{config.Binary, "-g", "daemon off;", "-c", conf}
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
