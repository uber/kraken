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
package configutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/validator.v2"
)

const (
	goodConfig = `
listen_address: localhost:4385
buffer_space: 1024
X:
  Y:
    V: val1
    Z:
      K1: v1
servers:
    - somewhere-zone1:8090
    - somewhere-else-zone1:8010
`

	invalidConfig = `
listen_address:
buffer_space: 1
servers:
`
	goodExtendsConfig = `
extends: %s
buffer_space: 512
X:
  Y:
    Z:
      K2: v2
servers:
    - somewhere-sjc2:8090
    - somewhere-else-sjc2:8010
`
	goodYetAnotherExtendsConfig = `
extends: %s
buffer_space: 256
servers:
    - somewhere-sjc3:8090
    - somewhere-else-sjc3:8010
`
)

type configuration struct {
	ListenAddress string   `yaml:"listen_address" validate:"nonzero"`
	BufferSpace   int      `yaml:"buffer_space" validate:"min=255"`
	Servers       []string `validate:"nonzero"`
	X             Xconfig  `yaml:"X"`
	Nodes         map[string]string
	Secret        string
}

type Xconfig struct {
	Y Yconfig `yaml:"Y"`
}

type Yconfig struct {
	V string  `yaml:"V"`
	Z Zconfig `yaml:"Z"`
}

type Zconfig struct {
	K1 string `yaml:"K1"`
	K2 string `yaml:"K2"`
}

func writeFile(t *testing.T, contents string) string {
	require := require.New(t)

	f, err := ioutil.TempFile("", "configtest")
	require.NoError(err)

	defer f.Close()

	_, err = f.Write([]byte(contents))
	require.NoError(err)

	return f.Name()
}

func TestLoad(t *testing.T) {
	require := require.New(t)

	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	var cfg configuration
	err := Load(fname, &cfg)
	require.NoError(err)
	require.Equal("localhost:4385", cfg.ListenAddress)
	require.Equal(1024, cfg.BufferSpace)
	require.Equal([]string{"somewhere-zone1:8090", "somewhere-else-zone1:8010"}, cfg.Servers)
}

func TestLoadFilesExtends(t *testing.T) {
	require := require.New(t)

	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	partialConfig := "buffer_space: 8080"
	partial := writeFile(t, partialConfig)
	defer os.Remove(partial)

	var cfg configuration
	err := loadFiles(&cfg, fname, partial)
	require.NoError(err)

	require.Equal(8080, cfg.BufferSpace)
	require.Equal("localhost:4385", cfg.ListenAddress)
}

func TestLoadFilesValidateOnce(t *testing.T) {
	require := require.New(t)

	const invalidConfig1 = `
    listen_address:
    buffer_space: 256
    servers:
    `

	const invalidConfig2 = `
    listen_address: "localhost:8080"
    servers:
      - somewhere-else-zone1:8010
    `

	fname1 := writeFile(t, invalidConfig1)
	defer os.Remove(fname1)

	fname2 := writeFile(t, invalidConfig2)
	defer os.Remove(invalidConfig2)

	// Either config by itself will not pass validation.
	var cfg1 configuration
	err := Load(fname1, &cfg1)
	require.Error(err)

	verr, ok := err.(ValidationError)
	require.True(ok)
	require.NotEmpty(verr.Error())

	require.Equal(validator.ErrorArray{validator.ErrZeroValue}, verr.ErrForField("ListenAddress"))
	require.Equal(validator.ErrorArray{validator.ErrZeroValue}, verr.ErrForField("Servers"))

	var cfg2 configuration
	err = Load(fname2, &cfg2)
	require.Error(err)

	verr, ok = err.(ValidationError)
	require.True(ok)
	require.NotEmpty(verr.Error())

	require.Equal(validator.ErrorArray{validator.ErrMin}, verr.ErrForField("BufferSpace"))

	// But merging load has no error.
	var mergedCfg configuration
	err = loadFiles(&mergedCfg, fname1, fname2)
	require.NoError(err)

	require.Equal("localhost:8080", mergedCfg.ListenAddress)
	require.Equal(256, mergedCfg.BufferSpace)
	require.Equal([]string{"somewhere-else-zone1:8010"}, mergedCfg.Servers)
}

func TestMissingFile(t *testing.T) {
	require := require.New(t)

	var cfg configuration
	err := Load("./no-config.yaml", &cfg)
	require.Error(err)
}

func TestInvalidYAML(t *testing.T) {
	require := require.New(t)

	var cfg configuration
	err := Load("./config_test.go", &cfg)
	require.Error(err)
}

func TestInvalidConfig(t *testing.T) {
	require := require.New(t)

	fname := writeFile(t, invalidConfig)
	defer os.Remove(fname)

	var cfg configuration
	err := Load(fname, &cfg)
	require.Error(err)

	verr, ok := err.(ValidationError)
	require.True(ok)

	errors := map[string]validator.ErrorArray{
		"BufferSpace":   {validator.ErrMin},
		"ListenAddress": {validator.ErrZeroValue},
		"Servers":       {validator.ErrZeroValue},
	}

	for field, errs := range errors {
		fieldErr := verr.ErrForField(field)
		require.NotNil(t, fieldErr, "Could not find field level error for %s", field)
		require.Equal(errs, fieldErr)
	}
}

func TestExtendsConfig(t *testing.T) {
	require := require.New(t)

	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(fname))
	extendsfn := writeFile(t, extends)
	defer os.Remove(extendsfn)

	var cfg configuration
	err := Load(extendsfn, &cfg)
	require.NoError(err)

	require.Equal("localhost:4385", cfg.ListenAddress)
	require.Equal(512, cfg.BufferSpace)
	require.Equal([]string{"somewhere-sjc2:8090", "somewhere-else-sjc2:8010"}, cfg.Servers)
	require.Equal("v1", cfg.X.Y.Z.K1)
	require.Equal("v2", cfg.X.Y.Z.K2)

	require.Equal("val1", cfg.X.Y.V)
}

func TestExtendsConfigDeep(t *testing.T) {
	require := require.New(t)

	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(fname))
	extendsfn := writeFile(t, extends)
	defer os.Remove(extendsfn)

	extends2 := fmt.Sprintf(goodYetAnotherExtendsConfig, filepath.Base(extends))
	extendsfn2 := writeFile(t, extends2)
	defer os.Remove(extendsfn2)

	var cfg configuration
	err := Load(extendsfn2, &cfg)
	require.NoError(err)

	require.Equal("localhost:4385", cfg.ListenAddress)
	require.Equal(256, cfg.BufferSpace)
	require.Equal([]string{"somewhere-sjc3:8090", "somewhere-else-sjc3:8010"}, cfg.Servers)
}

func TestExtendsConfigCircularRef(t *testing.T) {
	require := require.New(t)

	f1, err := ioutil.TempFile("", "configtest")
	require.NoError(err)

	f2, err := ioutil.TempFile("", "configtest")
	require.NoError(err)

	f3, err := ioutil.TempFile("", "configtest")
	require.NoError(err)

	defer f1.Close()
	defer f2.Close()
	defer f3.Close()

	_, err = f1.Write([]byte(goodConfig))
	require.NoError(err)
	defer os.Remove(f1.Name())

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(f3.Name()))
	_, err = f2.Write([]byte(extends))
	require.NoError(err)

	defer os.Remove(f2.Name())

	extends2 := fmt.Sprintf(goodYetAnotherExtendsConfig, filepath.Base(f2.Name()))
	_, err = f3.Write([]byte(extends2))
	require.NoError(err)

	defer os.Remove(f3.Name())

	var cfg configuration
	err = Load(f3.Name(), &cfg)
	require.Error(err)
	require.Contains(err.Error(), "cyclic reference in configuration extends detected")
}

func stubEnv(key, value string) func() {
	old := os.Getenv(key)
	os.Setenv(key, value)
	return func() {
		os.Setenv(key, old)
	}
}

func TestDefaultConfigFilesWithMultiplyConfigDir(t *testing.T) {
	defer stubEnv(configDirKey, "config")()

	tests := []struct {
		ConfigDir string
		Result    []string
	}{
		{
			ConfigDir: "testdata/multiple/configA:testdata/multiple/configB",
			Result: []string{
				"testdata/multiple/configB/base.yaml",
				"testdata/multiple/configB/production.yaml",
				"testdata/multiple/configB/production-zone2.yaml"},
		},
		{
			ConfigDir: "./testdata/multiple/configA:./testdata/multiple/configB:./random/",
			Result: []string{
				"testdata/multiple/configB/base.yaml",
				"testdata/multiple/configB/production.yaml",
				"testdata/multiple/configB/production-zone2.yaml"},
		},
		{
			ConfigDir: "./testdata/multiple/configA:",
			Result: []string{
				"testdata/multiple/configA/base.yaml",
				"testdata/multiple/configA/production.yaml",
				"testdata/multiple/configA/production-zone2.yaml"},
		},
	}

	for _, tt := range tests {
		require := require.New(t)

		os.Setenv(configDirKey, tt.ConfigDir)
		files, err := FilterCandidates("production-zone2.yaml")
		require.NoError(err)
		require.Equal(tt.Result, files, "config files mismatch for %s", tt.ConfigDir)
	}
}

func TestLoadSingleConfigDir(t *testing.T) {
	require := require.New(t)

	defer stubEnv(configDirKey, "testdata/single")()

	config := &configuration{}
	err := Load("test.yaml", config)
	require.NoError(err, "failed to load config from %v", os.Getenv(configDirKey))

	require.Equal(&configuration{
		ListenAddress: "127.0.0.1:8080",
		BufferSpace:   9000,
		Servers:       []string{"127.0.0.1:01", "127.0.0.1:02"},
		Secret:        "shh",
	}, config, "configs mismatch for %s", os.Getenv(configDirKey))
}

func loadMultipleConfigDir(require *require.Assertions, dir string) *configuration {
	defer stubEnv(configDirKey, dir)()

	config := &configuration{}
	err := Load("production-zone2.yaml", config)
	require.NoError(err, "failed to load config from %v", os.Getenv(configDirKey))
	return config
}

func TestLoadMultipleConfigDir(t *testing.T) {
	tests := []struct {
		ConfigDir     string
		Configuration configuration
	}{
		{
			ConfigDir: "testdata/multiple/configA:testdata/multiple/configB",
			Configuration: configuration{
				ListenAddress: "127.0.0.1:2",
				BufferSpace:   2000,
				Servers:       []string{"127.0.0.1:02"}, // each file contains single config attribute
				Nodes: map[string]string{ // map in each config file will be merged
					"configBProdZone2": "nodeB",
				},
			},
		},
		{
			ConfigDir: "testdata/multiple/configB:testdata/multiple/configA", // each file contains single config attribute
			Configuration: configuration{
				ListenAddress: "127.0.0.1:1",
				BufferSpace:   1000,
				Servers:       []string{"127.0.0.1:01"},
				Nodes: map[string]string{
					"configAProdZone2": "nodeA",
				},
			},
		},
		{
			ConfigDir: "testdata/multiple/configA:testdata/multiple/configB:testdata/multiple/configC", // each file contains single config attribute
			Configuration: configuration{
				ListenAddress: "127.0.0.1:3",
				BufferSpace:   3000,
				Servers:       []string{"127.0.0.1:03"},
				Nodes: map[string]string{
					"configCProdZone2": "nodeC",
				},
			},
		},
		{
			ConfigDir: "testdata/multiple/configB:testdata/multiple/configC:testdata/multiple/configA", // each file contains single config attribute
			Configuration: configuration{
				ListenAddress: "127.0.0.1:1",
				BufferSpace:   1000,
				Servers:       []string{"127.0.0.1:01"},
				Nodes: map[string]string{
					"configAProdZone2": "nodeA",
				},
			},
		},
		{
			ConfigDir: "testdata/multiple/configA:testdata/configFullD", // one file contains single config attribute and one file contains whole config attributes
			Configuration: configuration{
				ListenAddress: "configFullDProdZone2:03",
				BufferSpace:   3000,
				Servers:       []string{"configFullDProdZone2:03", "configFullDProdZone2:04"},
			},
		},
		{
			ConfigDir: "testdata/configFullD:testdata/multiple/configA", // one file contains single config attribute and one file contains whole config attributes
			Configuration: configuration{
				ListenAddress: "127.0.0.1:1",
				BufferSpace:   1000,
				Servers:       []string{"127.0.0.1:01"},
				Nodes: map[string]string{
					"configAProdZone2": "nodeA",
				},
			},
		},
	}

	for _, tt := range tests {
		require := require.New(t)

		config := loadMultipleConfigDir(require, tt.ConfigDir)
		require.Equal(&tt.Configuration, config, "configs mismatch for %s", tt.ConfigDir)
	}
}

func TestLoadMultipleConfigDirPriorityFullConflict(t *testing.T) {
	tests := []struct {
		ConfigDir     string
		Configuration configuration
	}{
		{
			ConfigDir: "testdata/configFullD:testdata/configFullE",
			Configuration: configuration{
				ListenAddress: "configFullEProdZone2:03",
				BufferSpace:   6000,
				Servers:       []string{"configFullEProdZone2:03", "configFullEProdZone2:04"},
			},
		},
		{
			ConfigDir: "testdata/configFullE:testdata/configFullD",
			Configuration: configuration{
				ListenAddress: "configFullDProdZone2:03",
				BufferSpace:   3000,
				Servers:       []string{"configFullDProdZone2:03", "configFullDProdZone2:04"},
			},
		},
	}

	for _, tt := range tests {
		require := require.New(t)

		config := loadMultipleConfigDir(require, tt.ConfigDir)
		require.Equal(&tt.Configuration, config, "configs mismatch for %s", tt.ConfigDir)
	}
}

func TestLoadMultipleConfigDirPriorityRelativeExtends(t *testing.T) {
	tests := []struct {
		ConfigDir     string
		Configuration configuration
	}{
		{
			ConfigDir: "testdata/multiple/configF:testdata/multiple/configG",
			Configuration: configuration{
				ListenAddress: "127.0.0.1:1",
				BufferSpace:   1000,
				Servers:       []string{"127.0.0.1:05"},
				Nodes: map[string]string{
					"configGProdZone2": "nodeG",
				},
			},
		},
		{
			ConfigDir: "testdata/multiple/configG:testdata/multiple/configF",
			Configuration: configuration{
				ListenAddress: "127.0.0.1:1",
				BufferSpace:   1000,
				Servers:       []string{"127.0.0.1:04"},
				Nodes: map[string]string{
					"configFProdZone2": "nodeF",
				},
			},
		},
	}

	for _, tt := range tests {
		require := require.New(t)

		config := loadMultipleConfigDir(require, tt.ConfigDir)
		require.Equal(&tt.Configuration, config, "configs mismatch for %s", tt.ConfigDir)
	}
}
