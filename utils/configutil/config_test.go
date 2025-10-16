// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package configutil

import (
	"fmt"
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
	f, err := os.CreateTemp("", "configtest")
	require.NoError(t, err)

	_, err = f.Write([]byte(contents))
	require.NoError(t, err)

	require.NoError(t, f.Close())

	return f.Name()
}

func TestLoad(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	var cfg configuration
	err := Load(fname, &cfg)
	require.NoError(t, err)
	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 1024, cfg.BufferSpace)
	require.Equal(t, []string{"somewhere-zone1:8090", "somewhere-else-zone1:8010"}, cfg.Servers)
}

func TestLoadFilesExtends(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	partialConfig := "buffer_space: 8080"
	partial := writeFile(t, partialConfig)
	defer func() {
		require.NoError(t, os.Remove(partial))
	}()

	var cfg configuration
	err := loadFiles(&cfg, []string{fname, partial})
	require.NoError(t, err)

	require.Equal(t, 8080, cfg.BufferSpace)
	require.Equal(t, "localhost:4385", cfg.ListenAddress)
}

func TestLoadFilesValidateOnce(t *testing.T) {
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
	defer func() {
		require.NoError(t, os.Remove(fname1))
	}()

	fname2 := writeFile(t, invalidConfig2)
	defer func() {
		require.NoError(t, os.Remove(fname2))
	}()

	// Either config by itself will not pass validation.
	var cfg1 configuration
	err := Load(fname1, &cfg1)
	require.Error(t, err)

	verr, ok := err.(ValidationError)
	require.True(t, ok)
	require.NotEmpty(t, verr.Error())

	require.Equal(t, validator.ErrorArray{validator.ErrZeroValue}, verr.ErrForField("ListenAddress"))
	require.Equal(t, validator.ErrorArray{validator.ErrZeroValue}, verr.ErrForField("Servers"))

	var cfg2 configuration
	err = Load(fname2, &cfg2)
	require.Error(t, err)

	verr, ok = err.(ValidationError)
	require.True(t, ok)
	require.NotEmpty(t, verr.Error())

	require.Equal(t, validator.ErrorArray{validator.ErrMin}, verr.ErrForField("BufferSpace"))

	// But merging load has no error.
	var mergedCfg configuration
	err = loadFiles(&mergedCfg, []string{fname1, fname2})
	require.NoError(t, err)

	require.Equal(t, "localhost:8080", mergedCfg.ListenAddress)
	require.Equal(t, 256, mergedCfg.BufferSpace)
	require.Equal(t, []string{"somewhere-else-zone1:8010"}, mergedCfg.Servers)
}

func TestMissingFile(t *testing.T) {
	var cfg configuration
	err := Load("./no-config.yaml", &cfg)
	require.Error(t, err)
}

func TestInvalidYAML(t *testing.T) {
	var cfg configuration
	err := Load("./config_test.go", &cfg)
	require.Error(t, err)
}

func TestInvalidConfig(t *testing.T) {
	fname := writeFile(t, invalidConfig)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	var cfg configuration
	err := Load(fname, &cfg)
	require.Error(t, err)

	verr, ok := err.(ValidationError)
	require.True(t, ok)

	errors := map[string]validator.ErrorArray{
		"BufferSpace":   {validator.ErrMin},
		"ListenAddress": {validator.ErrZeroValue},
		"Servers":       {validator.ErrZeroValue},
	}

	for field, errs := range errors {
		fieldErr := verr.ErrForField(field)
		require.NotNil(t, fieldErr, "Could not find field level error for %s", field)
		require.Equal(t, errs, fieldErr)
	}
}

func TestExtendsConfig(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(fname))
	extendsfn := writeFile(t, extends)
	defer func() {
		require.NoError(t, os.Remove(extendsfn))
	}()

	var cfg configuration
	err := Load(extendsfn, &cfg)
	require.NoError(t, err)

	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 512, cfg.BufferSpace)
	require.Equal(t, []string{"somewhere-sjc2:8090", "somewhere-else-sjc2:8010"}, cfg.Servers)
	require.Equal(t, "v1", cfg.X.Y.Z.K1)
	require.Equal(t, "v2", cfg.X.Y.Z.K2)

	require.Equal(t, "val1", cfg.X.Y.V)
}

func TestExtendsConfigDeep(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(fname))
	extendsfn := writeFile(t, extends)
	defer func() {
		require.NoError(t, os.Remove(extendsfn))
	}()

	extends2 := fmt.Sprintf(goodYetAnotherExtendsConfig, filepath.Base(extends))
	extendsfn2 := writeFile(t, extends2)
	defer func() {
		require.NoError(t, os.Remove(extendsfn2))
	}()

	var cfg configuration
	err := Load(extendsfn2, &cfg)
	require.NoError(t, err)

	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 256, cfg.BufferSpace)
	require.Equal(t, []string{"somewhere-sjc3:8090", "somewhere-else-sjc3:8010"}, cfg.Servers)
}

func TestExtendsConfigCircularRef(t *testing.T) {
	f1, err := os.CreateTemp("", "configtest")
	require.NoError(t, err)

	f2, err := os.CreateTemp("", "configtest")
	require.NoError(t, err)

	f3, err := os.CreateTemp("", "configtest")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, f1.Close())
	}()
	defer func() {
		require.NoError(t, f2.Close())
	}()
	defer func() {
		require.NoError(t, f3.Close())
	}()

	_, err = f1.Write([]byte(goodConfig))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(f1.Name()))
	}()

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(f3.Name()))
	_, err = f2.Write([]byte(extends))
	require.NoError(t, err)

	defer func() {
		require.NoError(t, os.Remove(f2.Name()))
	}()

	extends2 := fmt.Sprintf(goodYetAnotherExtendsConfig, filepath.Base(f2.Name()))
	_, err = f3.Write([]byte(extends2))
	require.NoError(t, err)

	defer func() {
		require.NoError(t, os.Remove(f3.Name()))
	}()

	var cfg configuration
	err = Load(f3.Name(), &cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cyclic reference in configuration extends detected")
}

func TestResolveExtends(t *testing.T) {
	tests := []struct {
		fpath    string
		extends  map[string]string
		expected []string
		err      error
	}{
		{
			fpath:    "/configs/c1",
			extends:  map[string]string{},
			expected: []string{"/configs/c1"},
		},
		{
			fpath:    "/configs/c1",
			extends:  map[string]string{"/configs/c1": "/configs/c2"},
			expected: []string{"/configs/c2", "/configs/c1"},
		},
		{
			fpath:    "/configs/c1",
			extends:  map[string]string{"/configs/c1": "c2"},
			expected: []string{"/configs/c2", "/configs/c1"},
		},
		{
			fpath:    "/configs/c1",
			extends:  map[string]string{"/configs/c1": "c2", "/configs/c2": "c1"},
			expected: nil,
			err:      ErrCycleRef,
		},
		{
			fpath:    "/configs/c1",
			extends:  map[string]string{"/configs/c1": "/etc/c2", "/etc/c2": "c3"},
			expected: []string{"/etc/c3", "/etc/c2", "/configs/c1"},
		},
	}

	for _, tt := range tests {
		fn := func(filename string) (string, error) {
			target, found := tt.extends[filename]
			if !found {
				return "", nil
			}
			return target, nil
		}
		filenames, err := resolveExtends(tt.fpath, fn)
		require.Equal(t, tt.err, err)
		require.Equal(t, tt.expected, filenames)
	}
}
