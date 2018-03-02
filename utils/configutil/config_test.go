package configutil

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
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
    - somewhere-sjc1:8090
    - somewhere-else-sjc1:8010
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
	f, err := ioutil.TempFile("", "configtest")
	require.NoError(t, err)

	defer f.Close()

	_, err = f.Write([]byte(contents))
	require.NoError(t, err)

	return f.Name()
}

func TestLoadFile(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	var cfg configuration
	err := LoadFile(fname, &cfg)
	require.NoError(t, err)
	assert.Equal(t, "localhost:4385", cfg.ListenAddress)
	assert.Equal(t, 1024, cfg.BufferSpace)
	assert.Equal(t, []string{"somewhere-sjc1:8090", "somewhere-else-sjc1:8010"}, cfg.Servers)
}

func TestLoadFilesExtends(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	partialConfig := "buffer_space: 8080"
	partial := writeFile(t, partialConfig)
	defer os.Remove(partial)

	var cfg configuration
	err := LoadFiles(&cfg, fname, partial)
	require.NoError(t, err)

	assert.Equal(t, 8080, cfg.BufferSpace)
	assert.Equal(t, "localhost:4385", cfg.ListenAddress)
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
      - somewhere-else-sjc1:8010
    `

	fname1 := writeFile(t, invalidConfig1)
	defer os.Remove(fname1)

	fname2 := writeFile(t, invalidConfig2)
	defer os.Remove(invalidConfig2)

	// Either config by itself will not pass validation.
	var cfg1 configuration
	err := LoadFile(fname1, &cfg1)
	require.Error(t, err)

	verr, ok := err.(ValidationError)
	require.True(t, ok)

	assert.Equal(t, validator.ErrorArray{validator.ErrZeroValue}, verr.ErrForField("ListenAddress"))
	assert.Equal(t, validator.ErrorArray{validator.ErrZeroValue}, verr.ErrForField("Servers"))

	var cfg2 configuration
	err = LoadFile(fname2, &cfg2)
	require.Error(t, err)

	verr, ok = err.(ValidationError)
	require.True(t, ok)

	assert.Equal(t, validator.ErrorArray{validator.ErrMin}, verr.ErrForField("BufferSpace"))

	// But merging load has no error.
	var mergedCfg configuration
	err = LoadFiles(&mergedCfg, fname1, fname2)
	require.NoError(t, err)

	assert.Equal(t, "localhost:8080", mergedCfg.ListenAddress)
	assert.Equal(t, 256, mergedCfg.BufferSpace)
	assert.Equal(t, []string{"somewhere-else-sjc1:8010"}, mergedCfg.Servers)
}

func TestMissingFile(t *testing.T) {
	var cfg configuration
	err := LoadFile("./no-config.yaml", &cfg)
	require.Error(t, err)
}

func TestInvalidYAML(t *testing.T) {
	var cfg configuration
	err := LoadFile("./config_test.go", &cfg)
	require.Error(t, err)
}

func TestInvalidConfig(t *testing.T) {
	fname := writeFile(t, invalidConfig)
	defer os.Remove(fname)

	var cfg configuration
	err := LoadFile(fname, &cfg)
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
		assert.Equal(t, errs, fieldErr)
	}
}

func TestExtendsConfig(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(fname))
	extendsfn := writeFile(t, extends)
	defer os.Remove(extendsfn)

	var cfg configuration
	err := LoadFile(extendsfn, &cfg)
	require.NoError(t, err)

	assert.Equal(t, "localhost:4385", cfg.ListenAddress)
	assert.Equal(t, 512, cfg.BufferSpace)
	assert.Equal(t, []string{"somewhere-sjc2:8090", "somewhere-else-sjc2:8010"}, cfg.Servers)
	assert.Equal(t, "v1", cfg.X.Y.Z.K1)
	assert.Equal(t, "v2", cfg.X.Y.Z.K2)

	assert.Equal(t, "val1", cfg.X.Y.V)
}

func TestExtendsConfigDeep(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(fname))
	extendsfn := writeFile(t, extends)
	defer os.Remove(extendsfn)

	extends2 := fmt.Sprintf(goodYetAnotherExtendsConfig, filepath.Base(extends))
	extendsfn2 := writeFile(t, extends2)
	defer os.Remove(extendsfn2)

	var cfg configuration
	err := LoadFile(extendsfn2, &cfg)
	require.NoError(t, err)

	assert.Equal(t, "localhost:4385", cfg.ListenAddress)
	assert.Equal(t, 256, cfg.BufferSpace)
	assert.Equal(t, []string{"somewhere-sjc3:8090", "somewhere-else-sjc3:8010"}, cfg.Servers)
}

func TestExtendsConfigCircularRef(t *testing.T) {
	f1, err := ioutil.TempFile("", "configtest")
	require.NoError(t, err)

	f2, err := ioutil.TempFile("", "configtest")
	require.NoError(t, err)

	f3, err := ioutil.TempFile("", "configtest")
	require.NoError(t, err)

	defer f1.Close()
	defer f2.Close()
	defer f3.Close()

	_, err = f1.Write([]byte(goodConfig))
	require.NoError(t, err)
	defer os.Remove(f1.Name())

	extends := fmt.Sprintf(goodExtendsConfig, filepath.Base(f3.Name()))
	_, err = f2.Write([]byte(extends))
	require.NoError(t, err)

	defer os.Remove(f2.Name())

	extends2 := fmt.Sprintf(goodYetAnotherExtendsConfig, filepath.Base(f2.Name()))
	_, err = f3.Write([]byte(extends2))
	require.NoError(t, err)

	defer os.Remove(f3.Name())

	var cfg configuration
	err = LoadFile(f3.Name(), &cfg)
	require.Error(t, err)
	require.Equal(t, err, errors.New("cyclic reference in configuration extends detected"))
}

func populateTestDir(t *testing.T, dirname, filename, contents string) string {
	tmp, err := ioutil.TempDir("", dirname)
	require.NoError(t, err)
	tmpfn := filepath.Join(tmp, filename)
	err = ioutil.WriteFile(tmpfn, []byte(contents), 0666)
	require.NoError(t, err)
	return tmp
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
				"testdata/multiple/configB/production-dca1.yaml"},
		},
		{
			ConfigDir: "./testdata/multiple/configA:./testdata/multiple/configB:./random/",
			Result: []string{
				"testdata/multiple/configB/base.yaml",
				"testdata/multiple/configB/production.yaml",
				"testdata/multiple/configB/production-dca1.yaml"},
		},
		{
			ConfigDir: "./testdata/multiple/configA:",
			Result: []string{
				"testdata/multiple/configA/base.yaml",
				"testdata/multiple/configA/production.yaml",
				"testdata/multiple/configA/production-dca1.yaml"},
		},
	}

	for _, tt := range tests {
		os.Setenv(configDirKey, tt.ConfigDir)
		files, err := FilterCandidates("production-dca1.yaml")
		require.NoError(t, err)
		assert.Equal(t, tt.Result, files, "config files mismatch for %s", tt.ConfigDir)
	}
}

func TestLoadSingleConfigDir(t *testing.T) {
	defer stubEnv(configDirKey, "testdata/single")()

	config := &configuration{}
	err := Load("test.yaml", config)
	require.NoError(t, err, "failed to load config from %v", os.Getenv(configDirKey))

	assert.Equal(t, &configuration{
		ListenAddress: "127.0.0.1:8080",
		BufferSpace:   9000,
		Servers:       []string{"127.0.0.1:01", "127.0.0.1:02"},
		Secret:        "shh",
	}, config, "configs mismatch for %s", os.Getenv(configDirKey))
}

func loadConfiguration(t *testing.T) *configuration {
	config := &configuration{}
	err := Load("production-dca1.yaml", config)
	require.NoError(t, err, "failed to load config from %v", os.Getenv(configDirKey))
	return config
}

func loadMultipleConfigDir(t *testing.T, dir string) *configuration {
	defer stubEnv(configDirKey, dir)()

	return loadConfiguration(t)
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
					"configBProdDCA1": "nodeB",
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
					"configAProdDCA1": "nodeA",
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
					"configCProdDCA1": "nodeC",
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
					"configAProdDCA1": "nodeA",
				},
			},
		},
		{
			ConfigDir: "testdata/multiple/configA:testdata/configFullD", // one file contains single config attribute and one file contains whole config attributes
			Configuration: configuration{
				ListenAddress: "configFullDProdDCA:03",
				BufferSpace:   3000,
				Servers:       []string{"configFullDProdDCA:03", "configFullDProdDCA:04"},
			},
		},
		{
			ConfigDir: "testdata/configFullD:testdata/multiple/configA", // one file contains single config attribute and one file contains whole config attributes
			Configuration: configuration{
				ListenAddress: "127.0.0.1:1",
				BufferSpace:   1000,
				Servers:       []string{"127.0.0.1:01"},
				Nodes: map[string]string{
					"configAProdDCA1": "nodeA",
				},
			},
		},
	}

	for _, tt := range tests {
		config := loadMultipleConfigDir(t, tt.ConfigDir)
		assert.Equal(t, &tt.Configuration, config, "configs mismatch for %s", tt.ConfigDir)
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
				ListenAddress: "configFullEProdDCA:03",
				BufferSpace:   6000,
				Servers:       []string{"configFullEProdDCA:03", "configFullEProdDCA:04"},
			},
		},
		{
			ConfigDir: "testdata/configFullE:testdata/configFullD",
			Configuration: configuration{
				ListenAddress: "configFullDProdDCA:03",
				BufferSpace:   3000,
				Servers:       []string{"configFullDProdDCA:03", "configFullDProdDCA:04"},
			},
		},
	}

	for _, tt := range tests {
		config := loadMultipleConfigDir(t, tt.ConfigDir)
		assert.Equal(t, &tt.Configuration, config, "configs mismatch for %s", tt.ConfigDir)
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
					"configGProdDCA1": "nodeG",
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
					"configFProdDCA1": "nodeF",
				},
			},
		},
	}

	for _, tt := range tests {
		config := loadMultipleConfigDir(t, tt.ConfigDir)
		assert.Equal(t, &tt.Configuration, config, "configs mismatch for %s", tt.ConfigDir)
	}
}
