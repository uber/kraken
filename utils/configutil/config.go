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
// Package configutil provides an interface for loading and validating configuration
// data from YAML files.
//
// Other YAML files could be included via the following directive:
//
// production.yaml:
// extends: base.yaml
//
// There is no multiple inheritance supported. Dependency tree suppossed to
// form a directed acyclic graph(DAG).
//
//
// Values from multiple configurations within the same hierarchy are deep merged
//
// Package support multiple configuraton directories potentially having multiple files
// with the the same name. In this the we just follow the path in extends and load all
// the file accroding to the relative directory, i.e configA: base.yaml production.yaml(extends base.yaml), configB: base.yaml the load sequance
// will be the following: configA(base.yaml), configA(production.yaml)
//
// Note regarding configuration merging:
//   Array defined in YAML will be overriden based on load sequence.
//   e.g. in the base.yaml:
//        sports:
//           - football
//        in the development.yaml:
//        extends: base.yaml
//        sports:
//           - basketball
//        after the merge:
//        sports:
//           - basketball  // only keep the latest one
//
//   Map defined in YAML will be merged together based on load sequence.
//   e.g. in the base.yaml:
//        sports:
//           football: true
//        in the development.yaml:
//        extends: base.yaml
//        sports:
//           basketball: true
//        after the merge:
//        sports:  // combine all the map fields
//           football: true
//           basketball: true
//
package configutil

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/uber/kraken/utils/stringset"

	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

const (
	configDirKey = "UBER_CONFIG_DIR"

	configDir       = "config"
	secretsFile     = "secrets.yaml"
	configSeparator = ":"
)

// ErrNoFilesToLoad is returned when you attemp to call LoadFiles without valid
// file paths.
var ErrNoFilesToLoad = errors.New("attempt to load configuration with no files")

// ErrCycleRef is returned when there are circular dependencies detected in
// configuraiton files extending each other.
var ErrCycleRef = errors.New("cyclic reference in configuration extends detected")

// Extends define a keywoword in config for extending a base configuration file.
type Extends struct {
	Extends string `yaml:"extends"`
}

// ValidationError is the returned when a configuration fails to pass
// validation.
type ValidationError struct {
	errorMap validator.ErrorMap
}

// ErrForField returns the validation error for the given field.
func (e ValidationError) ErrForField(name string) error {
	return e.errorMap[name]
}

func (e ValidationError) Error() string {
	var w bytes.Buffer

	fmt.Fprintf(&w, "validation failed")
	for f, err := range e.errorMap {
		fmt.Fprintf(&w, "   %s: %v\n", f, err)
	}

	return w.String()
}

// FilterCandidates filters candidate config files into only the ones that
// exist.
func FilterCandidates(fname string) ([]string, error) {
	realConfigDirs := []string{configDir}
	// Allow overriding the directory config is loaded from, useful for tests
	// inside subdirectories when the config dir is at the top-level of a
	// project.
	if configRoot := os.Getenv(configDirKey); configRoot != "" {
		realConfigDirs = strings.Split(configRoot, configSeparator)
	}
	return filterCandidatesFromDirs(fname, realConfigDirs)
}

func readExtend(configFile string) (string, error) {
	var cfg Extends

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return "", err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return "", fmt.Errorf("unmarshal %s: %s", configFile, err)
	}
	return cfg.Extends, nil
}

func getCandidate(fname string, dirs []string) string {
	candidate := ""
	for _, realConfigDir := range dirs {
		configFile := path.Join(realConfigDir, fname)
		if _, err := os.Stat(configFile); err == nil {
			candidate = configFile
		}
	}
	return candidate
}

func filterCandidatesFromDirs(fname string, dirs []string) ([]string, error) {
	var paths []string
	cSet := make(stringset.Set)

	// Go through all the 'extends' hierarchy until there is no base anymore or
	// some reference cycles have been detected.
	cSet.Add(fname)

	candidate := getCandidate(fname, dirs)
	fmt.Fprintf(os.Stderr, "candidate: %s\n", candidate)
	if candidate == "" {
		return nil, ErrNoFilesToLoad
	}

	for {
		paths = append([]string{candidate}, paths...)
		extends, err := readExtend(candidate)
		if err != nil {
			return nil, err
		} else if extends == "" {
			break
		}

		// Prevent circular references.
		if cSet.Has(extends) {
			return nil, ErrCycleRef
		}

		if !filepath.IsAbs(extends) {
			candidate = path.Join(filepath.Dir(candidate), extends)
		}
		cSet.Add(extends)
	}

	// Append secrets.
	candidate = getCandidate(secretsFile, dirs)
	if candidate != "" {
		paths = append(paths, candidate)
	}

	return paths, nil
}

// Load loads configuration based on config file name.
// If config directory cannot be derived from file name, get it from environment
// variables.
func Load(fname string, config interface{}) error {
	candidates, err := filterCandidatesFromDirs(
		filepath.Base(fname), []string{filepath.Dir(fname)})
	if err != nil && err != ErrNoFilesToLoad {
		return fmt.Errorf("find config under %s: %s", filepath.Dir(fname), err)
	} else if err == ErrNoFilesToLoad {
		candidates, err = FilterCandidates(fname)
		if err != nil {
			return fmt.Errorf(
				"find config under %s and %s: %s", filepath.Dir(fname), configDirKey, err)
		}
	}

	return loadFiles(config, candidates...)
}

// LoadFiles loads a list of files, deep-merging values.
func loadFiles(config interface{}, fnames ...string) error {
	if len(fnames) == 0 {
		return ErrNoFilesToLoad
	}
	for _, fname := range fnames {
		data, err := ioutil.ReadFile(fname)
		if err != nil {
			return err
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return fmt.Errorf("unmarshal %s: %s", fname, err)
		}
	}

	// Validate on the merged config at the end.
	if err := validator.Validate(config); err != nil {
		return ValidationError{
			errorMap: err.(validator.ErrorMap),
		}
	}
	return nil
}
