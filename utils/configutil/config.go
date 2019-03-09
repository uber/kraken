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
// form a linked list.
//
//
// Values from multiple configurations within the same hierarchy are deep merged
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
	"path"
	"path/filepath"

	"github.com/uber/kraken/utils/stringset"

	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

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

// Error implements the `error` interface.
func (e ValidationError) Error() string {
	var w bytes.Buffer

	fmt.Fprintf(&w, "validation failed")
	for f, err := range e.errorMap {
		fmt.Fprintf(&w, "   %s: %v\n", f, err)
	}

	return w.String()
}

// Load loads configuration based on config file name. It will
// follow extends directives and do a deep merge of those config
// files.
func Load(filename string, config interface{}) error {
	filenames, err := resolveExtends(filename, readExtend)
	if err != nil {
		return err
	}
	return loadFiles(config, filenames)
}

type getExtend func(filename string) (extends string, err error)

// resolveExtends returns the list of config paths that the original config `filename`
// points to.
func resolveExtends(filename string, extendReader getExtend) ([]string, error) {
	filenames := []string{filename}
	seen := make(stringset.Set)
	for {
		extends, err := extendReader(filename)
		if err != nil {
			return nil, err
		} else if extends == "" {
			break
		}

		// If the file path of the extends field in the config is not absolute
		// we assume that it is in the same directory as the current config
		// file.
		if !filepath.IsAbs(extends) {
			extends = path.Join(filepath.Dir(filename), extends)
		}

		// Prevent circular references.
		if seen.Has(extends) {
			return nil, ErrCycleRef
		}

		filenames = append([]string{extends}, filenames...)
		seen.Add(extends)
		filename = extends
	}
	return filenames, nil
}

func readExtend(configFile string) (string, error) {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return "", err
	}

	var cfg Extends
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return "", fmt.Errorf("unmarshal %s: %s", configFile, err)
	}
	return cfg.Extends, nil
}

// loadFiles loads a list of files, deep-merging values.
func loadFiles(config interface{}, fnames []string) error {
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
