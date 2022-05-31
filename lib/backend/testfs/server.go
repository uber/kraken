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
package testfs

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/uber/kraken/utils/handler"

	"github.com/go-chi/chi"
)

// Server provides HTTP endpoints for operating on files on disk.
type Server struct {
	sync.RWMutex
	dir string
}

// NewServer creates a new Server.
func NewServer() *Server {
	dir, err := ioutil.TempDir("/tmp", "kraken-testfs")
	if err != nil {
		panic(err)
	}
	return &Server{dir: dir}
}

// Handler returns an HTTP handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/health", s.healthHandler)
	r.Head("/files/*", handler.Wrap(s.statHandler))
	r.Get("/files/*", handler.Wrap(s.downloadHandler))
	r.Post("/files/*", handler.Wrap(s.uploadHandler))
	r.Get("/list/*", handler.Wrap(s.listHandler))
	return r
}

// Cleanup cleans up the underlying directory of s.
func (s *Server) Cleanup() {
	os.RemoveAll(s.dir)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func (s *Server) statHandler(w http.ResponseWriter, r *http.Request) error {
	s.RLock()
	defer s.RUnlock()

	name := r.URL.Path[len("/files/"):]

	info, err := os.Stat(s.path(name))
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("file store: %s", err)
	}
	w.Header().Add("Size", strconv.FormatInt(info.Size(), 10))
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) downloadHandler(w http.ResponseWriter, r *http.Request) error {
	s.RLock()
	defer s.RUnlock()

	name := r.URL.Path[len("/files/"):]

	f, err := os.Open(s.path(name))
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("open: %s", err)
	}
	if _, err := io.Copy(w, f); err != nil {
		return handler.Errorf("copy: %s", err)
	}
	return nil
}

func (s *Server) uploadHandler(w http.ResponseWriter, r *http.Request) error {
	s.Lock()
	defer s.Unlock()

	name := r.URL.Path[len("/files/"):]

	p := s.path(name)
	if err := os.MkdirAll(filepath.Dir(p), 0775); err != nil {
		return handler.Errorf("mkdir: %s", err)
	}
	f, err := os.Create(p)
	if err != nil {
		return handler.Errorf("create: %s", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, r.Body); err != nil {
		return handler.Errorf("copy: %s", err)
	}
	return nil
}

func (s *Server) listHandler(w http.ResponseWriter, r *http.Request) error {
	s.RLock()
	defer s.RUnlock()

	prefix := s.path(r.URL.Path[len("/list/"):])

	var paths []string
	err := filepath.Walk(prefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if path == prefix {
			// Handle case where prefix is a file.
			return nil
		}
		path, err = filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk: %s", err)
	}
	if err := json.NewEncoder(w).Encode(&paths); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}

// path normalizes some file or directory entry into a path.
func (s *Server) path(entry string) string {
	// Allows listing tags by repo.
	entry = strings.Replace(entry, ":", "/", -1)
	return filepath.Join(s.dir, entry)
}
