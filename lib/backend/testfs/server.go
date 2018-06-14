package testfs

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/httputil"

	"github.com/pressly/chi"
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
	r.Head("/files/:name", handler.Wrap(s.statHandler))
	r.Get("/files/:name", handler.Wrap(s.downloadHandler))
	r.Post("/files/:name", handler.Wrap(s.uploadHandler))
	r.Get("/dir/:dir", handler.Wrap(s.listHandler))
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

	name, err := httputil.ParseParam(r, "name")
	if err != nil {
		return err
	}
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

	name, err := httputil.ParseParam(r, "name")
	if err != nil {
		return err
	}
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

	name, err := httputil.ParseParam(r, "name")
	if err != nil {
		return err
	}
	p := s.path(name)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
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

	dir, err := httputil.ParseParam(r, "dir")
	if err != nil {
		return err
	}
	infos, err := ioutil.ReadDir(s.path(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return handler.Errorf("read dir: %s", err)
	}
	var names []string
	for _, info := range infos {
		names = append(names, info.Name())
	}
	if err := json.NewEncoder(w).Encode(&names); err != nil {
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
