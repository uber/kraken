package testfs

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/handler"

	"github.com/docker/distribution/uuid"
	"github.com/pressly/chi"
)

// Server provides HTTP upload / download endpoints around a file store.
type Server struct {
	fs      store.FileStore
	cleanup func()
}

// NewServer creates a new Server.
func NewServer() *Server {
	fs, cleanup := store.LocalFileStoreFixture()
	s := &Server{fs, cleanup}
	return s
}

// Handler returns an HTTP handler for Server.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/health", s.healthHandler)
	r.Get("/files/:name", handler.Wrap(s.downloadHandler))
	r.Post("/files/:name", handler.Wrap(s.uploadHandler))
	return r
}

// Cleanup cleans up Server's underlying file store.
func (s *Server) Cleanup() {
	s.cleanup()
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

func (s *Server) downloadHandler(w http.ResponseWriter, r *http.Request) error {
	name := chi.URLParam(r, "name")
	if name == "" {
		return handler.Errorf("name required").Status(http.StatusBadRequest)
	}
	f, err := s.fs.GetCacheFileReader(name)
	if err != nil {
		if os.IsNotExist(err) {
			return handler.ErrorStatus(http.StatusNotFound)
		}
		return fmt.Errorf("file store: %s", err)
	}
	if _, err := io.Copy(w, f); err != nil {
		return handler.Errorf("copy: %s", err)
	}
	return nil
}

func (s *Server) uploadHandler(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	name := chi.URLParam(r, "name")
	if name == "" {
		return handler.Errorf("name required").Status(http.StatusBadRequest)
	}

	tmp := fmt.Sprintf("%s.%s", name, uuid.Generate().String())
	if err := s.fs.CreateUploadFile(tmp, 0); err != nil {
		return err
	}
	writer, err := s.fs.GetUploadFileReadWriter(tmp)
	if err != nil {
		return err
	}
	defer writer.Close()
	if _, err := io.Copy(writer, r.Body); err != nil {
		return fmt.Errorf("copy: %s", err)
	}
	defer r.Body.Close()

	if err := s.fs.MoveUploadFileToCache(tmp, name); err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}
