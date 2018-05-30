package dockerregistry

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"code.uber.internal/infra/kraken/lib/store"
)

const (
	_startedAtSuffix = "_startedat"
	_startedAtLayout = time.RFC3339
)

func init() {
	store.RegisterMetadata(regexp.MustCompile(_startedAtSuffix), &startedAtMetadataFactory{})

	// TODO(evelynl): use _ instead of /, otherwise it won't support reload.
	store.RegisterMetadata(regexp.MustCompile("_hashstates/\\w+/\\w+$"), &hashStateMetadataFactory{})
}

type startedAtMetadataFactory struct{}

func (f startedAtMetadataFactory) Create(suffix string) store.Metadata {
	return &startedAtMetadata{}
}

// startedAtMetadata is used to remove uploads that have been active for too long.
type startedAtMetadata struct {
	time time.Time
}

func newStartedAtMetadata(t time.Time) *startedAtMetadata {
	return &startedAtMetadata{t}
}

func (s *startedAtMetadata) GetSuffix() string {
	return _startedAtSuffix
}

func (s *startedAtMetadata) Movable() bool {
	return false
}

func (s *startedAtMetadata) Serialize() ([]byte, error) {
	return []byte(s.time.Format(_startedAtLayout)), nil
}

func (s *startedAtMetadata) Deserialize(b []byte) error {
	t, err := time.Parse(_startedAtLayout, string(b))
	if err != nil {
		return err
	}
	s.time = t
	return nil
}

type hashStateMetadataFactory struct{}

func (f hashStateMetadataFactory) Create(suffix string) store.Metadata {
	parts := strings.Split(suffix, "/")
	if len(parts) != 3 {
		return nil
	}
	algo := parts[1]
	offset := parts[2]
	return newHashStateMetadata(algo, offset)
}

// hashStateMetadata stores partial hash result of upload data for resumable upload.
// Docker registry double writes to a writer and digester, and the digester
// generates this snapshot.
type hashStateMetadata struct {
	algo    string
	offset  string
	content []byte
}

func newHashStateMetadata(algo, offset string) *hashStateMetadata {
	return &hashStateMetadata{
		algo:   algo,
		offset: offset,
	}
}

func (h *hashStateMetadata) GetSuffix() string {
	return fmt.Sprintf("_hashstates/%s/%s", h.algo, h.offset)
}

func (h *hashStateMetadata) Movable() bool {
	return false
}

func (h *hashStateMetadata) Serialize() ([]byte, error) {
	return h.content, nil
}

func (h *hashStateMetadata) Deserialize(b []byte) error {
	h.content = b
	return nil
}

func (h *hashStateMetadata) dockerPath() string {
	return fmt.Sprintf("hashstates/%s/%s", h.algo, h.offset)
}
