package store

import (
	"fmt"
	"regexp"

	"code.uber.internal/infra/kraken/client/store/base"
)

func init() {
	// TODO: use _ instead of /, otherwise it won't support reload.
	base.RegisterMetadata(regexp.MustCompile("_hashstates/\\w+/\\w+$"), &hashStateFactory{})
}

type hashStateFactory struct{}

func (f hashStateFactory) Create(suffix string) base.MetadataType {
	algoRe := regexp.MustCompile("_hashstates/(\\w+)/\\w+$")
	algo := algoRe.FindStringSubmatch(suffix)[0]
	offsetRe := regexp.MustCompile("_hashstates/\\w+/(\\w+)$")
	offset := offsetRe.FindStringSubmatch(suffix)[0]
	return NewHashState(algo, offset)
}

// hashState stores partial hash result of upload data for resumable upload.
// Docker registry double writes to a writer and digester, and the digester generates this snapshot.
type hashState struct {
	algo   string
	offset string
}

// NewHashState initializes and returns an new MetadataType obj.
func NewHashState(algo, offset string) base.MetadataType {
	return &hashState{
		algo:   algo,
		offset: offset,
	}
}

func (h hashState) GetSuffix() string {
	return fmt.Sprintf("_hashstates/%s/%s", h.algo, h.offset)
}

func (h hashState) Movable() bool {
	return false
}
