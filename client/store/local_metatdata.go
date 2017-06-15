package store

import (
	"fmt"
	"regexp"
	"strings"
)

// const enum representing the status of a torrent's piece
const (
	PieceClean    = uint8(0)
	PieceDirty    = uint8(1)
	PieceDone     = uint8(2)
	PieceDontCare = uint8(3)
)

// Hook for test metadata classes so they can be reloaded.
var _testMetadataLookupFuncs []func(string) MetadataType

func getMetadataType(fp string) MetadataType {
	if strings.HasSuffix(fp, "_status") {
		return &pieceStatus{}
	}
	if strings.HasSuffix(fp, "_startedat") {
		return &startedAt{}
	}
	if re := regexp.MustCompile("_hashstates/\\w+/\\w+$"); re.MatchString(fp) {
		algRe := regexp.MustCompile("_hashstates/(\\w+)/\\w+$")
		alg := algRe.FindStringSubmatch(fp)[0]
		offsetRe := regexp.MustCompile("_hashstates/\\w+/(\\w+)$")
		offset := offsetRe.FindStringSubmatch(fp)[0]

		return &hashState{
			alg:    alg,
			offset: offset,
		}
	}

	// Check test metadata classes
	for _, f := range _testMetadataLookupFuncs {
		mt := f(fp)
		if mt != nil {
			return mt
		}
	}

	return nil
}

type pieceStatus struct{}

func getPieceStatus() MetadataType {
	return pieceStatus{}
}

func (p pieceStatus) Suffix() string {
	return "_status"
}

func (p pieceStatus) IsValidState(state FileState) bool {
	switch state {
	case stateDownload:
		return true
	default:
		return false
	}
}

type startedAt struct{}

func getStartedAt() MetadataType {
	return startedAt{}
}

func (s startedAt) Suffix() string {
	return "_startedat"
}

func (s startedAt) IsValidState(state FileState) bool {
	switch state {
	case stateDownload:
		return true
	default:
		return false
	}
}

// hashState stores partial hash result of upload data for resumable upload.
// Docker registry double writes to a writer and digester, and the digester generates this snapshot.
type hashState struct {
	alg    string
	offset string
}

func getHashState(alg, offset string) MetadataType {
	return hashState{
		alg:    alg,
		offset: offset,
	}
}

func (h hashState) Suffix() string {
	return fmt.Sprintf("_hashstates/%s/%s", h.alg, h.offset)
}

func (h hashState) IsValidState(state FileState) bool {
	switch state {
	case stateUpload:
		return true
	default:
		return false
	}
}

type refCount struct{}

func getRefCount() MetadataType {
	return refCount{}
}

func (r refCount) Suffix() string {
	return "_refcount"
}

func (r refCount) IsValidState(state FileState) bool {
	switch state {
	case stateCache:
		return true
	default:
		return false
	}
}
