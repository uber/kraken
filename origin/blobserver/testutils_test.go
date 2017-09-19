package blobserver

import (
	"bytes"
	"hash"
	"net/http"

	"github.com/spaolacci/murmur3"

	"code.uber.internal/infra/kraken/lib/hrw"
	hashcfg "code.uber.internal/infra/kraken/origin/config"
)

const (
	emptyDigestHex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	randomUUID     = "b9cb2c15-3cb5-46bf-a63c-26b0c5b9bc24"
)

type mockResponseWriter struct {
	header http.Header
	status int
	buf    *bytes.Buffer
}

func newMockResponseWriter() *mockResponseWriter {
	return &mockResponseWriter{
		header: http.Header{},
		buf:    bytes.NewBuffer([]byte("")),
	}
}

func newMockHashConfig() (hashcfg.HashConfig, *hrw.RendezvousHash) {
	hashConfig := hashcfg.HashConfig{
		Verbose:    true,
		NumReplica: 2,
		HashNodes: map[string]hashcfg.HashNodeConfig{
			"kraken-origin-master01-dca1": hashcfg.HashNodeConfig{Label: "origin1", Weight: 100},
			"kraken-origin-master02-dca1": hashcfg.HashNodeConfig{Label: "origin2", Weight: 100},
			"kraken-origin-master03-dca1": hashcfg.HashNodeConfig{Label: "origin3", Weight: 100},
		},
		Label:    "origin1",
		Hostname: "kraken-origin-master01-dca1",
		LabelToHostname: map[string]string{
			"origin1": "kraken-origin-master01-dca1",
			"origin2": "kraken-origin-master02-dca1",
			"origin3": "kraken-origin-master03-dca1",
		},
	}
	hashState := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)
	for _, node := range hashConfig.HashNodes {
		hashState.AddNode(node.Label, node.Weight)
	}
	return hashConfig, hashState
}
