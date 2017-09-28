package blobserver

import (
	"bytes"
	"hash"
	"net/http"
	"strconv"
	"time"

	"github.com/spaolacci/murmur3"

	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/utils"
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

func newMockHashConfig() (Config, *hrw.RendezvousHash) {
	config := Config{
		NumReplica: 2,
		HashNodes: map[string]HashNodeConfig{
			"dummy-origin-master01-dca1": {Label: "origin1", Weight: 100},
			"dummy-origin-master02-dca1": {Label: "origin2", Weight: 100},
			"dummy-origin-master03-dca1": {Label: "origin3", Weight: 100},
		},
		Label:    "origin1",
		Hostname: "dummy-origin-master01-dca1",
		LabelToHostname: map[string]string{
			"origin1": "dummy-origin-master01-dca1",
			"origin2": "dummy-origin-master02-dca1",
			"origin3": "dummy-origin-master03-dca1",
		},
	}
	hashState := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)
	for _, node := range config.HashNodes {
		hashState.AddNode(node.Label, node.Weight)
	}
	return config, hashState
}

func configFixture(weights []int) Config {
	hashNodes := make(map[string]HashNodeConfig)

	for i, w := range weights {
		host := "host_" + strconv.Itoa(i) + ":1234"
		if i == 0 {
			host, _ = utils.GetLocalIP()
		}
		hashNodes[host] = HashNodeConfig{
			Label:  "origin" + strconv.Itoa(i),
			Weight: w,
		}
	}

	config := Config{
		HashNodes:  hashNodes,
		NumReplica: 3,
		Repair: RepairConfig{
			NumWorkers:   10,
			NumRetries:   3,
			RetryDelayMs: time.Duration(200 * time.Millisecond),
		},
	}

	// TODO(codyg): Remove this once context is vanquished.
	config.LabelToHostname = make(map[string]string, len(config.HashNodes))
	for h, node := range config.HashNodes {
		config.LabelToHostname[node.Label] = h
	}
	hostname, err := utils.GetLocalIP()
	if err != nil {
		panic(err)
	}
	config.Hostname = hostname
	config.Label = config.HashNodes[hostname].Label

	return config
}
