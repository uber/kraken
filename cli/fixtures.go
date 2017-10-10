package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"code.uber.internal/go-common.git/x/log"
	"github.com/pressly/chi"

	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/origin/blobserver"
)

// TestOriginServer represents Test origin server
type TestOriginServer struct {
	label     string
	listener  net.Listener
	digests   []OriginContent
	hashstate *hrw.RendezvousHash
	config    blobserver.Config
}

// OriginContentFixture returns content items
func OriginContentFixture() OriginContentList {
	cl := OriginContentList{
		OriginContentItems: []OriginContent{
			{
				Digest: "15eaa75240aed625be3e142205df3adbbb7051802b32f450e40276be8582b6d8",
				Size:   10,
				Path:   "/data/12/ea",
			},
			{
				Digest: "c3fad2d188365979aecc8f4deed84c745907fbee3c6eb981d9ad3c3cd44913a9",
				Size:   20,
				Path:   "/data/c3/fa",
			},
			{
				Digest: "095ebe3b7e21645e700de76ac7b02db3d18add6de8d64526b7360a12751f5e28",
				Size:   30,
				Path:   "/data/09/5e",
			},
			{
				Digest: "106ea88fe2ccb794396590dd3c082b334d117812928e1b0bedafac1a54274ac4",
				Size:   40,
				Path:   "/data/10/6e",
			},
			{
				Digest: "14722f2e4c8fa3ba142e2c955c253db8f1274f1761fa0afda85de0f11571b8c8",
				Size:   50,
				Path:   "/data/14/72",
			},
			{
				Digest: "6f858f4b582302063936eedea2375c52d137aac95c184e94b3f0b46bb887362a",
				Size:   60,
				Path:   "/data/6f/85",
			},
		},
	}

	return cl
}

// OriginFixture creates a cluster of origin servers, initializes them to weights
// and fill them out from the list of blob digests
func OriginFixture(digests []OriginContent, weights []int) ([]*TestOriginServer, blobserver.Config) {

	listeners := make([]net.Listener, len(weights))
	hashstate := make(map[string]blobserver.HashNodeConfig)

	for i, w := range weights {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}

		hashstate[listener.Addr().String()] = blobserver.HashNodeConfig{
			Label:  "origin" + strconv.Itoa(i),
			Weight: w,
		}
		listeners[i] = listener
	}

	config := blobserver.Config{HashNodes: hashstate, NumReplica: 3}
	origins := make([]*TestOriginServer, len(weights))

	hs := initHashState(config)

	for i := 0; i < len(weights); i++ {
		origins[i] = &TestOriginServer{
			label:     "origin" + strconv.Itoa(i),
			listener:  listeners[i],
			hashstate: hs,
			digests:   []OriginContent{},
			config:    config,
		}

		origins[i].fillContentItems(digests)
	}

	return origins, config
}

// fillContentItems adds content item to an internal list of blob digests
func (to *TestOriginServer) fillContentItems(digests []OriginContent) {
	for _, ci := range digests {
		rhl, err := to.hashstate.GetOrderedNodes(ci.Digest, 1)
		if err != nil {
			panic(err)
		}
		if rhl[0].Label == to.label {
			to.digests = append(to.digests, ci)
		}
	}
}

// fillContentItems adds content item to an internal list of blob digests
func (to *TestOriginServer) findOriginByLabel(originLabel string) string {
	// Add all configured nodes to a hashing statae
	for origin, node := range to.config.HashNodes {

		if node.Label == originLabel {
			return origin
		}
	}
	return ""
}

// Serve starts serving http requests
func (to *TestOriginServer) Serve() {

	router := chi.NewRouter()

	router.Get("/info", to.InfoHandler)
	router.Get("/content/", to.ContentListHandler)
	router.Post("/repair/:digest", to.RepairHandler)
	router.Head("/blobs/:digest", to.HeadBlobHandler)
	router.Get("/blobs/:digest", to.GetBlobHandler)
	router.Delete("/blobs/:digest", to.DeleteHandler)

	http.Serve(to.listener, router)
}

// Close and shutdown http server
func (to *TestOriginServer) Close() {
	to.listener.Close()
}

// InfoHandler info
func (to *TestOriginServer) InfoHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	info := &OriginCapacity{
		Capacity:    10000,
		Utilization: 5000,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(info)
}

// ContentListHandler lists content
func (to *TestOriginServer) ContentListHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	cl := &OriginContentList{OriginContentItems: to.digests}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(cl)
}

// HeadBlobHandler content blobs request
func (to *TestOriginServer) HeadBlobHandler(w http.ResponseWriter, r *http.Request) {

	digest := chi.URLParam(r, "digest")
	if len(digest) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// returns error if missing info hash
	if digest == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing digest in request."))
		return
	}

	for i := range to.digests {
		if digest == to.digests[i].Digest {
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	w.WriteHeader(http.StatusNotFound)
}

// GetBlobHandler content blob get request
func (to *TestOriginServer) GetBlobHandler(w http.ResponseWriter, r *http.Request) {

	digest := chi.URLParam(r, "digest")
	if len(digest) == 0 {
		w.Write([]byte("Empty digest in a request."))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream-v1")

	for i := range to.digests {
		if digest == to.digests[i].Digest {
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	w.WriteHeader(http.StatusNotFound)
}

// RepairHandler repairs content
func (to *TestOriginServer) RepairHandler(w http.ResponseWriter, r *http.Request) {

	digest := chi.URLParam(r, "digest")
	if len(digest) == 0 {
		w.Write([]byte("Empty digest in a request."))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	for i := range to.digests {
		if digest == to.digests[i].Digest {
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	// got here meaning content was not found locally,
	// need to reguest it from one of the origin servers
	rhl, err := to.hashstate.GetOrderedNodes(digest, 1)
	if err != nil {
		panic(err)
	}

	origin := to.findOriginByLabel(rhl[0].Label)
	if origin == "" {
		panic(fmt.Errorf("Origin server is not found for the content: %s", digest))
	}

	// get content from origin
	// it shold be a blocking function in production that
	// does not return until content is downloaded
	// here we just return Status OK
	// assuming we get the content successfully
	w.WriteHeader(http.StatusOK)
}

// DeleteHandler deletes content blob
func (to *TestOriginServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	digest := chi.URLParam(r, "digest")
	if len(digest) == 0 {
		w.Write([]byte("Empty digest in a request."))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	// returns error if missing info hash
	if digest == "" {
		w.Write([]byte("Failed to delete content item. Missing digest in request."))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	index := -1
	for i := range to.digests {
		if digest == to.digests[i].Digest {
			index = i
		}
	}

	if index >= 0 {
		// remove the element from array
		// not preserving order
		to.digests[len(to.digests)-1], to.digests[index] = to.digests[index], to.digests[len(to.digests)-1]
		to.digests = to.digests[:len(to.digests)-1]

		w.WriteHeader(http.StatusOK)
		return
	}
	// writes all torrents statuses
	w.WriteHeader(http.StatusNotFound)
}
