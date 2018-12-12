package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/healthcheck"
	"github.com/uber/kraken/lib/hostlist"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/tracker/metainfoclient"
	"github.com/uber/kraken/utils/memsize"
)

type uploader struct {
	metaInfoClient metainfoclient.Client
	originCluster  blobclient.ClusterClient
}

func newUploader(origin, tracker string, tls *tls.Config) *uploader {
	r := blobclient.NewClientResolver(blobclient.NewProvider(), hostlist.Fixture(origin))
	originCluster := blobclient.NewClusterClient(r)

	return &uploader{
		metaInfoClient: metainfoclient.New(healthcheck.NoopFailed(hostlist.Fixture(tracker)), tls),
		originCluster:  originCluster,
	}
}

func (u *uploader) upload(
	fileSize, pieceSize uint64) (ih core.InfoHash, d core.Digest, err error) {

	log.Println("Generating blob data ...")

	d, f, err := generateBlobFile(fileSize)
	if err != nil {
		return ih, d, fmt.Errorf("generate blob file: %s", err)
	}
	defer os.Remove(f.Name())

	log.Println("Uploading to origin server...")

	if err := u.originCluster.UploadBlob(backend.NoopNamespace, d, f); err != nil {
		return ih, d, fmt.Errorf("origin upload: %s", err)
	}
	if err := u.originCluster.OverwriteMetaInfo(d, int64(pieceSize)); err != nil {
		return ih, d, fmt.Errorf("origin overwrite metainfo: %s", err)
	}
	mi, err := u.originCluster.GetMetaInfo("noexist", d)
	if err != nil {
		return ih, d, fmt.Errorf("origin get metainfo: %s", err)
	}

	return mi.InfoHash(), d, nil
}

func generateBlobFile(size uint64) (core.Digest, *os.File, error) {
	r := io.LimitReader(rand.New(rand.NewSource(time.Now().Unix())), int64(size))

	d := core.NewDigester()
	r = d.Tee(r)

	f, err := ioutil.TempFile("", "")
	if err != nil {
		return core.Digest{}, nil, err
	}

	if _, err := io.CopyBuffer(f, r, make([]byte, 4*memsize.MB)); err != nil {
		return core.Digest{}, nil, err
	}
	f.Seek(0, 0)

	return d.Digest(), f, nil
}
