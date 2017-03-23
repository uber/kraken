package dockerregistry

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/log"
	cache "code.uber.internal/infra/dockermover/storage"
	"code.uber.internal/infra/kraken/kraken/test-tracker"
	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	sd "github.com/docker/distribution/registry/storage/driver"
)

// Blobs b
type Blobs struct {
	tracker *tracker.Tracker
	client  *torrent.Client
	lru     *cache.FileCacheMap
}

// NewBlobs creates Blobs
func NewBlobs(tr *tracker.Tracker, cl *torrent.Client, c *cache.FileCacheMap) *Blobs {
	return &Blobs{
		tracker: tr,
		client:  cl,
		lru:     c,
	}
}

func (b *Blobs) getBlobStat(path, sha string) (fi sd.FileInfo, err error) {
	var info os.FileInfo
	_, ok := b.lru.Get(sha, func(fp string) error {
		info, err = os.Stat(fp)
		if err != nil {
			return err
		}
		fi = sd.FileInfoInternal{
			FileInfoFields: sd.FileInfoFields{
				Path:    info.Name(),
				Size:    info.Size(),
				ModTime: info.ModTime(),
				IsDir:   info.IsDir(),
			},
		}
		return nil
	})
	if !ok {
		err = b.download(path, sha)
		if err != nil {
			return nil, sd.PathNotFoundError{
				DriverName: "p2p",
				Path:       path,
			}
		}
		_, ok = b.lru.Get(sha, func(fp string) error {
			info, err = os.Stat(fp)
			if err != nil {
				return err
			}
			fi = sd.FileInfoInternal{
				FileInfoFields: sd.FileInfoFields{
					Path:    info.Name(),
					Size:    info.Size(),
					ModTime: info.ModTime(),
					IsDir:   info.IsDir(),
				},
			}
			return nil
		})
		if !ok {
			return nil, fmt.Errorf("Unable to get file from cache %s", path)
		}
	}

	if err != nil {
		return nil, err
	}

	if fi == nil {
		return nil, fmt.Errorf("Unable to get file info of %s", path)
	}

	return fi, nil
}

func (b *Blobs) putBlobData(path, dir, sha string, content []byte) error {
	fp := dir + sha
	var mi *metainfo.MetaInfo
	_, ok, _ := b.lru.Add(sha, fp, func(fp string) error {
		// Write to file
		f, err := os.Create(fp)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = f.Write(content)
		if err != nil {
			// Remove the file if error copying data
			defer os.Remove(fp)
			return err
		}

		mi, err = b.tracker.CreateTorrentInfo(sha, fp)
		if err != nil {
			return err
		}
		err = b.tracker.CreateTorrentFromInfo(sha, mi)
		return err
	})

	_, err := b.client.AddTorrent(mi)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("Failed to put content for %s", path)
	}

	return nil
}

func (b *Blobs) download(path, sha string) error {
	completed := false

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		uri, err := b.tracker.GetMagnet(sha)
		if err != nil {
			wg.Done()
			return
		}
		t, err := b.client.AddMagnet(uri)
		if err != nil {
			wg.Done()
			return
		}

		completedPieces := 0
		psc := t.SubscribePieceStateChanges()
		to := make(chan byte, 1)
		go func() {
			time.Sleep(p2ptimeout * time.Second)
			to <- uint8(1)
		}()
		go func() {
			for {
				select {
				case v := <-psc.Values:
					if v.(torrent.PieceStateChange).Complete {
						completedPieces = completedPieces + 1
					}
					if completedPieces == t.NumPieces() {
						completed = true
						wg.Done()
						return
					}
				case <-to:
					log.Errorf("Timeout waiting for %s to download", path)
					wg.Done()
					return
				}
			}
		}()
		log.Info("wait for info")
		<-t.GotInfo()
		log.Info("wait for download")
		t.DownloadAll()
	}()

	wg.Wait()
	if completed {
		return nil
	}

	return sd.PathNotFoundError{
		DriverName: "p2p",
		Path:       path,
	}
}

func (b *Blobs) getOrDownloadBlobData(path, sha string) (data []byte, err error) {
	// check cache
	_, ok := b.lru.Get(sha, func(fp string) error {
		data, err = ioutil.ReadFile(fp)
		return nil
	})
	if ok {
		return data, err
	}

	err = b.download(path, sha)
	if err != nil {
		return nil, err
	}

	_, ok = b.lru.Get(sha, func(fp string) error {
		data, err = ioutil.ReadFile(fp)
		return nil
	})
	if ok {
		return data, err
	}

	return nil, fmt.Errorf("Failed to download %s", path)
}

func (b *Blobs) getOrDownloadBlobReader(path, sha string, offset int64) (reader io.ReadCloser, err error) {
	var ok bool
	reader, ok, err = b.getBlobReader(path, sha, offset)
	if err != nil {
		return nil, err
	}
	if ok {
		return reader, nil
	}

	err = b.download(path, sha)
	if err != nil {
		return nil, err
	}

	reader, ok, err = b.getBlobReader(path, sha, offset)
	if err != nil {
		return nil, err
	}
	if ok {
		return reader, nil
	}

	return nil, fmt.Errorf("Failed to download %s", path)
}

func (b *Blobs) getBlobReader(path, sha string, offset int64) (io.ReadCloser, bool, error) {
	var err error
	var f *os.File
	var reader ChanReadCloser
	c := make(chan bool, 1)

	go func() {
		_, ok := b.lru.Get(sha, func(fp string) error {
			to := make(chan byte, 1)
			go func() {
				time.Sleep(readtimeout * time.Second)
				to <- uint8(1)
			}()

			f, err = os.Open(fp)
			if err != nil {
				return err
			}

			// set offest
			_, err = f.Seek(offset, 0)
			if err != nil {
				return err
			}

			reader = ChanReadCloser{
				Chan: make(chan byte, 1),
				f:    f,
			}
			c <- true

			// wait for file close or timeout
			select {
			case <-reader.Chan:
				break
			case <-to:
				log.Errorf("Timeout reading file %s", path)
			}

			return nil
		})

		// treat error
		if !ok || err != nil {
			c <- false
		}
	}()

	// block until either lru.Get returns false or a reader is created
	d := <-c
	if d {
		return reader, true, nil
	}
	return nil, false, err
}
