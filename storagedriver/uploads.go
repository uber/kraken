package storagedriver

import (
	"bufio"
	"io"
	"os"
	"time"

	"code.uber.internal/go-common.git/x/log"
	cache "code.uber.internal/infra/dockermover/storage"
	"code.uber.internal/infra/kraken/tracker"
	sd "github.com/docker/distribution/registry/storage/driver"
)

// Uploads b
type Uploads struct {
	lru     *cache.FileCacheMap
	tracker *tracker.Tracker
}

// NewUploads creates a new Uploads
func NewUploads(t *tracker.Tracker, c *cache.FileCacheMap) *Uploads {
	return &Uploads{
		lru:     c,
		tracker: t,
	}
}

func (u *Uploads) initUpload(dir, uuid string) error {
	// create timestamp and tempfile
	ts := time.Now()
	s, err := os.Create(dir + uuid + "_startedat")
	if err != nil {
		return err
	}
	defer s.Close()
	// write timestamp
	s.WriteString(ts.Format(time.RFC3339) + "\n")
	f, err := os.Create(dir + uuid)
	if err != nil {
		return err
	}
	return f.Close()
}

func (u *Uploads) getUploadStartTime(dir, uuid string) ([]byte, error) {
	f, err := os.Open(dir + uuid + "_startedat")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	// read start date
	date, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}
	return date, nil
}

func (u *Uploads) getUploadReader(path, dir, uuid string, offset int64) (reader io.ReadCloser, err error) {
	c := make(chan bool, 1)
	var f *os.File
	go func() {
		to := make(chan byte, 1)
		go func() {
			time.Sleep(readtimeout * time.Second)
			to <- uint8(1)
		}()

		f, err = os.Open(dir + uuid)
		if err != nil {
			c <- false
		}

		// set offest
		_, err = f.Seek(offset, 0)
		if err != nil {
			c <- false
		}

		reader = ChanReadCloser{
			Chan: make(chan byte, 1),
			f:    f,
		}
		c <- true

		// wait for file close or timeout
		select {
		case <-reader.(*ChanReadCloser).Chan:
			break
		case <-to:
			log.Errorf("Timeout reading file %s", path)
		}
	}()
	b := <-c
	if b {
		return reader, nil
	}
	return nil, err
}

func (u *Uploads) getUploadDataStat(dir, uuid string) (fi sd.FileInfo, err error) {
	var info os.FileInfo
	fp := dir + uuid
	info, err = os.Stat(fp)
	if err != nil {
		return nil, err
	}
	fi = sd.FileInfoInternal{
		FileInfoFields: sd.FileInfoFields{
			Path:    info.Name(),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		},
	}
	return fi, nil
}

func (u *Uploads) commitUpload(srcdir, srcuuid, destdir, destsha string) (err error) {
	srcfp := srcdir + srcuuid
	destfp := destdir + destsha
	u.lru.Add(destsha, destfp, func(fp string) error {
		err = os.Rename(srcfp, destfp)
		if err != nil {
			return err
		}
		err = u.tracker.CreateTorrent(destsha, destfp)
		return err
	})
	// remove timestamp file
	os.Remove(srcfp + "_statedat")
	return
}
