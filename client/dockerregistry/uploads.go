package dockerregistry

import (
	"bufio"
	"io"
	"os"
	"time"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/kraken/test-tracker"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	sd "github.com/docker/distribution/registry/storage/driver"
)

// Uploads b
type Uploads struct {
	store   *store.LocalFileStore
	tracker *tracker.Tracker
	client  *torrent.Client
}

// NewUploads creates a new Uploads
func NewUploads(t *tracker.Tracker, cl *torrent.Client, s *store.LocalFileStore) *Uploads {
	return &Uploads{
		store:   s,
		tracker: t,
		client:  cl,
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
	var f *os.File

	f, err = os.Open(dir + uuid)
	if err != nil {
		return nil, err
	}

	// set offest
	_, err = f.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	reader = ChanReadCloser{
		f: f,
	}

	return reader, nil
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
	var mi *metainfo.MetaInfo

	err = u.store.MoveUploadFileToCache(srcuuid, destsha)
	if err != nil {
		return err
	}
	mi, err = u.tracker.CreateTorrentInfo(destsha, destfp)
	if err != nil {
		return err
	}
	err = u.tracker.CreateTorrentFromInfo(destsha, mi)
	if err != nil {
		return err
	}

	_, err = u.client.AddTorrent(mi)
	if err != nil {
		return err
	}
	// remove timestamp file
	os.Remove(srcfp + "_statedat")
	return
}
