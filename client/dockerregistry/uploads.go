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
	"github.com/docker/distribution/uuid"
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

func (u *Uploads) getUploadReader(path, dir, uuid string, offset int64) (io.ReadCloser, error) {
	reader, err := u.store.GetUploadFileReader(uuid)
	if err != nil {
		return nil, err
	}

	// set offest
	_, err = reader.Seek(offset, 0)
	if err != nil {
		return nil, err
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

// putBlobData is used to write content to files directly, like image manifest and metadata.
func (u *Uploads) putBlobData(fileName string, content []byte) error {
	var mi *metainfo.MetaInfo

	// It's better to have a random extension to avoid race condition.
	var randFileName = fileName + "." + uuid.Generate().String()
	_, err := u.store.CreateUploadFile(randFileName, int64(len(content)))
	if err != nil {
		return err
	}
	writer, err := u.store.GetUploadFileReadWriter(randFileName)
	if err != nil {
		return err
	}
	_, err = writer.Write(content)
	if err != nil {
		writer.Close()
		return err
	}
	writer.Close()

	// TODO (@yiran) Shouldn't use file path directly.
	// TODO (@yiran) Maybe it's okay to fail with "os.IsExist"
	err = u.store.MoveUploadFileToCache(randFileName, fileName)
	if err != nil {
		return err
	}
	path, err := u.store.GetCacheFilePath(fileName)
	if err != nil {
		return err
	}
	mi, err = u.tracker.CreateTorrentInfo(fileName, path)
	if err != nil {
		return err
	}
	err = u.tracker.CreateTorrentFromInfo(fileName, mi)
	if err != nil {
		return err
	}

	_, err = u.client.AddTorrent(mi)
	if err != nil {
		return err
	}

	return nil
}
