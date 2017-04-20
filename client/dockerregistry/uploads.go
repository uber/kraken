package dockerregistry

import (
	"io"
	"os"
	"time"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/client/torrentclient"

	sd "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/uuid"
)

// Uploads b
type Uploads struct {
	store  *store.LocalFileStore
	client *torrentclient.Client
}

// NewUploads creates a new Uploads
func NewUploads(cl *torrentclient.Client, s *store.LocalFileStore) *Uploads {
	return &Uploads{
		store:  s,
		client: cl,
	}
}

func (u *Uploads) initUpload(dir, uuid string) error {
	// Create timestamp and tempfile
	_, err := u.store.CreateUploadFile(uuid, 0)
	if err != nil {
		return err
	}

	return u.store.SetUploadFileStartedAt(uuid, []byte(time.Now().Format(time.RFC3339)))
}

func (u *Uploads) getUploadStartTime(dir, uuid string) ([]byte, error) {
	return u.store.GetUploadFileStartedAt(uuid)
}

func (u *Uploads) getUploadReader(path, dir, uuid string, offset int64) (io.ReadCloser, error) {
	reader, err := u.store.GetUploadFileReader(uuid)
	if err != nil {
		return nil, err
	}

	// Set offest
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

// commmitUpload move a complete data blob from upload directory to cache diretory
func (u *Uploads) commitUpload(srcdir, srcuuid, destdir, destsha string) (err error) {
	// Remove timestamp file
	err = u.store.DeleteUploadFileStartedAt(srcuuid)
	if err != nil {
		return err
	}

	err = u.store.MoveUploadFileToCache(srcuuid, destsha)
	if err != nil {
		return err
	}

	destfp := destdir + destsha
	err = u.client.CreateTorrentFromFile(destsha, destfp)
	if err != nil {
		return err
	}

	return
}

// putBlobData is used to write content to files directly, like image manifest and metadata.
func (u *Uploads) putBlobData(fileName string, content []byte) error {
	// It's better to have a random extension to avoid race condition.
	randFileName := fileName + "." + uuid.Generate().String()
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

	err = u.store.MoveUploadFileToCache(randFileName, fileName)
	if os.IsExist(err) {
		// It's okay to fail with "os.IsExist"
		return nil
	}
	if err != nil {
		return err
	}

	path, err := u.store.GetCacheFilePath(fileName)
	if err != nil {
		return err
	}

	// TODO (@yiran) Shouldn't use file path directly.
	return u.client.CreateTorrentFromFile(fileName, path)
}
