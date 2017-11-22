package dockerregistry

import (
	"io"
	"os"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer"
	"code.uber.internal/infra/kraken/lib/store"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/uuid"
)

// Uploads b
type Uploads struct {
	store      store.FileStore
	transferer transfer.ImageTransferer
}

// NewUploads creates a new Uploads
func NewUploads(transferer transfer.ImageTransferer, s store.FileStore) *Uploads {
	return &Uploads{
		store:      s,
		transferer: transferer,
	}
}

// GetContent returns uploads content based given subtype
func (u *Uploads) GetContent(path string, subtype PathSubType) ([]byte, error) {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return nil, err
	}
	switch subtype {
	case _startedat:
		return u.getUploadStartTime(u.store.Config().UploadDir, uuid)
	case _hashstates:
		algo, offset, err := GetUploadAlgoAndOffset(path)
		if err != nil {
			return nil, err
		}
		return u.store.GetUploadFileHashState(uuid, algo, offset)
	}
	return nil, InvalidRequestError{path}
}

// GetReader returns a readercloser for uploaded contant
func (u *Uploads) GetReader(path string, subtype PathSubType, offset int64) (io.ReadCloser, error) {
	switch subtype {
	case _data:
		uuid, err := GetUploadUUID(path)
		if err != nil {
			return nil, err
		}
		return u.getUploadReader(uuid, offset)
	}
	return nil, InvalidRequestError{path}
}

// PutUploadContent writes to upload file given type and content
func (u *Uploads) PutUploadContent(path string, subtype PathSubType, content []byte) error {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return err
	}

	switch subtype {
	case _startedat:
		return u.initUpload(uuid)
	case _data:
		return u.putBlobData(uuid, content)
	case _hashstates:
		algo, offset, err := GetUploadAlgoAndOffset(path)
		if err != nil {
			return err
		}
		return u.store.SetUploadFileHashState(uuid, content, algo, offset)
	}
	return InvalidRequestError{path}
}

// PutBlobContent writes content to a blob
func (u *Uploads) PutBlobContent(path string, content []byte) error {
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil
	}
	return u.putBlobData(digest, content)
}

// GetWriter returns a writer for uploaded content
func (u *Uploads) GetWriter(path string, subtype PathSubType) (storagedriver.FileWriter, error) {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return nil, err
	}
	switch subtype {
	case _data:
		return u.store.GetUploadFileReadWriter(uuid)
	}
	return nil, InvalidRequestError{path}
}

// GetStat returns upload file info
func (u *Uploads) GetStat(path string) (storagedriver.FileInfo, error) {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return nil, err
	}
	return u.getUploadDataStat(u.store.Config().UploadDir, uuid)
}

// ListHashStates lists all upload hashstates
func (u *Uploads) ListHashStates(path string, subtype PathSubType) ([]string, error) {
	uuid, err := GetUploadUUID(path)
	if err != nil {
		return nil, err
	}
	switch subtype {
	case _hashstates:
		return u.store.ListUploadFileHashStatePaths(uuid)
	}
	return nil, InvalidRequestError{path}
}

// Move moves upload file to cached blob store
func (u *Uploads) Move(uploadsPath string, blobsPath string) error {
	uuid, err := GetUploadUUID(uploadsPath)
	if err != nil {
		return err
	}

	digest, err := GetBlobDigest(blobsPath)
	if err != nil {
		return err
	}
	return u.commitUpload(uuid, u.store.Config().CacheDir, digest)
}

func (u *Uploads) initUpload(uuid string) error {
	// Create timestamp and tempfile
	if err := u.store.CreateUploadFile(uuid, 0); err != nil {
		return err
	}

	return u.store.SetUploadFileStartedAt(uuid, []byte(time.Now().Format(time.RFC3339)))
}

func (u *Uploads) getUploadStartTime(dir, uuid string) ([]byte, error) {
	return u.store.GetUploadFileStartedAt(uuid)
}

func (u *Uploads) getUploadReader(uuid string, offset int64) (io.ReadCloser, error) {
	reader, err := u.store.GetUploadFileReader(uuid)
	if err != nil {
		return nil, err
	}

	// Set offset
	_, err = reader.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func (u *Uploads) getUploadDataStat(dir, uuid string) (fi storagedriver.FileInfo, err error) {
	info, err := u.store.GetUploadFileStat(uuid)
	if err != nil {
		return nil, err
	}
	// Hacking the path, since kraken storage driver is also the consumer of this info.
	// Instead of the relative path from root that docker registry expected, just use uuid.
	fi = storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    uuid,
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		},
	}
	return fi, nil
}

type fileCloner struct {
	fs   store.FileStore
	name string
}

func (c *fileCloner) Clone() (io.ReadCloser, error) {
	return c.fs.GetCacheFileReader(c.name)
}

// commmitUpload move a complete data blob from upload directory to cache diretory
func (u *Uploads) commitUpload(srcuuid, destdir, destsha string) (err error) {
	info, err := u.store.GetUploadFileStat(srcuuid)
	if err != nil {
		return err
	}

	err = u.store.MoveUploadFileToCache(srcuuid, destsha)
	if err != nil {
		return err
	}

	return u.transferer.Upload(destsha, &fileCloner{u.store, destsha}, info.Size())
}

// putBlobData is used to write content to files directly, like image manifest and metadata.
func (u *Uploads) putBlobData(fileName string, content []byte) error {
	// It's better to have a random extension to avoid race condition.
	randFileName := fileName + "." + uuid.Generate().String()
	if err := u.store.CreateUploadFile(randFileName, int64(len(content))); err != nil {
		return err
	}

	rw, err := u.store.GetUploadFileReadWriter(randFileName)
	if err != nil {
		return err
	}
	defer rw.Close()

	_, err = rw.Write(content)
	if err != nil {
		return err
	}

	if _, err := rw.Seek(0, 0); err != nil {
		return err
	}

	info, err := u.store.GetUploadFileStat(randFileName)
	if err != nil {
		return err
	}

	err = u.store.MoveUploadFileToCache(randFileName, fileName)
	if os.IsExist(err) {
		// It's okay to fail with "os.IsExist"
		return nil
	}
	if err != nil {
		return err
	}

	err = u.transferer.Upload(fileName, &fileCloner{u.store, fileName}, info.Size())
	if err != nil {
		return err
	}
	return nil
}
