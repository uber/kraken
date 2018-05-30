package dockerregistry

import (
	"fmt"
	"io"
	"os"
	stdpath "path"
	"time"

	"code.uber.internal/infra/kraken/core"
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
		hs := newHashStateMetadata(algo, offset)
		if err := u.store.GetUploadFileMetadata(uuid, hs); err != nil {
			return nil, err
		}
		return hs.Serialize()
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
		hs := newHashStateMetadata(algo, offset)
		if err := hs.Deserialize(content); err != nil {
			return fmt.Errorf("deserialize hash state: %s", err)
		}
		return u.store.SetUploadFileMetadata(uuid, hs)
	}
	return InvalidRequestError{path}
}

// PutBlobContent writes content to a blob
func (u *Uploads) PutBlobContent(path string, content []byte) error {
	digest, err := GetBlobDigest(path)
	if err != nil {
		return nil
	}
	return u.putBlobData(digest.Hex(), content)
}

// GetWriter returns a writer for uploaded content
func (u *Uploads) GetWriter(path string, subtype PathSubType) (store.FileReadWriter, error) {
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
		var paths []string
		u.store.RangeUploadMetadata(uuid, func(md store.Metadata) error {
			if hs, ok := md.(*hashStateMetadata); ok {
				p := stdpath.Join("localstore", "_uploads", uuid, hs.dockerPath())
				paths = append(paths, p)
			}
			return nil
		})
		return paths, nil
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
	return u.commitUpload(uuid, u.store.Config().CacheDir, digest.Hex())
}

func (u *Uploads) initUpload(uuid string) error {
	// Create timestamp and tempfile
	if err := u.store.CreateUploadFile(uuid, 0); err != nil {
		return err
	}
	s := newStartedAtMetadata(time.Now())
	return u.store.SetUploadFileMetadata(uuid, s)
}

func (u *Uploads) getUploadStartTime(dir, uuid string) ([]byte, error) {
	var s startedAtMetadata
	if err := u.store.GetUploadFileMetadata(uuid, &s); err != nil {
		return nil, err
	}
	return s.Serialize()
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

// commmitUpload move a complete data blob from upload directory to cache diretory
func (u *Uploads) commitUpload(srcuuid, destdir, destsha string) error {
	if err := u.store.MoveUploadFileToCache(srcuuid, destsha); err != nil {
		return fmt.Errorf("move upload file to cache: %s", err)
	}
	f, err := u.store.GetCacheFileReader(destsha)
	if err != nil {
		return fmt.Errorf("get cache file: %s", err)
	}
	d, err := core.NewSHA256DigestFromHex(destsha)
	if err != nil {
		return fmt.Errorf("new digest: %s", err)
	}
	if err := u.transferer.Upload("TODO", d, f); err != nil {
		return fmt.Errorf("upload: %s", err)
	}
	return nil
}

// putBlobData is used to write content to files directly, like image manifest and metadata.
func (u *Uploads) putBlobData(fileName string, content []byte) error {
	// It's better to have a random extension to avoid race condition.
	randFileName := fileName + "." + uuid.Generate().String()
	if err := u.store.CreateUploadFile(randFileName, int64(len(content))); err != nil {
		return fmt.Errorf("create upload file: %s", err)
	}

	rw, err := u.store.GetUploadFileReadWriter(randFileName)
	if err != nil {
		return fmt.Errorf("get upload file: %s", err)
	}
	defer rw.Close()
	if _, err := rw.Write(content); err != nil {
		return fmt.Errorf("write content: %s", err)
	}
	if _, err := rw.Seek(0, 0); err != nil {
		return fmt.Errorf("seek: %s", err)
	}

	if err := u.store.MoveUploadFileToCache(randFileName, fileName); err != nil {
		if os.IsExist(err) {
			// It's okay to fail with "os.IsExist"
			return nil
		}
		return fmt.Errorf("move upload file to cache: %s", err)
	}
	f, err := u.store.GetCacheFileReader(fileName)
	if err != nil {
		return fmt.Errorf("get cache file: %s", err)
	}
	d, err := core.NewSHA256DigestFromHex(fileName)
	if err != nil {
		return fmt.Errorf("new digest: %s", err)
	}
	if err := u.transferer.Upload("TODO", d, f); err != nil {
		return fmt.Errorf("upload: %s", err)
	}
	return nil
}
