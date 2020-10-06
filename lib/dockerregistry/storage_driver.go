// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dockerregistry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/uber/kraken/lib/dockerregistry/transfer"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/log"

	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/uber-go/tally"
)

// The path layout in the storage backend is roughly as follows:
//
//		<root>/v2
//			-> repositories/
// 				-><name>/
// 					-> _manifests/
// 						revisions
//							-> <manifest digest path>
//								-> link
// 						tags/<tag>
//							-> current/link
// 							-> index
//								-> <algorithm>/<hex digest>/link
// 					-> _layers/
// 						<layer links to blob store>
// 					-> _uploads/<id>
// 						data
// 						startedat
// 						hashstates/<algorithm>/<offset>
//			-> blobs/<algorithm>
//				<split directory content addressable storage>

// Name of storage driver.
const Name = "kraken"

func init() {
	factory.Register(Name, &krakenStorageDriverFactory{})
}

// InvalidRequestError implements error and contains the path that is not supported
type InvalidRequestError struct {
	path string
}

func (e InvalidRequestError) Error() string {
	return fmt.Sprintf("invalid request: %s", e.path)
}

func toDriverError(err error, path string) error {
	if errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, transfer.ErrBlobNotFound) ||
		errors.Is(err, transfer.ErrTagNotFound) {
		return driver.PathNotFoundError{
			DriverName: Name,
			Path:       path,
		}
	}
	return err
}

type krakenStorageDriverFactory struct{}

func getParam(params map[string]interface{}, name string) interface{} {
	p, ok := params[name]
	if !ok || p == nil {
		log.Fatalf("Required parameter %s not found", name)
	}
	return p
}

func (factory *krakenStorageDriverFactory) Create(
	params map[string]interface{}) (driver.StorageDriver, error) {

	// Common parameters.
	constructor := getParam(params, "constructor").(string)
	config := getParam(params, "config").(Config)
	transferer := getParam(params, "transferer").(transfer.ImageTransferer)
	metrics := getParam(params, "metrics").(tally.Scope)

	switch constructor {
	case _rw:
		castore := getParam(params, "castore").(*store.CAStore)
		return NewReadWriteStorageDriver(config, castore, transferer, metrics), nil
	case _ro:
		blobstore := getParam(params, "blobstore").(BlobStore)
		return NewReadOnlyStorageDriver(config, blobstore, transferer, metrics), nil
	default:
		return nil, fmt.Errorf("unknown constructor %s", constructor)
	}
}

// KrakenStorageDriver is a storage driver
type KrakenStorageDriver struct {
	config     Config
	transferer transfer.ImageTransferer
	blobs      *blobs
	uploads    uploads
	manifests  *manifests
	metrics    tally.Scope
}

// NewReadWriteStorageDriver creates a KrakenStorageDriver which can push / pull blobs.
func NewReadWriteStorageDriver(
	config Config,
	cas *store.CAStore,
	transferer transfer.ImageTransferer,
	metrics tally.Scope) *KrakenStorageDriver {

	return &KrakenStorageDriver{
		config:     config,
		transferer: transferer,
		blobs:      newBlobs(cas, transferer),
		uploads:    newCASUploads(cas, transferer),
		manifests:  newManifests(transferer),
		metrics:    metrics,
	}
}

// NewReadOnlyStorageDriver creates a KrakenStorageDriver which can only pull blobs.
func NewReadOnlyStorageDriver(
	config Config,
	bs BlobStore,
	transferer transfer.ImageTransferer,
	metrics tally.Scope) *KrakenStorageDriver {

	return &KrakenStorageDriver{
		config:     config,
		transferer: transferer,
		blobs:      newBlobs(bs, transferer),
		uploads:    disabledUploads{},
		manifests:  newManifests(transferer),
		metrics:    metrics,
	}
}

// Name returns driver namae
func (d *KrakenStorageDriver) Name() string {
	return Name
}

// GetContent returns content in the path
// sample path: /docker/registry/v2/repositories/external/ubuntu/_layers/sha256/a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4/link
func (d *KrakenStorageDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	log.Debugf("(*KrakenStorageDriver).GetContent %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	var data []byte
	switch pathType {
	case _manifests:
		data, err = d.manifests.getDigest(path, pathSubType)
	case _uploads:
		data, err = d.uploads.getContent(path, pathSubType)
	case _layers:
		data, err = d.blobs.getDigest(path)
	case _blobs:
		data, err = d.blobs.getContent(ctx, path)
	default:
		return nil, InvalidRequestError{path}
	}
	if err != nil {
		return nil, toDriverError(err, path)
	}
	return data, nil
}

// Reader returns a reader of path at offset
func (d *KrakenStorageDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	log.Debugf("(*KrakenStorageDriver).Reader %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	var reader io.ReadCloser
	switch pathType {
	case _uploads:
		reader, err = d.uploads.reader(path, pathSubType, offset)
	case _blobs:
		reader, err = d.blobs.reader(ctx, path, offset)
	default:
		return nil, InvalidRequestError{path}
	}
	if err != nil {
		return nil, toDriverError(err, path)
	}
	return reader, nil
}

// PutContent writes content to path
func (d *KrakenStorageDriver) PutContent(ctx context.Context, path string, content []byte) error {
	log.Debugf("(*KrakenStorageDriver).PutContent %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return err
	}

	switch pathType {
	case _manifests:
		err = d.manifests.putContent(path, pathSubType)
	case _uploads:
		err = d.uploads.putContent(path, pathSubType, content)
	case _layers:
		// noop
		return nil
	case _blobs:
		err = d.uploads.putBlobContent(path, content)
	default:
		return InvalidRequestError{path}
	}
	if err != nil {
		return toDriverError(err, path)
	}
	return nil
}

// Writer returns a writer of path
func (d *KrakenStorageDriver) Writer(ctx context.Context, path string, append bool) (driver.FileWriter, error) {
	log.Debugf("(*KrakenStorageDriver).Writer %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	switch pathType {
	case _uploads:
		w, err := d.uploads.writer(path, pathSubType)
		if err != nil {
			return nil, toDriverError(err, path)
		}
		if append {
			if _, err := w.Seek(0, io.SeekEnd); err != nil {
				return nil, err
			}
		}
		return w, nil
	default:
		return nil, InvalidRequestError{path}
	}
}

// Stat returns fileinfo of path
func (d *KrakenStorageDriver) Stat(ctx context.Context, path string) (driver.FileInfo, error) {
	log.Debugf("(*KrakenStorageDriver).Stat %s", path)
	pathType, _, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	var info driver.FileInfo
	switch pathType {
	case _uploads:
		info, err = d.uploads.stat(path)
	case _blobs:
		info, err = d.blobs.stat(ctx, path)
	case _manifests:
		info, err = d.manifests.stat(path)
	default:
		return nil, InvalidRequestError{path}
	}
	if err != nil {
		return nil, toDriverError(err, path)
	}
	return info, nil
}

// List returns a list of content given path
func (d *KrakenStorageDriver) List(ctx context.Context, path string) ([]string, error) {
	log.Debugf("(*KrakenStorageDriver).List %s", path)
	pathType, pathSubType, err := ParsePath(path)
	if err != nil {
		return nil, err
	}

	var l []string
	switch pathType {
	case _uploads:
		l, err = d.uploads.list(path, pathSubType)
	case _manifests:
		l, err = d.manifests.list(path)
	default:
		return nil, InvalidRequestError{path}
	}
	if err != nil {
		return nil, toDriverError(err, path)
	}
	return l, nil
}

// Move moves sourcePath to destPath
func (d *KrakenStorageDriver) Move(ctx context.Context, sourcePath string, destPath string) error {
	log.Debugf("(*KrakenStorageDriver).Move %s %s", sourcePath, destPath)
	pathType, _, err := ParsePath(sourcePath)
	if err != nil {
		return err
	}

	switch pathType {
	case _uploads:
		err = d.uploads.move(sourcePath, destPath)
	default:
		return InvalidRequestError{sourcePath + " to " + destPath}
	}
	if err != nil {
		return toDriverError(err, sourcePath)
	}
	return nil
}

// Delete deletes path
func (d *KrakenStorageDriver) Delete(ctx context.Context, path string) error {
	log.Debugf("(*KrakenStorageDriver).Delete %s", path)
	return driver.PathNotFoundError{
		DriverName: Name,
		Path:       path,
	}
}

// URLFor returns url for path
func (d *KrakenStorageDriver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	log.Debugf("(*KrakenStorageDriver).URLFor %s", path)
	return "", fmt.Errorf("Not implemented")
}

// Walk is not implemented.
func (d *KrakenStorageDriver) Walk(ctx context.Context, path string, f driver.WalkFn) error {
	return errors.New("walk not implemented")
}
