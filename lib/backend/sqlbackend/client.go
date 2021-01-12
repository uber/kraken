// Copyright (c) 2016-2020 Uber Technologies, Inc.
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
package sqlbackend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jinzhu/gorm"
	// Import mysql and sqlite to register them with GORM
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"gopkg.in/yaml.v2"
)

type factory struct{}

func (f *factory) Name() string {
	return "sql"
}

func (f *factory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal sql config")
	}

	authConfBytes, err := yaml.Marshal(authConfRaw)
	if err != nil {
		return nil, errors.New("marshal s3 auth config")
	}

	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal sql config")
	}
	var userAuth UserAuthConfig
	if err := yaml.Unmarshal(authConfBytes, &userAuth); err != nil {
		return nil, errors.New("unmarshal s3 auth config")
	}

	return NewClient(config, userAuth)
}

// Client implements a backend.Client for SQL.
type Client struct {
	cfg Config
	db  *gorm.DB
}

// NewClient creates a new Client for a SQL database.
func NewClient(config Config, authConfig UserAuthConfig) (*Client, error) {
	conStr, err := getDBConnectionString(config, authConfig)
	if err != nil {
		return nil, fmt.Errorf("error building database connection string: %v", err)
	}

	db, err := gorm.Open(config.Dialect, conStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}
	db.AutoMigrate(Tag{})

	db.LogMode(config.DebugLogging)

	client := &Client{config, db}
	return client, nil
}

func getDBConnectionString(config Config, authConfig UserAuthConfig) (string, error) {
	conn := config.ConnectionString
	if conn == "" {
		return "", errors.New("connection_string is not set")
	}

	auth, ok := authConfig[config.Username]
	if ok {
		user := auth.SQL.User
		pass := auth.SQL.Password

		if user == "" && pass != "" {
			return "", errors.New("must specify DB user when specifying DB password")
		}

		if user != "" && pass == "" {
			return fmt.Sprintf("%s@%s", user, conn), nil
		}

		if user != "" && pass != "" {
			return fmt.Sprintf("%s:%s@%s", user, pass, conn), nil
		}
	}

	return conn, nil
}

func decomposeDockerTag(name string) (string, string, error) {
	tokens := strings.Split(name, ":")
	if len(tokens) != 2 {
		return "", "", errors.New("name must be in format 'repo:tag'")
	}
	repo := tokens[0]
	if len(repo) == 0 {
		return "", "", errors.New("repo must be non-empty")
	}
	tag := tokens[1]
	if len(tag) == 0 {
		return "", "", errors.New("tag must be non-empty")
	}

	return repo, tag, nil
}

// Stat returns a non-nil core.BlobInfo struct if the tag exists, an error otherwise.
func (c *Client) Stat(_, name string) (*core.BlobInfo, error) {
	repo, tag, err := decomposeDockerTag(name)
	if err != nil {
		return nil, fmt.Errorf("tag path: %s. Err was %s", name, err)
	}

	gormTag := Tag{}
	res := c.db.
		Where(Tag{Repository: repo, Tag: tag}).
		First(&gormTag)

	if res.RecordNotFound() {
		return nil, backenderrors.ErrBlobNotFound
	}

	if res.Error != nil {
		return nil, res.Error
	}

	var size int64
	return core.NewBlobInfo(size), nil
}

// Download gets the tag from the database and then writes the image ID to the output writer.
func (c *Client) Download(_, name string, dst io.Writer) error {
	repo, tag, err := decomposeDockerTag(name)
	if err != nil {
		return fmt.Errorf("tag path: %s. Err was %s", name, err)
	}

	gormTag := Tag{}
	res := c.db.
		Where(Tag{Repository: repo, Tag: tag}).
		First(&gormTag)

	if res.RecordNotFound() {
		return backenderrors.ErrBlobNotFound
	}

	if res.Error != nil {
		return res.Error
	}

	if _, err := dst.Write([]byte(gormTag.ImageID)); err != nil {
		return err
	}

	return nil
}

// Upload upserts the tag into the database.
func (c *Client) Upload(_, name string, src io.Reader) error {
	repo, tag, err := decomposeDockerTag(name)
	if err != nil {
		return fmt.Errorf("tag path: %s. Err was %s", name, err)
	}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(src); err != nil {
		return err
	}
	imageID := buf.String()

	gormTag := Tag{
		Repository: repo,
		Tag:        tag,
		ImageID:    imageID,
	}

	// If not found, insert new tag into the database.
	// If found, update the image ID
	res := c.db.
		Where(Tag{Repository: repo, Tag: tag}).
		Assign(Tag{ImageID: imageID}).
		FirstOrCreate(&gormTag)

	if res.Error != nil {
		return res.Error
	}

	return nil
}

// List lists names with start with prefix.
func (c *Client) List(prefix string, _ ...backend.ListOption) (*backend.ListResult, error) {

	switch prefix {
	case "":
		return dockerCatalogQuery(c)
	default:
		return dockerTagsQuery(c, prefix)
	}
}

func dockerCatalogQuery(c *Client) (*backend.ListResult, error) {
	gormTags := make([]Tag, 0)
	res := c.db.
		Select("DISTINCT(repository)").
		Order("repository").
		Find(&gormTags)

	if res.Error != nil {
		return nil, res.Error
	}

	names := make([]string, len(gormTags))
	for i, tag := range gormTags {
		// this is dumb, but the consumers of List expect results to take the form of "repo:tag" even if we are just
		// listing repositories, so we have to attach a dummy tag
		names[i] = fmt.Sprintf("%s:%s", tag.Repository, "dummy")
	}

	return &backend.ListResult{
		Names: names,
	}, nil
}

func dockerTagsQuery(c *Client, prefix string) (*backend.ListResult, error) {
	gormTags := make([]Tag, 0)
	// prefix takes the form /<repository>/_manifests/tags, so we strip the useless stuff
	t := strings.TrimSuffix(prefix, "/_manifests/tags")
	repo := strings.TrimPrefix(t, "/")
	res := c.db.
		Select("tag").
		Where("repository = ?", repo).
		Order("tag").
		Find(&gormTags)

	if res.Error != nil {
		return nil, res.Error
	}

	names := make([]string, len(gormTags))
	for i, tag := range gormTags {
		names[i] = fmt.Sprintf("%s:%s", repo, tag.Tag)
	}

	return &backend.ListResult{
		Names: names,
	}, nil
}
