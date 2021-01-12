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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
)

func generateSingleTag(sqlClient *Client, repo string, tag string) Tag {
	imageID := core.DigestFixture().String()
	r := strings.NewReader(imageID)
	err := sqlClient.Upload("", fmt.Sprintf("%s:%s", repo, tag), r)
	if err != nil {
		panic(err)
	}
	return Tag{Repository: repo, Tag: tag, ImageID: imageID}
}

func newClient() *Client {
	sqlClient, err := NewClient(Config{Dialect: "sqlite3", ConnectionString: ":memory:"}, UserAuthConfig{})
	if err != nil {
		panic(err)
	}
	return sqlClient
}

func TestClientFactory(t *testing.T) {
	config := Config{Dialect: "sqlite3", ConnectionString: ":memory:"}

	f := factory{}
	_, err := f.Create(config, nil)
	require.NoError(t, err)
}

func TestClientFactoryAuth(t *testing.T) {
	config := Config{Dialect: "sqlite3", ConnectionString: ":memory:"}

	f := factory{}
	_, err := f.Create(config, nil)
	require.NoError(t, err)
}

func TestGetDBConnectionString(t *testing.T) {
	testCases := map[string]struct {
		cfg     Config
		authCfg UserAuthConfig
		connStr string
		wantErr string
	}{
		"full connection string": {
			cfg: Config{
				Username:         "testuser",
				ConnectionString: "avengers.net/users",
			},
			authCfg: UserAuthConfig{
				"testuser": AuthConfig{
					SQL: SQL{
						User:     "captain_marvel",
						Password: "higher_further_faster",
					},
				},
			},
			connStr: "captain_marvel:higher_further_faster@avengers.net/users",
		},
	}
	for testName, tt := range testCases {
		t.Run(testName, func(t *testing.T) {
			connStr, err := getDBConnectionString(tt.cfg, tt.authCfg)

			if tt.wantErr != "" {
				assert.Equal(t, "", connStr)
				assert.EqualError(t, err, tt.wantErr)
			}

			if tt.connStr != "" {
				assert.Nil(t, err)
				assert.Equal(t, tt.connStr, connStr)
			}
		})
	}
}

func TestStat(t *testing.T) {
	sqlClient := newClient()
	tag := generateSingleTag(sqlClient, "batman", "robin")

	res, err := sqlClient.Stat("", fmt.Sprintf("%s:%s", tag.Repository, tag.Tag))
	assert.NotNil(t, res)
	assert.NoError(t, err)
}

func TestStatNotExist(t *testing.T) {
	res, err := newClient().Stat("", "bad-repo:bad-tag")
	assert.Nil(t, res)
	assert.EqualError(t, err, backenderrors.ErrBlobNotFound.Error())
}

func TestStatBadTagName(t *testing.T) {
	res, err := newClient().Stat("", "this_is_wrong:")
	assert.Nil(t, res)
	assert.Error(t, err)
}

func TestDownload(t *testing.T) {
	sqlClient := newClient()
	tag := generateSingleTag(sqlClient, "ironman", "mk-vii")

	w := new(bytes.Buffer)
	err := sqlClient.Download("", fmt.Sprintf("%s:%s", tag.Repository, tag.Tag), w)
	assert.NoError(t, err)
	assert.Equal(t, tag.ImageID, w.String())
}

func TestDownloadNotExist(t *testing.T) {
	w := new(bytes.Buffer)
	err := newClient().Download("", "bad-repo:bad-tag", w)
	assert.EqualError(t, err, backenderrors.ErrBlobNotFound.Error())
}

func TestDownloadBadTagName(t *testing.T) {
	w := new(bytes.Buffer)
	err := newClient().Download("", ":this_is_wrong", w)
	assert.Error(t, err)
}

func TestUploadNewAndUpdateTag(t *testing.T) {
	sqlClient := newClient()
	newRepoTag := "new-repo:new-tag"
	res, err := sqlClient.Stat("", newRepoTag)
	assert.Nil(t, res)
	assert.EqualError(t, err, backenderrors.ErrBlobNotFound.Error())

	// Upload new tag
	imageID := "scarlet_witch"
	err = sqlClient.Upload("", newRepoTag, strings.NewReader(imageID))
	assert.NoError(t, err)

	w := new(bytes.Buffer)
	err = sqlClient.Download("", newRepoTag, w)
	assert.NoError(t, err)
	assert.Equal(t, imageID, w.String())

	// Update existing tag
	newImageID := "scarlet_witch_and_vision"
	err = sqlClient.Upload("", newRepoTag, strings.NewReader(newImageID))
	require.NoError(t, err)

	w = new(bytes.Buffer)
	err = sqlClient.Download("", newRepoTag, w)
	assert.NoError(t, err)
	assert.Equal(t, newImageID, w.String())
}

func TestUploadBadTagName(t *testing.T) {
	err := newClient().Upload("", "::this_is_wrong:::", strings.NewReader("bleh"))
	assert.Error(t, err)
}

func TestListCatalog(t *testing.T) {
	sqlClient := newClient()
	tags := []string{
		"a:1",
		"a:2",
		"b:1",
		"c:1",
	}
	for _, tag := range tags {
		b := bytes.NewBufferString(core.DigestFixture().String())
		require.NoError(t, sqlClient.Upload("", tag, b))
	}
	res, err := sqlClient.List("")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a:dummy", "b:dummy", "c:dummy"}, res.Names)
}

func TestListTags(t *testing.T) {
	sqlClient := newClient()
	tags := []string{
		"a:1",
		"a:2",
		"b:1",
		"c:1",
	}
	for _, tag := range tags {
		b := bytes.NewBufferString(core.DigestFixture().String())
		require.NoError(t, sqlClient.Upload("", tag, b))
	}
	res, err := sqlClient.List("a/_manifests/tags")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a:1", "a:2"}, res.Names)
}

func TestListTagsNotFound(t *testing.T) {
	res, err := newClient().List("no-tag-exists/_manifests/tags")
	require.NotNil(t, res)
	assert.NoError(t, err)

	assert.Equal(t, len(res.Names), 0)
}

func TestListBadTags(t *testing.T) {
	sqlClient := newClient()
	res, err := sqlClient.List("many-tags/_manifests/tagz")
	require.NotNil(t, res)
	assert.NoError(t, err)

	assert.Equal(t, len(res.Names), 0)

	res, err = sqlClient.List("many-tags/_manifests/tags/whoops")
	require.NotNil(t, res)
	assert.NoError(t, err)

	assert.Equal(t, len(res.Names), 0)

	res, err = sqlClient.List("ThIs/Is/_VeRy/BaD")
	require.NotNil(t, res)
	assert.NoError(t, err)

	assert.Equal(t, len(res.Names), 0)
}
