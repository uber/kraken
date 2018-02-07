package trackerserver

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

const manifestStr = `{
                 "schemaVersion": 2,
                 "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                 "config": {
                    "mediaType": "application/octet-stream",
                    "size": 11936,
                    "digest": "sha256:d2176faa6180566e5e6727e101ba26b13c19ef35f171c9b4419c4d50626aad9d"
                 },
                 "layers": [{
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 52998821,
                    "digest": "sha256:1508613826413590a9fdb496cbedb0c2ebf564cfbcd2c85c2a07bb3a40813233"
                 },
                 {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 115242848,
                    "digest": "sha256:f1f1d5da237f1b069eae23cdc9b291e217a4c1fda8f29262c4275a786a4dd322"
                  }]}`

func TestGetManifestHandler(t *testing.T) {
	name := "repo:tag1"

	t.Run("Return 400 on empty tag name", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/manifest/", nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()
		getResponse := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)
	})

	t.Run("Return 404 on manifest not found", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/manifest/"+name, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().GetManifest(name).Return("", os.ErrNotExist)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 404, response.StatusCode)
	})

	t.Run("Return 200 and manifest", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/manifest/"+name, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().GetManifest(name).Return(manifestStr, nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
		data, _ := ioutil.ReadAll(response.Body)
		var j1, j2 interface{}
		j1, err := json.Marshal(data)
		assert.Equal(t, err, nil)
		err = json.Unmarshal([]byte(manifestStr), &j2)
		assert.Equal(t, err, nil)

		result := reflect.DeepEqual(j1, j1)
		assert.Equal(t, result, true)
	})
}

func TestPostManifestHandler(t *testing.T) {
	name := "tag1"

	t.Run("Return 400 on invalid manifest", func(t *testing.T) {
		getRequest, _ := http.NewRequest("POST",
			"/manifest/"+name, bytes.NewBuffer([]byte("")))

		mocks := &testMocks{}
		defer mocks.mockController(t)()
		getResponse := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)
	})

	t.Run("Return 200", func(t *testing.T) {
		getRequest, _ := http.NewRequest("POST",
			"/manifest/"+name, bytes.NewBuffer([]byte(manifestStr)))

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().CreateManifest(name, manifestStr).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
	})
}
