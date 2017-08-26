package service

import (
	"bytes"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/torlib"
	"github.com/stretchr/testify/assert"
)

func TestGetMetaInfoHandler(t *testing.T) {
	mi := torlib.MetaInfoFixture()
	serialized, err := mi.Serialize()
	assert.Nil(t, err)
	name := mi.Name()

	t.Run("Return 400 on empty name", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/info?name=", nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()
		getResponse := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)
	})

	t.Run("Return 404 on name not found", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/info?name="+name, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().GetTorrent(name).Return("", os.ErrNotExist)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 404, response.StatusCode)
	})

	t.Run("Return 200 and info hash", func(t *testing.T) {
		getRequest, _ := http.NewRequest("GET",
			"/info?name="+name, nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().GetTorrent(name).Return(serialized, nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
		data := make([]byte, len(serialized))
		response.Body.Read(data)
		assert.Equal(t, serialized, string(data[:]))
	})
}

func TestPostMetaInfoHandler(t *testing.T) {
	mi := torlib.MetaInfoFixture()
	infoHash := mi.InfoHash
	serialized, err := mi.Serialize()
	assert.Nil(t, err)
	name := mi.Name()

	t.Run("Return 400 on empty name or infohash", func(t *testing.T) {
		getRequest, _ := http.NewRequest("POST",
			"/info?name=", nil)

		mocks := &testMocks{}
		defer mocks.mockController(t)()
		getResponse := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)

		getRequest, _ = http.NewRequest("POST",
			"/info?name="+name, nil)
		getResponse = mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 400, getResponse.StatusCode)
	})

	t.Run("Return 200", func(t *testing.T) {
		getRequest, _ := http.NewRequest("POST",
			"/info?name="+name+"&info_hash="+infoHash.HexString(), bytes.NewBuffer([]byte(serialized)))

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().CreateTorrent(mi).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
	})
}
