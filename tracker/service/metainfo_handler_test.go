package service

import (
	"bytes"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/torlib"
	"github.com/stretchr/testify/assert"
)

const metaStr = `d8:announce10:trackerurl4:infod6:lengthi2e4:name8:torrent012:piece lengthi1e6:pieces0:eePASS`

func TestGetMetaInfoHandler(t *testing.T) {
	_, err := torlib.NewMetaInfoFromBytes([]byte(metaStr))
	assert.Nil(t, err)
	name := "asdfhjkl"

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

		mocks.datastore.EXPECT().GetTorrent(name).Return(metaStr, nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
		data := make([]byte, len(metaStr))
		response.Body.Read(data)
		assert.Equal(t, metaStr, string(data[:]))
	})
}

func TestPostMetaInfoHandler(t *testing.T) {
	mi, err := torlib.NewMetaInfoFromBytes([]byte(metaStr))
	assert.Nil(t, err)
	infoHash := mi.InfoHash
	name := "asdfhjkl"

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
			"/info?name="+name+"&info_hash="+infoHash.HexString(), bytes.NewBuffer([]byte(metaStr)))

		mocks := &testMocks{}
		defer mocks.mockController(t)()

		mocks.datastore.EXPECT().CreateTorrent(mi).Return(nil)
		response := mocks.CreateHandlerAndServeRequest(getRequest)
		assert.Equal(t, 200, response.StatusCode)
	})
}
