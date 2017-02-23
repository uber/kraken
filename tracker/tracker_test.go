package tracker

import (
	"testing"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/require"

	"io/ioutil"
	"os"

	"code.uber.internal/infra/kraken/configuration"
)

type TestConn struct {
	reply interface{}
	err   error
}

func (tc *TestConn) SetReply(r interface{}, e error) {
	tc.reply = r
	tc.err = e
	return
}

func (tc *TestConn) Close() error {
	return nil
}

func (tc *TestConn) Err() error {
	return nil
}

func (tc *TestConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return tc.reply, tc.err
}

func (tc *TestConn) Send(commandName string, args ...interface{}) error {
	return nil
}

func (tc *TestConn) Flush() error {
	return nil
}

func (tc *TestConn) Receive() (reply interface{}, err error) {
	return tc.reply, tc.err
}

/*
func TestAnnounce(t *testing.T) {
	assert := require.New(t)
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	redis := &TestConn{}
	peer1, _ := json.Marshal(Peer{
		IP:   "host1",
		Port: 5055,
	})
	peer2, _ := json.Marshal(Peer{
		IP:   "host2",
		Port: 5056,
	})
	redis.SetReply([]string{string(peer1[:]), string(peer2[:])}, nil)
	tracker := NewTracker(c, redis)
	r := httptest.NewRequest("GET", "/announce", nil)
	r.URL.Query().Set("info_hash", "asdf1")
	r.URL.Query().Set("port", "5059")
	w := httptest.NewRecorder()
	tracker.Announce(w, r)
	assert.Equal("d8:Intervali300e8:Leechersi0e5:Peersld2:IDle2:IPle4:Porti5055eed2:IDle2:IPle4:Porti5056eee7:Seedersi0ee", w.Body.String())

	// 0 peers
	tracker.redis.(*TestConn).SetReply([]string{}, nil)
	r = httptest.NewRequest("GET", "/announce", nil)
	r.URL.Query().Set("info_hash", "asdf1")
	r.URL.Query().Set("port", "5059")
	w = httptest.NewRecorder()
	tracker.Announce(w, r)
	assert.Equal("0 peers", w.Body.String())
}*/

func TestGetMagnet(t *testing.T) {
	assert := require.New(t)
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	r := &TestConn{}
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) { return r, nil },
	}
	r.SetReply("magnet?blah", nil)
	tracker := NewTracker(c, pool)
	val, _ := tracker.GetMagnet("key1")
	assert.Equal("magnet?blah", val)
}

func TestAddPeer(t *testing.T) {
	assert := require.New(t)
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	r := &TestConn{}
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) { return r, nil },
	}
	tracker := NewTracker(c, pool)
	assert.Nil(tracker.AddPeer("sha1", "host1", "5000"))
}

func TestCreateTorrent(t *testing.T) {
	assert := require.New(t)
	dir, _ := os.Getwd()
	f, _ := ioutil.TempFile(dir, "testcreatetorrent1")
	f.Write([]byte("hello!"))
	f.Close()
	defer os.Remove(f.Name())
	cp := configuration.GetConfigFilePath("test.yaml")
	c := configuration.NewConfig(cp)
	r := &TestConn{}
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) { return r, nil },
	}
	r.SetReply("Sorry", nil)
	tracker := NewTracker(c, pool)
	err := tracker.CreateTorrent("key1", f.Name())
	assert.NotNil(err)
	//re := regexp.MustCompile("Failed to set key key1 val magnet.*dn=key1.*announce for 604800")
	//assert.True(re.Match([]byte(err.Error())))
}
