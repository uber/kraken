package tracker

import (
	"net/http"

	"fmt"

	"strconv"

	"strings"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/configuration"
	"code.uber.internal/infra/kraken/utils"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
)

// NotFoundError occurs when tracker cannot get the information
type NotFoundError struct {
	msg string
}

// NewNotFoundError creates NotFoundError with message
func NewNotFoundError(message string) error {
	return &NotFoundError{msg: message}
}

func (e *NotFoundError) Error() string {
	return e.msg
}

type httpResponse struct {
	//FailureReason string      `bencode:"failure reason"`
	Interval int `bencode:"interval"`
	//TrackerID  string      `bencode:"tracker id"`
	Complete   int         `bencode:"complete"`
	Incomplete int         `bencode:"incomplete"`
	Peers      interface{} `bencode:"peers"`
}

// Peer contains peer ip and port number
type Peer map[string]interface{}

// Tracker receives a docker image, breaks it into torrents and register torrents with tracker
type Tracker struct {
	config *configuration.Config
	redis  redis.Conn
}

// NewTracker creates a new Exporter
func NewTracker(config *configuration.Config, conn redis.Conn) *Tracker {
	return &Tracker{
		config: config,
		redis:  conn,
	}
}

// Serve serve
func (ex *Tracker) Serve() {
	router := mux.NewRouter()
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("OK"))
	}).Methods("GET")
	router.HandleFunc("/announce", ex.Announce).Methods("GET")
	log.Infof("Tracker listening at %s", ex.config.Announce)
	log.Fatal(http.ListenAndServe(ex.config.Announce, router))
}

// Announce returns a list of peers that has requested piece
func (ex *Tracker) Announce(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	// split host and port
	hostport := strings.SplitN(host, ":", 2)
	if len(hostport) > 2 || len(hostport) < 1 {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("bad host: %s", host)))
		log.Error(fmt.Sprintf("bad host: %s", host))
		return
	}
	host = hostport[0]
	q := r.URL.Query()
	ih := q.Get("info_hash")
	port := q.Get("port")

	ret, err := ex.redis.Do("lrange", ih, 0, -1)
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Error(err.Error())
		return
	}

	if ret == nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Empty peer list"))
		log.Error("Empty peer list")
		return
	}

	peersStr := ret.([]interface{})
	found := false

	var peers []Peer
	for _, p := range peersStr {
		log.Infof("Peer %s", p.([]byte))
		var peer Peer
		err := bencode.Unmarshal(p.([]byte), &peer)
		if peer["ip"] == host && fmt.Sprintf("%d", peer["port"]) == port {
			found = true
		}
		if err != nil {
			log.Error(err.Error())
			continue
		}
		peers = append(peers, peer)
	}

	if len(peers) == 0 {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("0 peers"))
		log.Error("0 peers")
		return
	}

	if !found {
		// Add peer to list
		ex.AddPeer(ih, host, port)
	}

	respBody := httpResponse{
		Interval: ex.config.AnnounceInterval,
		Peers:    peers,
	}

	data, err := bencode.Marshal(respBody)
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Error(err.Error())
		return
	}

	log.Infof("Response %s", data)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
	return
}

// GetMagnet handles requests received to get magnet uri string
func (ex *Tracker) GetMagnet(key string) (string, error) {
	val, err := ex.redis.Do("get", key)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", NewNotFoundError(fmt.Sprintf("Cannot find magnet with key %s", key))
	}

	log.Infof("%s", val)

	return fmt.Sprintf("%s", val), nil
}

// AddPeer adds peer to list given info hash
func (ex *Tracker) AddPeer(ih, host, port string) error {
	portno, err := strconv.ParseInt(port, 10, 64)
	p := Peer{
		"ip":      host,
		"peer id": host,
		"port":    portno,
	}

	peerBlob, err := bencode.Marshal(p)
	if err != nil {
		return err
	}
	_, err = ex.redis.Do("rpush", ih, string(peerBlob[:]))
	if err != nil {
		return err
	}

	return nil
}

// GetDigestFromRepoTag returns digest string (sha256) given repo and tag
func (ex *Tracker) GetDigestFromRepoTag(repo string, tag string) (string, error) {
	sha, err := ex.redis.Do("get", fmt.Sprintf("%s:%s", repo, tag))
	if err != nil {
		return "", err
	}

	if sha == nil {
		return "", NewNotFoundError(fmt.Sprintf("Cannot get digest from repo %s tag %s", repo, tag))
	}

	return fmt.Sprintf("%s", sha), nil
}

// SetDigestForRepoTag set digest for given repo and tag
func (ex *Tracker) SetDigestForRepoTag(repo string, tag string, digest string) error {
	ok, err := ex.redis.Do("setex", fmt.Sprintf("%s:%s", repo, tag), ex.config.ExpireSec, digest)
	if err != nil {
		return err
	}

	if ok.(string) != "OK" {
		return fmt.Errorf("Failed to set repo %s tag %s for digest %s for %d", repo, tag, digest, ex.config.ExpireSec)
	}
	return nil
}

// SetRepoTag set tag for repo
func (ex *Tracker) SetRepoTag(repo string, tag string) error {
	_, err := ex.redis.Do("rpush", repo, tag)
	if err != nil {
		return err
	}

	return nil
}

// GetRepoTags returns list of tags
func (ex *Tracker) GetRepoTags(repo string) ([]string, error) {
	ret, err := ex.redis.Do("lrange", repo, 0, -1)
	if err != nil {
		return nil, err
	}

	if ret == nil {
		return nil, nil
	}

	var s []string

	for _, tag := range ret.([]interface{}) {
		s = append(s, string(tag.([]uint8)))
	}

	return s, nil
}

// AddRepo append repo given to repo list
func (ex *Tracker) AddRepo(repo string) error {
	_, err := ex.redis.Do("rpush", "REPOSITORIES", repo)
	if err != nil {
		return err
	}

	return nil
}

// GetRepos returns a list of repos
func (ex *Tracker) GetRepos() ([]string, error) {
	ret, err := ex.redis.Do("lrange", "REPOSITORIES", 0, -1)
	if err != nil {
		return nil, err
	}

	if ret == nil {
		return nil, nil
	}

	var s []string

	for _, tag := range ret.([]interface{}) {
		s = append(s, fmt.Sprintf("repositories/%s", tag.([]uint8)))
	}

	return s, nil
}

// CreateTorrent creates torrent and register with db
func (ex *Tracker) CreateTorrent(key string, fp string) error {
	info := metainfo.Info{
		PieceLength: int64(ex.config.PieceLength),
	}
	err := info.BuildFromFilePath(fp)
	if err != nil {
		return err
	}

	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		return err
	}

	log.Debugf("InfoBytes: %s", infoBytes)

	mi := metainfo.MetaInfo{
		InfoBytes: infoBytes,
		Announce:  "http://" + ex.config.Announce + "/announce",
	}

	ih := mi.HashInfoBytes()

	magnetURI := mi.Magnet(key, ih)

	// store key - magnet uri
	ok, err := ex.redis.Do("setex", key, ex.config.ExpireSec, magnetURI.String())
	if err != nil {
		return err
	}

	if ok.(string) != "OK" {
		return fmt.Errorf("Failed to set key %s val %s for %d", key, magnetURI.String(), ex.config.ExpireSec)
	}

	// store infoHash - peerlist
	// get host
	var hn string
	if ex.config.Environment == "development" {
		hn = "127.0.0.1"
	} else {
		hn, err = utils.GetHostName()
		if err != nil {
			return err
		}
	}

	// get port number
	port, err := ex.config.GetClientPort()
	if err != nil {
		log.Error(err.Error())
		return err
	}

	return ex.AddPeer(ih.AsString(), hn, port)
}
