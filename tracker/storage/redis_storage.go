package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/utils/log"
)

// RedisStorage errors.
var (
	ErrNoOrigins = errors.New("no origins found")
)

func peerSetKey(infoHash string, window int64) string {
	return fmt.Sprintf("peerset:%s:%d", infoHash, window)
}

func originsKey(infoHash string) string {
	return fmt.Sprintf("origins:%s", infoHash)
}

func metaInfoKey(name string) string {
	return fmt.Sprintf("metainfo:%s", name)
}

func serializePeer(p *torlib.PeerInfo) string {
	var completeBit int
	if p.Complete {
		completeBit = 1
	}
	return fmt.Sprintf("%s:%s:%d:%d", p.PeerID, p.IP, p.Port, completeBit)
}

type peerIdentity struct {
	PeerID string
	IP     string
	Port   int64
}

func deserializePeer(s string) (id peerIdentity, complete bool, err error) {
	parts := strings.Split(s, ":")
	if len(parts) != 4 {
		return id, false, fmt.Errorf("invalid peer encoding: expected 'pid:ip:port:complete'")
	}
	peerID := parts[0]
	ip := parts[1]
	port, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return id, false, fmt.Errorf("cannot parse port: %s", err)
	}
	id = peerIdentity{peerID, ip, port}
	complete = parts[3] == "1"
	return id, complete, nil
}

// RedisStorage provides fast lookup for peers and torrent metainfo with expiration.
type RedisStorage struct {
	config RedisConfig
	pool   *redis.Pool

	// Allow overriding time.Now() for testing purposes.
	now func() time.Time
}

// NewRedisStorage creates a RedisStorage instance.
func NewRedisStorage(config RedisConfig) (*RedisStorage, error) {
	config, err := config.applyDefaults()
	if err != nil {
		return nil, fmt.Errorf("configuration: %s", err)
	}

	log.Infof("Redis storage initializing with config:\n%s", config)

	s := &RedisStorage{
		config: config,
		pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				// TODO Add options
				return redis.Dial(
					"tcp",
					config.Addr,
					redis.DialConnectTimeout(config.DialTimeout),
					redis.DialReadTimeout(config.ReadTimeout),
					redis.DialWriteTimeout(config.WriteTimeout))
			},
			MaxIdle:     config.MaxIdleConns,
			MaxActive:   config.MaxActiveConns,
			IdleTimeout: config.IdleConnTimeout,
			Wait:        true,
		},
		now: time.Now,
	}

	// Ensure we can connect to Redis.
	c, err := s.pool.Dial()
	if err != nil {
		return nil, err
	}
	c.Close()

	return s, nil
}

func (s *RedisStorage) curPeerSetWindow() int64 {
	t := s.now().Unix()
	return t - (t % int64(s.config.PeerSetWindowSize.Seconds()))
}

func (s *RedisStorage) peerSetWindows() []int64 {
	cur := s.curPeerSetWindow()
	ws := make([]int64, s.config.MaxPeerSetWindows)
	for i := range ws {
		ws[i] = cur - int64(i)*int64(s.config.PeerSetWindowSize.Seconds())
	}
	return ws
}

// UpdatePeer writes p to Redis with a TTL.
func (s *RedisStorage) UpdatePeer(p *torlib.PeerInfo) error {
	c, err := s.pool.Dial()
	if err != nil {
		return err
	}
	defer c.Close()

	w := s.curPeerSetWindow()
	expireAt := w + int64(s.config.PeerSetWindowSize.Seconds())*int64(s.config.MaxPeerSetWindows)

	// Add p to the current window.
	k := peerSetKey(p.InfoHash, w)
	c.Send("MULTI")
	c.Send("SADD", k, serializePeer(p))
	c.Send("EXPIREAT", k, expireAt)
	_, err = c.Do("EXEC")

	return err
}

// GetPeers returns all PeerInfos associated with infoHash.
func (s *RedisStorage) GetPeers(infoHash string) ([]*torlib.PeerInfo, error) {
	c, err := s.pool.Dial()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// Eliminate duplicates from other windows and collapses complete bits.
	peers := make(map[peerIdentity]bool)
	for _, w := range s.peerSetWindows() {
		result, err := redis.Strings(c.Do("SMEMBERS", peerSetKey(infoHash, w)))
		if err != nil {
			return nil, err
		}
		for _, s := range result {
			id, complete, err := deserializePeer(s)
			if err != nil {
				log.Errorf("Error deserializing peer %q: %s", s, err)
				continue
			}
			peers[id] = peers[id] || complete
		}
	}

	peerInfos := make([]*torlib.PeerInfo, 0, len(peers))
	for id, complete := range peers {
		p := &torlib.PeerInfo{
			InfoHash: infoHash,
			PeerID:   id.PeerID,
			IP:       id.IP,
			Port:     id.Port,
			Complete: complete,
		}
		peerInfos = append(peerInfos, p)
	}

	return peerInfos, nil
}

// GetOrigins returns all origin PeerInfos for infoHash. Returns ErrNoOrigins if
// no origins exist in Redis.
func (s *RedisStorage) GetOrigins(infoHash string) ([]*torlib.PeerInfo, error) {
	c, err := s.pool.Dial()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	result, err := redis.String(c.Do("GET", originsKey(infoHash)))
	if err != nil {
		if err == redis.ErrNil {
			return nil, ErrNoOrigins
		}
		return nil, err
	}

	var peerInfos []*torlib.PeerInfo
	for _, s := range strings.Split(result, ",") {
		id, complete, err := deserializePeer(s)
		if err != nil {
			log.Errorf("Error deserializing origin %q: %s", s, err)
			continue
		}
		p := &torlib.PeerInfo{
			InfoHash: infoHash,
			PeerID:   id.PeerID,
			IP:       id.IP,
			Port:     id.Port,
			Complete: complete,
			Origin:   true,
		}
		peerInfos = append(peerInfos, p)
	}
	return peerInfos, nil
}

// UpdateOrigins overwrites all origin PeerInfos for infoHash with the given origins.
func (s *RedisStorage) UpdateOrigins(infoHash string, origins []*torlib.PeerInfo) error {
	c, err := s.pool.Dial()
	if err != nil {
		return err
	}
	defer c.Close()

	var serializedOrigins []string
	for _, o := range origins {
		serializedOrigins = append(serializedOrigins, serializePeer(o))
	}
	v := strings.Join(serializedOrigins, ",")

	_, err = c.Do("SETEX", originsKey(infoHash), int(s.config.OriginsTTL.Seconds()), v)
	return err
}

// GetMetaInfo returns metainfo for the given file name. Returns ErrNotFound if
// no metainfo exists for name.
func (s *RedisStorage) GetMetaInfo(name string) ([]byte, error) {
	c, err := s.pool.Dial()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	b, err := redis.Bytes(c.Do("GET", metaInfoKey(name)))
	if err != nil {
		if err == redis.ErrNil {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return b, nil
}

// SetMetaInfo writes metainfo. Returns ErrExists if metainfo already exists for
// mi's file name.
func (s *RedisStorage) SetMetaInfo(mi *torlib.MetaInfo) error {
	c, err := s.pool.Dial()
	if err != nil {
		return err
	}
	defer c.Close()

	b, err := mi.Serialize()
	if err != nil {
		return fmt.Errorf("serialize metainfo: %s", err)
	}

	n, err := redis.Int(c.Do("SETNX", metaInfoKey(mi.Name()), b))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrExists
	}
	return nil
}
