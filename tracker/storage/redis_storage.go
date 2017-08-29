package storage

import (
	"fmt"
	"time"

	config "code.uber.internal/infra/kraken/config/tracker"
	"code.uber.internal/infra/kraken/torlib"
	"github.com/garyburd/redigo/redis"
)

func peerKey(infoHash string, peerID string) string {
	return fmt.Sprintf("peer:%s:%s", infoHash, peerID)
}

func torrentKey(name string) string {
	return fmt.Sprintf("tor:%s", name)
}

func peerSetKey(infoHash string, window int64) string {
	return fmt.Sprintf("peerset:%s:%d", infoHash, window)
}

// RedisStorage provides fast lookup for peers and torrent metainfo with expiration.
type RedisStorage struct {
	cfg  config.RedisConfig
	pool *redis.Pool

	// Allow overriding time.Now() for testing purposes.
	now func() time.Time
}

// NewRedisStorage creates a RedisStorage instance.
func NewRedisStorage(cfg config.RedisConfig) (*RedisStorage, error) {
	s := &RedisStorage{
		cfg: cfg,
		pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				// TODO Add options
				return redis.Dial("tcp", cfg.Addr)
			},
			MaxIdle:     cfg.MaxIdleConns,
			MaxActive:   cfg.MaxActiveConns,
			IdleTimeout: time.Duration(cfg.IdleConnTimeoutSecs) * time.Second,
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
	return t - (t % int64(s.cfg.PeerSetWindowSizeSecs))
}

func (s *RedisStorage) peerSetWindows() []int64 {
	cur := s.curPeerSetWindow()
	ws := make([]int64, s.cfg.MaxPeerSetWindows)
	for i := range ws {
		ws[i] = cur - int64(i*s.cfg.PeerSetWindowSizeSecs)
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
	expireAt := w + int64(s.cfg.PeerSetWindowSizeSecs*s.cfg.MaxPeerSetWindows)

	c.Send("MULTI")

	// Add p to the current window.
	psk := peerSetKey(p.InfoHash, w)
	c.Send("SADD", psk, p.PeerID)
	c.Send("EXPIREAT", psk, expireAt)

	// Update the fields of p.
	pk := peerKey(p.InfoHash, p.PeerID)
	c.Send("HMSET", redis.Args{}.Add(pk).AddFlat(p)...)
	c.Send("EXPIREAT", pk, expireAt)

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

	peerIDs := make(map[string]struct{})
	for _, w := range s.peerSetWindows() {
		result, err := redis.Strings(c.Do("SMEMBERS", peerSetKey(infoHash, w)))
		if err != nil {
			return nil, err
		}
		for _, peerID := range result {
			peerIDs[peerID] = struct{}{}
		}
	}

	peers := make([]*torlib.PeerInfo, 0, len(peerIDs))
	for peerID := range peerIDs {
		result, err := redis.Values(c.Do("HGETALL", peerKey(infoHash, peerID)))
		if err != nil {
			return nil, err
		}
		if len(result) == 0 {
			// Peer no longer exists.
			continue
		}
		p := &torlib.PeerInfo{
			InfoHash: infoHash,
			PeerID:   peerID,
		}
		if err := redis.ScanStruct(result, p); err != nil {
			return nil, err
		}
		peers = append(peers, p)
	}

	return peers, nil
}

// CreateTorrent writes mi to Redis with a TTL.
func (s *RedisStorage) CreateTorrent(mi *torlib.MetaInfo) error {
	c, err := s.pool.Dial()
	if err != nil {
		return err
	}
	defer c.Close()

	v, err := mi.Serialize()
	if err != nil {
		return err
	}
	_, err = c.Do("SETEX", torrentKey(mi.Name()), s.cfg.TorrentTTLSecs, v)
	return err
}

// GetTorrent returns serialized MetaInfo for the given file name.
func (s *RedisStorage) GetTorrent(name string) (string, error) {
	c, err := s.pool.Dial()
	if err != nil {
		return "", err
	}
	defer c.Close()

	return redis.String(c.Do("GET", torrentKey(name)))
}
