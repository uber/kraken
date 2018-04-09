package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/andres-erbsen/clock"
	"github.com/garyburd/redigo/redis"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/log"
	"code.uber.internal/infra/kraken/utils/randutil"
)

// RedisStorage errors.
var (
	ErrNoOrigins = errors.New("no origins found")
)

func peerSetKey(h core.InfoHash, window int64) string {
	return fmt.Sprintf("peerset:%s:%d", h.String(), window)
}

func originsKey(h core.InfoHash) string {
	return fmt.Sprintf("origins:%s", h.String())
}

func metaInfoKey(name string) string {
	return fmt.Sprintf("metainfo:%s", name)
}

func serializePeer(p *core.PeerInfo) string {
	var completeBit int
	if p.Complete {
		completeBit = 1
	}
	return fmt.Sprintf("%s:%s:%d:%d", p.PeerID.String(), p.IP, p.Port, completeBit)
}

type peerIdentity struct {
	peerID core.PeerID
	ip     string
	port   int
}

func deserializePeer(s string) (id peerIdentity, complete bool, err error) {
	parts := strings.Split(s, ":")
	if len(parts) != 4 {
		return id, false, fmt.Errorf("invalid peer encoding: expected 'pid:ip:port:complete'")
	}
	peerID, err := core.NewPeerID(parts[0])
	if err != nil {
		return id, false, fmt.Errorf("parse peer id: %s", err)
	}
	ip := parts[1]
	port, err := strconv.Atoi(parts[2])
	if err != nil {
		return id, false, fmt.Errorf("parse port: %s", err)
	}
	id = peerIdentity{peerID, ip, port}
	complete = parts[3] == "1"
	return id, complete, nil
}

// RedisStorage provides fast lookup for peers and torrent metainfo with expiration.
type RedisStorage struct {
	config RedisConfig
	pool   *redis.Pool
	clk    clock.Clock
}

// NewRedisStorage creates a RedisStorage instance.
func NewRedisStorage(config RedisConfig, clk clock.Clock) (*RedisStorage, error) {
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
		clk: clk,
	}

	// Ensure we can connect to Redis.
	c, err := s.pool.Dial()
	if err != nil {
		return nil, fmt.Errorf("dial redis: %s", err)
	}
	c.Close()

	return s, nil
}

func (s *RedisStorage) curPeerSetWindow() int64 {
	t := s.clk.Now().Unix()
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
func (s *RedisStorage) UpdatePeer(h core.InfoHash, p *core.PeerInfo) error {
	c := s.pool.Get()
	defer c.Close()

	w := s.curPeerSetWindow()
	expireAt := w + int64(s.config.PeerSetWindowSize.Seconds())*int64(s.config.MaxPeerSetWindows)

	// Add p to the current window.
	k := peerSetKey(h, w)

	c.Send("MULTI")
	c.Send("SADD", k, serializePeer(p))
	c.Send("EXPIREAT", k, expireAt)
	_, err := c.Do("EXEC")

	return err
}

// GetPeers returns at most n PeerInfos associated with h.
func (s *RedisStorage) GetPeers(h core.InfoHash, n int) ([]*core.PeerInfo, error) {
	c := s.pool.Get()
	defer c.Close()

	// Try to sample n peers from each window in randomized order until we have
	// collected n distinct peers. This achieves random sampling across multiple
	// windows.
	// TODO(codyg): One limitation of random window sampling is we're no longer
	// guaranteed to include the latest completion bits. A simple way to mitigate
	// this is to decrease the number of windows.
	windows := s.peerSetWindows()
	randutil.ShuffleInt64s(windows)

	// Eliminate duplicates from other windows and collapses complete bits.
	selected := make(map[peerIdentity]bool)

	var i int
	for len(selected) < n && i < len(windows) {
		result, err := redis.Strings(c.Do("SRANDMEMBER", peerSetKey(h, windows[i]), n-len(selected)))
		if err != nil {
			return nil, err
		}
		for _, s := range result {
			id, complete, err := deserializePeer(s)
			if err != nil {
				log.Errorf("Error deserializing peer %q: %s", s, err)
				continue
			}
			selected[id] = selected[id] || complete
		}
		i++
	}

	var peers []*core.PeerInfo
	for id, complete := range selected {
		p := core.NewPeerInfo(id.peerID, id.ip, id.port, false, complete)
		peers = append(peers, p)
	}
	return peers, nil
}

// GetOrigins returns all origin PeerInfos for h. Returns ErrNoOrigins if
// no origins exist in Redis.
func (s *RedisStorage) GetOrigins(h core.InfoHash) ([]*core.PeerInfo, error) {
	c := s.pool.Get()
	defer c.Close()

	result, err := redis.String(c.Do("GET", originsKey(h)))
	if err != nil {
		if err == redis.ErrNil {
			return nil, ErrNoOrigins
		}
		return nil, err
	}

	var origins []*core.PeerInfo
	for _, s := range strings.Split(result, ",") {
		id, complete, err := deserializePeer(s)
		if err != nil {
			log.Errorf("Error deserializing origin %q: %s", s, err)
			continue
		}
		o := core.NewPeerInfo(id.peerID, id.ip, id.port, true, complete)
		origins = append(origins, o)
	}
	return origins, nil
}

// UpdateOrigins overwrites all origin PeerInfos for h with the given origins.
func (s *RedisStorage) UpdateOrigins(h core.InfoHash, origins []*core.PeerInfo) error {
	c := s.pool.Get()
	defer c.Close()

	var serializedOrigins []string
	for _, o := range origins {
		serializedOrigins = append(serializedOrigins, serializePeer(o))
	}
	v := strings.Join(serializedOrigins, ",")

	_, err := c.Do("SETEX", originsKey(h), int(s.config.OriginsTTL.Seconds()), v)
	return err
}

// GetMetaInfo returns metainfo for name. Returns ErrNotFound if no metainfo exists for name.
func (s *RedisStorage) GetMetaInfo(name string) ([]byte, error) {
	c := s.pool.Get()
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
func (s *RedisStorage) SetMetaInfo(mi *core.MetaInfo) error {
	c := s.pool.Get()
	defer c.Close()

	b, err := mi.Serialize()
	if err != nil {
		return fmt.Errorf("serialize metainfo: %s", err)
	}

	r, err := c.Do(
		"SET", metaInfoKey(mi.Name()), b,
		"EX", strconv.Itoa(int(s.config.MetaInfoTTL.Seconds())),
		"NX")
	if err != nil {
		return err
	}
	if r == nil {
		return ErrExists
	}
	return nil
}
