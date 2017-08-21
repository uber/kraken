package torlib

import (
	"io"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken-torrent/bencode"
)

// AnnounceList is a list of tracker announcers
// index is the tier of the list, smaller index means this list of announcers is more preferred.
type AnnounceList [][]string

// MetaInfo contains torrent metadata
type MetaInfo struct {
	Info         Info         `bencode:"info"`
	Announce     string       `bencode:"announce"`
	AnnounceList AnnounceList `bencode:"announce-list,omitempty"`
	CreationDate int64        `bencode:"creation date,omitempty"`
	Comment      string       `bencode:"comment,omitempty"`
	CreatedBy    string       `bencode:"created by,omitempty"`
	Encoding     string       `bencode:"encoding,omitempty"`

	// infohash is computed by MetaInfo.Info
	// we store a copy of the hash here to avoid unnecessary rehash
	// infohash must be set before this struct is used
	InfoHash InfoHash
}

// NewMetaInfoFromInfo create MetaInfo from Info
func NewMetaInfoFromInfo(info Info, announce string) (*MetaInfo, error) {
	mi := &MetaInfo{
		Info: info,
	}
	err := mi.initialize()
	if err != nil {
		return nil, err
	}
	return mi, err
}

// NewMetaInfoFromFile creates MetaInfo from a file
func NewMetaInfoFromFile(
	name string,
	fp string,
	piecelength int64,
	announceList AnnounceList,
	comment string,
	createdBy string,
	encoding string) (*MetaInfo, error) {

	info, err := NewInfoFromFile(name, fp, piecelength)
	if err != nil {
		return nil, err
	}

	mi := MetaInfo{
		Info:         info,
		AnnounceList: announceList,
		CreationDate: time.Now().Unix(),
		CreatedBy:    createdBy,
		Encoding:     encoding,
	}

	err = mi.initialize()
	if err != nil {
		return nil, err
	}

	return &mi, nil
}

// NewMetaInfoFromBytes creates MetaInfo from bytes
func NewMetaInfoFromBytes(data []byte) (*MetaInfo, error) {
	var mi MetaInfo
	err := bencode.Unmarshal(data, &mi)
	if err != nil {
		return nil, err
	}

	err = mi.initialize()
	if err != nil {
		return nil, err
	}

	return &mi, nil
}

// Name returns torrent name
func (mi *MetaInfo) Name() string {
	return mi.Info.Name
}

// WriteBencode encodes to bencoded form.
func (mi *MetaInfo) WriteBencode(w io.Writer) error {
	return bencode.NewEncoder(w).Encode(mi)
}

// Serialize returns metainfo as a bencoded string
func (mi *MetaInfo) Serialize() (string, error) {
	bytes, err := bencode.Marshal(mi)
	if err != nil {
		log.Error(err)
		return "", err
	}
	return string(bytes[:]), nil
}

// initialize computes info hash and set default fields
func (mi *MetaInfo) initialize() error {
	err := mi.setInfoHash()
	if err != nil {
		return err
	}

	return nil
}

// setInfoHash computes hash of mi.Info and sets mi.infohash
func (mi *MetaInfo) setInfoHash() error {
	hash, err := mi.Info.ComputeInfoHash()
	if err != nil {
		return err
	}
	mi.InfoHash = hash
	return nil
}
