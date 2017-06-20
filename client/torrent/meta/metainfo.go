package meta

import (
	"io"
	"os"
	"time"

	"code.uber.internal/infra/kraken-torrent/bencode"
)

// TorrentInfo is torrent metadata
type TorrentInfo struct {
	InfoBytes    bencode.Bytes `bencode:"info"`
	Announce     string        `bencode:"announce,omitempty"`
	AnnounceList AnnounceList  `bencode:"announce-list,omitempty"`
	Nodes        []Node        `bencode:"nodes,omitempty"`
	CreationDate int64         `bencode:"creation date,omitempty"`
	Comment      string        `bencode:"comment,omitempty"`
	CreatedBy    string        `bencode:"created by,omitempty"`
	Encoding     string        `bencode:"encoding,omitempty"`
	URLList      []string      `bencode:"url-list,omitempty"`
}

// AnnounceList a list of tracker announcers
type AnnounceList [][]string

// OverridesAnnounce whether the AnnounceList should be preferred over a single URL announce.
func (al AnnounceList) OverridesAnnounce(announce string) bool {
	for _, tier := range al {
		for _, url := range tier {
			if url != "" || announce == "" {
				return true
			}
		}
	}
	return false
}

// DistinctValues returns a list of unique trackers
func (al AnnounceList) DistinctValues() (ret map[string]struct{}) {
	for _, tier := range al {
		for _, v := range tier {
			if ret == nil {
				ret = make(map[string]struct{})
			}
			ret[v] = struct{}{}
		}
	}
	return
}

// Load loads a MetaInfo from an io.Reader. Returns a non-nil error in case of
// failure.
func Load(r io.Reader) (*TorrentInfo, error) {
	var mi TorrentInfo
	d := bencode.NewDecoder(r)
	err := d.Decode(&mi)
	if err != nil {
		return nil, err
	}
	return &mi, nil
}

// LoadFromFile is convenience function for loading a TorrentInfo from a file.
func LoadFromFile(filename string) (*TorrentInfo, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Load(f)
}

// UnmarshalInfo unmarshalls info from bencoded byte array
func (mi TorrentInfo) UnmarshalInfo() (info Info, err error) {
	err = bencode.Unmarshal(mi.InfoBytes, &info)
	return
}

// HashInfoBytes hashes the info bytes
func (mi TorrentInfo) HashInfoBytes() (infoHash Hash) {
	return HashBytes(mi.InfoBytes)
}

// Write encodes to bencoded form.
func (mi TorrentInfo) Write(w io.Writer) error {
	return bencode.NewEncoder(w).Encode(mi)
}

// SetDefaults sets good default values in preparation for creating a new TorrentInfo file.
func (mi *TorrentInfo) SetDefaults() {
	mi.Comment = "yoloham"
	mi.CreatedBy = "code.uber.internal/infra/kraken-torrent"
	mi.CreationDate = time.Now().Unix()
	// mi.Info.PieceLength = 256 * 1024
}

// UpvertedAnnounceList returns the announce list converted from the old single announce field if
// necessary.
func (mi *TorrentInfo) UpvertedAnnounceList() AnnounceList {
	if mi.AnnounceList.OverridesAnnounce(mi.Announce) {
		return mi.AnnounceList
	}
	if mi.Announce != "" {
		return [][]string{{mi.Announce}}
	}
	return nil
}
