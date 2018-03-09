package core

import (
	"bytes"
	"io/ioutil"
	"os"

	"code.uber.internal/infra/kraken/utils/randutil"
)

const fixtureTempDir = "/tmp/kraken_fixtures"

func init() {
	os.Mkdir(fixtureTempDir, 0755)
}

// PeerIDFixture returns a randomly generated PeerID.
func PeerIDFixture() PeerID {
	p, err := RandomPeerID()
	if err != nil {
		panic(err)
	}
	return p
}

// InfoHashFixture returns a randomly generated InfoHash.
func InfoHashFixture() InfoHash {
	return MetaInfoFixture().InfoHash
}

// PeerInfoFixture returns a randomly generated PeerInfo.
func PeerInfoFixture() *PeerInfo {
	return &PeerInfo{
		InfoHash: InfoHashFixture().String(),
		PeerID:   PeerIDFixture().String(),
		IP:       randutil.IP(),
		Port:     int64(randutil.Port()),
		Complete: false,
	}
}

// PeerInfoForMetaInfoFixture returns a randomly generated PeerInfo associated
// with the given MetaInfo.
func PeerInfoForMetaInfoFixture(mi *MetaInfo) *PeerInfo {
	p := PeerInfoFixture()
	p.InfoHash = mi.InfoHash.String()
	return p
}

// OriginInfoForMetaInfoFixture returns a randomly generated origin PeerInfo associated
// with the given MetaInfo.
func OriginInfoForMetaInfoFixture(mi *MetaInfo) *PeerInfo {
	o := PeerInfoForMetaInfoFixture(mi)
	o.Origin = true
	o.Complete = true
	return o
}

// ToPeerInfoFixture joins pctx and mi into a PeerInfo.
func ToPeerInfoFixture(pctx PeerContext, mi *MetaInfo) *PeerInfo {
	p := PeerInfoFixture()
	p.InfoHash = mi.InfoHash.String()
	p.PeerID = pctx.PeerID.String()
	p.IP = pctx.IP
	p.Port = int64(pctx.Port)
	p.DC = pctx.Zone
	p.Origin = pctx.Origin
	if p.Origin {
		p.Complete = true
	}
	return p
}

// TestTorrentFile joins a MetaInfo with the file contents used to generate
// said MetaInfo. Note, does not include any physical files so no cleanup is
// necessary.
type TestTorrentFile struct {
	MetaInfo *MetaInfo
	Content  []byte
}

// CustomTestTorrentFileFixture returns a randomly generated TestTorrentFile
// of the given size and piece length.
// TODO(codyg): Move this to storage package.
func CustomTestTorrentFileFixture(size uint64, pieceLength uint64) *TestTorrentFile {
	f, err := ioutil.TempFile(fixtureTempDir, "torrent_")
	if err != nil {
		panic(err)
	}
	defer os.Remove(f.Name())

	content := randutil.Text(size)

	digest, err := NewDigester().FromReader(bytes.NewBuffer(content))
	if err != nil {
		panic(err)
	}

	if err := ioutil.WriteFile(f.Name(), content, 0755); err != nil {
		panic(err)
	}

	info, err := NewInfoFromFile(digest.Hex(), f.Name(), int64(pieceLength))
	if err != nil {
		panic(err)
	}
	mi, err := NewMetaInfoFromInfo(info, "")
	if err != nil {
		panic(err)
	}

	return &TestTorrentFile{mi, content}
}

// TestTorrentFileFixture returns a randomly generated TestTorrentFile.
func TestTorrentFileFixture() *TestTorrentFile {
	return CustomTestTorrentFileFixture(128, 32)
}

// MetaInfoFixture returns a randomly generated MetaInfo.
func MetaInfoFixture() *MetaInfo {
	return TestTorrentFileFixture().MetaInfo
}

// CustomMetaInfoFixture returns a randomly generated MetaInfo of the given size
// and piece length.
func CustomMetaInfoFixture(size, pieceLength uint64) *MetaInfo {
	return CustomTestTorrentFileFixture(size, pieceLength).MetaInfo
}

// DigestFixture returns a random Digest.
func DigestFixture() Digest {
	b := randutil.Text(32)
	d, err := NewDigester().FromBytes(b)
	if err != nil {
		panic(err)
	}
	return d
}

// DigestWithBlobFixture returns a random digest and its corresponding blob.
func DigestWithBlobFixture() (d Digest, blob []byte) {
	blob = randutil.Text(256)
	d, err := NewDigester().FromBytes(blob)
	if err != nil {
		panic(err)
	}
	return d, blob
}

// PeerContextFixture returns a randomly generated PeerContext.
func PeerContextFixture() PeerContext {
	return PeerContext{
		IP:     randutil.IP(),
		Port:   randutil.Port(),
		PeerID: PeerIDFixture(),
		Zone:   "sjc1",
	}
}

// OriginContextFixture returns a randomly generated origin PeerContext.
func OriginContextFixture() PeerContext {
	octx := PeerContextFixture()
	octx.Origin = true
	return octx
}
