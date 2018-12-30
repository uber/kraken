package core

import (
	"bytes"
	"fmt"

	"github.com/uber/kraken/utils/randutil"
)

// BlobFixture joins all information associated with a blob for testing convenience.
type BlobFixture struct {
	Content  []byte
	Digest   Digest
	MetaInfo *MetaInfo
}

// Length returns the length of the blob.
func (f *BlobFixture) Length() int64 {
	return int64(len(f.Content))
}

// Info returns a BlobInfo for f.
func (f *BlobFixture) Info() *BlobInfo {
	return NewBlobInfo(f.Length())
}

// CustomBlobFixture creates a BlobFixture with custom fields.
func CustomBlobFixture(content []byte, digest Digest, mi *MetaInfo) *BlobFixture {
	return &BlobFixture{content, digest, mi}
}

// SizedBlobFixture creates a randomly generated BlobFixture of given size with given piece lengths.
func SizedBlobFixture(size uint64, pieceLength uint64) *BlobFixture {
	b := randutil.Text(size)
	d, err := NewDigester().FromBytes(b)
	if err != nil {
		panic(err)
	}
	mi, err := NewMetaInfo(d, bytes.NewReader(b), int64(pieceLength))
	if err != nil {
		panic(err)
	}
	return &BlobFixture{
		Content:  b,
		Digest:   d,
		MetaInfo: mi,
	}
}

// NewBlobFixture creates a randomly generated BlobFixture.
func NewBlobFixture() *BlobFixture {
	return SizedBlobFixture(256, 8)
}

// PeerIDFixture returns a randomly generated PeerID.
func PeerIDFixture() PeerID {
	p, err := RandomPeerID()
	if err != nil {
		panic(err)
	}
	return p
}

// PeerInfoFixture returns a randomly generated PeerInfo.
func PeerInfoFixture() *PeerInfo {
	return NewPeerInfo(PeerIDFixture(), randutil.IP(), randutil.Port(), false, false)
}

// OriginPeerInfoFixture returns a randomly generated PeerInfo for an origin.
func OriginPeerInfoFixture() *PeerInfo {
	return NewPeerInfo(PeerIDFixture(), randutil.IP(), randutil.Port(), true, true)
}

// MetaInfoFixture returns a randomly generated MetaInfo.
func MetaInfoFixture() *MetaInfo {
	return NewBlobFixture().MetaInfo
}

// InfoHashFixture returns a randomly generated InfoHash.
func InfoHashFixture() InfoHash {
	return MetaInfoFixture().InfoHash()
}

// DigestFixture returns a random Digest.
func DigestFixture() Digest {
	return NewBlobFixture().Digest
}

// DigestListFixture returns a list of random Digests.
func DigestListFixture(n int) []Digest {
	var l DigestList
	for i := 0; i < n; i++ {
		l = append(l, DigestFixture())
	}
	return l
}

// PeerContextFixture returns a randomly generated PeerContext.
func PeerContextFixture() PeerContext {
	pctx, err := NewPeerContext(
		RandomPeerIDFactory,
		"zone1",
		"test01-zone1",
		randutil.IP(),
		randutil.Port(),
		false)
	if err != nil {
		panic(err)
	}
	return pctx
}

// OriginContextFixture returns a randomly generated origin PeerContext.
func OriginContextFixture() PeerContext {
	octx := PeerContextFixture()
	octx.Origin = true
	return octx
}

// TagFixture creates a random tag for service labrat.
func TagFixture() string {
	return fmt.Sprintf("uber-usi/labrat:%s", randutil.Text(8))
}
