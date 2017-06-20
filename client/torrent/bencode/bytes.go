package bencode

// Bytes is an alias for byte array
type Bytes []byte

var (
	_ Unmarshaler = &Bytes{}
	_ Marshaler   = &Bytes{}
	_ Marshaler   = Bytes{}
)

// UnmarshalBencode unmarshall benoded stream
func (bts *Bytes) UnmarshalBencode(b []byte) error {
	*bts = append([]byte(nil), b...)
	return nil
}

// MarshalBencode marshalls into benoded stream
func (bts Bytes) MarshalBencode() ([]byte, error) {
	return bts, nil
}
