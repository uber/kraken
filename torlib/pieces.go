package torlib

import (
	"encoding/hex"
	"encoding/json"
)

// Pieces is a slice of piece hash
type Pieces []byte

// UnmarshalJSON decode bytes to raw hash bytes
func (p *Pieces) UnmarshalJSON(b []byte) error {
	var str string
	json.Unmarshal(b, &str)
	decoded, err := hex.DecodeString(str)
	if err != nil {
		return err
	}
	*p = decoded

	return nil
}

// MarshalJSON encode raw hash to bytes
func (p Pieces) MarshalJSON() ([]byte, error) {
	hexPieces := hex.EncodeToString(p)
	return json.Marshal(hexPieces)
}
