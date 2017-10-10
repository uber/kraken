package image

import "code.uber.internal/infra/kraken/utils/randutil"

// DigestFixture returns a random Digest.
func DigestFixture() Digest {
	b := randutil.Text(32)
	d, err := NewDigester().FromBytes(b)
	if err != nil {
		panic(err)
	}
	return d
}
