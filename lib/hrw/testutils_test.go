package hrw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// assertKeyDistribution makes sure the ratio of keys falls in a particular
// bucket conforms to general weight distribution within accuracy.
func assertKeyDistribution(t *testing.T, rh *RendezvousHash, nodekeys NodeKeysTable, numKeys int, totalWeights float64, delta float64) {
	for name, v := range nodekeys {
		node, _ := rh.GetNode(name)
		assert.NotEqual(t, node, nil)

		assert.InDelta(t, float64(len(v))/float64(numKeys), float64(node.Weight)/totalWeights, delta)
	}
}

type ByScore []float64

func (a ByScore) Len() int           { return len(a) }
func (a ByScore) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByScore) Less(i, j int) bool { return a[i] < a[j] }
