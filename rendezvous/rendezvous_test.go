package weightedhash

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
	"hash"
	"math"
	"os"
	"sort"
	"testing"
)

func assertKeyDistribution(t *testing.T, rh *RendezvousHash, nodekeys NodeKeysTable, numKeys int, totalWeights float64, delta float64) {
	for name, v := range nodekeys {
		node, _ := rh.GetNode(name)
		assert.NotEqual(t, node, nil)

		// make sure the ratio of keys falling in a particular bucket
		// conforms to general weight distribution within 1% accuracy
		assert.InDelta(t, float64(len(v))/float64(numKeys), float64(node.Weight)/totalWeights, delta)
	}

}

var (
	hashes     []HashFactory
	scoreFuncs []UIntToFloat
	numKeys    int
)

func Murmur3Hash() hash.Hash {
	h32 := murmur3.New64()
	return h32
}

func TestMain(m *testing.M) {
	hashes = []HashFactory{Murmur3Hash, sha256.New, md5.New}
	scoreFuncs = []UIntToFloat{BigIntToFloat64, UInt64ToFloat64}
	numKeys = 10000

	os.Exit(m.Run())
}

func TestKeyDistribution(t *testing.T) {
	for _, hash := range hashes {
		for _, scoreFunc := range scoreFuncs {

			t.Run("Ensure a weighted key distribution is in place", func(t *testing.T) {
				rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)
				assertKeyDistribution(t, rh, nodekeys, numKeys, 1500.0, 0.02)
			})
		}
	}
}

func TestAddRemoveNodes(t *testing.T) {
	for _, hash := range hashes {
		for _, scoreFunc := range scoreFuncs {

			t.Run("Make sure removing nodes to WRH does not cause rehashing of the entire hashing ring", func(t *testing.T) {
				rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)

				rh.RemoveNode("1")
				assert.Equal(t, len(rh.Nodes), 3)

				for name, v := range nodekeys {
					if name == "1" { //obviously "1" node is going to be relocated to other nodes
						continue
					}
					// the rmaining nodes should not change their allocation buckets
					for key := range v {
						nodes, _ := rh.GetOrderedNodes(key, 1)
						assert.Equal(t, nodes[0].Label, name)
					}
				}
			})
			t.Run("Make sure adding nodes to WRH will only cause other nodes to lose some population of keys", func(t *testing.T) {
				rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)

				rh.AddNode("4", 200)
				nodekeys["4"] = make(map[string]struct{})

				assert.Equal(t, len(rh.Nodes), 5)

				for name, v := range nodekeys {
					if name == "4" { //new node "4" will get some keys from other nodes
						continue
					}
					// the remaining nodes should not change their allocation buckets
					for key := range v {
						nodes, _ := rh.GetOrderedNodes(key, 1)
						if nodes[0].Label != name {
							assert.Equal(t, nodes[0].Label, "4")
							nodekeys[nodes[0].Label][key] = struct{}{}
							delete(nodekeys[name], key)
						}
					}
				}
				assertKeyDistribution(t, rh, nodekeys, numKeys, 1700.0, 0.02)
			})
		}
	}
}

type ByScore []float64

func (a ByScore) Len() int           { return len(a) }
func (a ByScore) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByScore) Less(i, j int) bool { return a[i] < a[j] }

func TestGetOrderedNodes(t *testing.T) {
	for _, hash := range hashes {
		for _, scoreFunc := range scoreFuncs {

			t.Run("Ensure the length of returned nodes", func(t *testing.T) {
				rh, _ := RendezvousHashFixture(0, hash, scoreFunc, 100, 200, 400, 800)
				keys := HashKeyFixture(1, hash)

				var scores []float64
				for _, node := range rh.Nodes {
					score := node.Score(keys[0])
					scores = append(scores, score)
				}
				sort.Sort(ByScore(scores))
				nodes, _ := rh.GetOrderedNodes(keys[0], 4)
				assert.Equal(t, len(nodes), 4)
			})
			t.Run("Ensure order of returned nodes", func(t *testing.T) {
				rh, _ := RendezvousHashFixture(0, hash, scoreFunc, 100, 200, 400, 800)
				keys := HashKeyFixture(1, hash)

				var scores []float64
				for _, node := range rh.Nodes {
					score := node.Score(keys[0])
					scores = append(scores, score)
				}
				sort.Sort(ByScore(scores))
				nodes, _ := rh.GetOrderedNodes(keys[0], 4)
				for index, node := range nodes {
					score := node.Score(keys[0])
					assert.Equal(t, score, scores[4-index-1])
				}
			})
		}
	}
}

func TestChangeCapacity(t *testing.T) {
	for _, hash := range hashes {
		for _, scoreFunc := range scoreFuncs {
			t.Run("Ensure adding capacity in one of WRH nodes will relocate some keys to it", func(t *testing.T) {
				rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)

				_, index := rh.GetNode("3")
				rh.Nodes[index].Weight = 1000

				for name, v := range nodekeys {
					// some keys in nodes should change their allocation buckets to accomdate for
					// new capacity on node "3"
					for key := range v {
						nodes, _ := rh.GetOrderedNodes(key, 1)
						if nodes[0].Label != name {
							assert.Equal(t, nodes[0].Label, "3")
							nodekeys[nodes[0].Label][key] = struct{}{}
							delete(nodekeys[name], key)
						} else {
							assert.Equal(t, nodes[0].Label, name)
						}
					}
				}

				// make sure we still keep the target distribution after resharding
				assertKeyDistribution(t, rh, nodekeys, numKeys, 1700.0, 0.02)
			})
			t.Run("Ensure removing capacity in one of WRH nodes will move some keys from it to other nodes", func(t *testing.T) {
				rh, nodekeys := RendezvousHashFixture(numKeys, hash, scoreFunc, 100, 200, 400, 800)

				_, index := rh.GetNode("3")
				rh.Nodes[index].Weight = 200

				for name, v := range nodekeys {
					// the remaining nodes should not change their allocation buckets
					for key := range v {
						nodes, _ := rh.GetOrderedNodes(key, 1)
						if nodes[0].Label != name {
							assert.Equal(t, name, "3")
							assert.NotEqual(t, nodes[0].Label, "3")
							nodekeys[nodes[0].Label][key] = struct{}{}
							delete(nodekeys[name], key)
						} else {
							assert.Equal(t, nodes[0].Label, name)
						}
					}
				}

				// make sure we still keep the target distribution after resharding
				assertKeyDistribution(t, rh, nodekeys, numKeys, 900.0, 0.02)
			})
		}
	}
}

func TestScoreFunctionFloatPrecision(t *testing.T) {
	byteLength := []int{8, 16, 32} // 64, 128, 256 bits

	for index, bl := range byteLength {
		for indexScore, scoreFunc := range scoreFuncs {
			//UInt64ToFloat64 can't work on values > 64 bits
			if index > 0 && indexScore > 0 {
				continue
			}
			maxHashValue := make([]byte, bl)
			val := make([]byte, bl)

			for i := 0; i < bl; i++ {
				maxHashValue[i] = 0xFF
				val[i] = 0
			}

			val[len(val)-1] = 1
			floatVal := scoreFunc(val, maxHashValue, nil)
			assert.NotEqual(t, floatVal, 0.0)
			assert.Equal(t, math.IsNaN(math.Log(floatVal)), false)
			assert.Equal(t, math.IsInf(math.Log(floatVal), 1), false)
			assert.Equal(t, math.IsInf(math.Log(floatVal), -1), false)
		}
	}
}

func TestScoreFunctionUint64ToFloat64BadValues(t *testing.T) {

	maxHashValue := make([]byte, 8)
	for i := 0; i < 8; i++ {
		maxHashValue[i] = 0xFF
	}

	u64val := (1 << 53)
	for i := 0; i <= 11; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(u64val))

		floatVal := UInt64ToFloat64(b, maxHashValue, nil)

		assert.Equal(t, floatVal, 0.0)

		floatVal = UInt64ToFloat64(b, maxHashValue, murmur3.New64())

		assert.NotEqual(t, floatVal, 0.0)
		assert.Equal(t, math.IsNaN(math.Log(floatVal)), false)
		assert.Equal(t, math.IsInf(math.Log(floatVal), 1), false)
		assert.Equal(t, math.IsInf(math.Log(floatVal), -1), false)
		u64val = u64val << 1

	}
	u64val = (1 << 53)
	for i := 0; i <= 11; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(u64val))

		floatVal := UInt64ToFloat64(b, maxHashValue, murmur3.New64())

		assert.NotEqual(t, floatVal, 0.0)
		assert.Equal(t, math.IsNaN(math.Log(floatVal)), false)
		assert.Equal(t, math.IsInf(math.Log(floatVal), 1), false)
		assert.Equal(t, math.IsInf(math.Log(floatVal), -1), false)
		u64val = u64val << 1
	}

}

func benchmarkHashScore(hashFactory HashFactory, scoreFunc UIntToFloat, b *testing.B) {
	keys := HashKeyFixture(1, hashFactory)
	rh, _ := RendezvousHashFixture(0, hashFactory, scoreFunc, 100, 200, 400, 800)

	for i := 0; i < b.N; i++ {
		rh.GetOrderedNodes(keys[0], 4)
	}
}

func BenchmarkMurmur3UInt64ToFloat64(b *testing.B) {
	benchmarkHashScore(Murmur3Hash, UInt64ToFloat64, b)
}

func BenchmarkSha256UInt64ToFloat64(b *testing.B) {
	benchmarkHashScore(sha256.New, UInt64ToFloat64, b)
}

func BenchmarkMurmur3BigIntToFloat64(b *testing.B) {
	benchmarkHashScore(Murmur3Hash, BigIntToFloat64, b)
}

func BenchmarkSha256BigIntToFloat64(b *testing.B) {
	benchmarkHashScore(sha256.New, BigIntToFloat64, b)
}
