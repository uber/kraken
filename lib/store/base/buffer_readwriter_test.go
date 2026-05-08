// Copyright (c) 2016-2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package base

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferReadWriter_Write(t *testing.T) {
	tests := []struct {
		name           string
		writes         [][]byte
		expectedResult []byte
		expectedSize   int64
	}{
		{
			name:           "sequential writes",
			writes:         [][]byte{[]byte("hello"), []byte(" world")},
			expectedResult: []byte("hello world"),
			expectedSize:   11,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := NewBufferReadWriter(0)

			for _, data := range tt.writes {
				n, err := buf.Write(data)
				require.NoError(t, err)
				assert.Equal(t, len(data), n)
			}

			assert.Equal(t, tt.expectedSize, buf.Size())
		})
	}
}

func TestBufferReadWriter_WriteAt(t *testing.T) {
	tests := []struct {
		name       string
		operations []struct {
			data   []byte
			offset int64
		}
		expectedResult []byte
		expectedSize   int64
	}{
		{
			name: "out-of-order writes",
			operations: []struct {
				data   []byte
				offset int64
			}{
				{[]byte("56789"), 5},
				{[]byte("01234"), 0},
			},
			expectedResult: []byte("0123456789"),
			expectedSize:   10,
		},
		{
			name: "writes with gaps",
			operations: []struct {
				data   []byte
				offset int64
			}{
				{[]byte("hello"), 0},
				{[]byte("world"), 6},
			},
			expectedSize: 11,
		},
		{
			name: "overwrite",
			operations: []struct {
				data   []byte
				offset int64
			}{
				{[]byte("hello world"), 0},
				{[]byte("WORLD"), 6},
			},
			expectedResult: []byte("hello WORLD"),
			expectedSize:   11,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := NewBufferReadWriter(0)

			for _, op := range tt.operations {
				n, err := buf.WriteAt(op.data, op.offset)
				require.NoError(t, err)
				assert.Equal(t, len(op.data), n)
			}

			assert.Equal(t, tt.expectedSize, buf.Size())
		})
	}
}

func TestBufferReadWriter_Read(t *testing.T) {
	tests := []struct {
		name          string
		setupData     []byte
		readSizes     []int
		expectedReads [][]byte
	}{
		{
			name:          "sequential reads",
			setupData:     []byte("hello world"),
			readSizes:     []int{5, 6},
			expectedReads: [][]byte{[]byte("hello"), []byte(" world")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := NewBufferReadWriter(0)
			_, err := buf.Write(tt.setupData)
			require.NoError(t, err)
			_, err = buf.Seek(0, io.SeekStart)
			require.NoError(t, err)

			for i, size := range tt.readSizes {
				data := make([]byte, size)
				n, err := buf.Read(data)
				require.NoError(t, err)
				assert.Equal(t, size, n)
				assert.Equal(t, tt.expectedReads[i], data)
			}
		})
	}
}

func TestBufferReadWriter_ReadAt(t *testing.T) {
	tests := []struct {
		name         string
		setupData    []byte
		offset       int64
		readSize     int
		expectedData []byte
	}{
		{
			name:         "random access",
			setupData:    []byte("hello world"),
			offset:       6,
			readSize:     5,
			expectedData: []byte("world"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := NewBufferReadWriter(0)
			_, err := buf.Write(tt.setupData)
			require.NoError(t, err)

			data := make([]byte, tt.readSize)
			n, err := buf.ReadAt(data, tt.offset)
			require.NoError(t, err)
			assert.Equal(t, tt.readSize, n)
			assert.Equal(t, tt.expectedData, data)
			_, err = buf.Seek(0, io.SeekStart)
			require.NoError(t, err)

			resetData := make([]byte, 5)
			n, err = buf.Read(resetData)
			require.NoError(t, err)
			assert.Equal(t, 5, n)
			assert.Equal(t, []byte("hello"), resetData)
		})
	}
}

func TestBufferReadWriter_Seek(t *testing.T) {
	tests := []struct {
		name           string
		setupData      []byte
		seekOffset     int64
		seekWhence     int
		expectedOffset int64
		expectedRead   []byte
		expectError    bool
	}{
		{
			name:           "seek from start",
			setupData:      []byte("hello world"),
			seekOffset:     6,
			seekWhence:     io.SeekStart,
			expectedOffset: 6,
			expectedRead:   []byte("world"),
		},
		{
			name:           "seek from end",
			setupData:      []byte("hello world"),
			seekOffset:     -5,
			seekWhence:     io.SeekEnd,
			expectedOffset: 6,
			expectedRead:   []byte("world"),
		},
		{
			name:           "negative position",
			setupData:      []byte("hello world"),
			seekOffset:     -100,
			seekWhence:     io.SeekStart,
			expectedOffset: 0,
			expectError:    true,
		},
		{
			name:           "invalid whence",
			setupData:      []byte("hello world"),
			seekOffset:     0,
			seekWhence:     999,
			expectedOffset: 0,
			expectError:    true,
		},
		{
			name:           "seek beyond end",
			setupData:      []byte("hello world"),
			seekOffset:     100,
			seekWhence:     io.SeekStart,
			expectedOffset: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := NewBufferReadWriter(0)
			_, err := buf.Write(tt.setupData)
			require.NoError(t, err)

			offset, err := buf.Seek(tt.seekOffset, tt.seekWhence)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedOffset, offset)

			if tt.expectedRead != nil {
				data := make([]byte, len(tt.expectedRead))
				n, err := buf.Read(data)
				require.NoError(t, err)
				assert.Equal(t, len(tt.expectedRead), n)
				assert.Equal(t, tt.expectedRead, data)
			}
		})
	}
}

func TestBufferReadWriter_SeekCurrent(t *testing.T) {
	buf := NewBufferReadWriter(0)
	_, err := buf.Write([]byte("hello world"))
	require.NoError(t, err)

	_, err = buf.Seek(0, io.SeekStart)
	require.NoError(t, err)
	_, err = buf.Read(make([]byte, 5))
	require.NoError(t, err)

	offset, err := buf.Seek(1, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(6), offset)

	data := make([]byte, 5)
	n, err := buf.Read(data)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("world"), data)
}

func TestBufferReadWriter_TestReader(t *testing.T) {
	content := []byte(`Kraken is a peer-to-peer (P2P) Docker registry that focuses on scalability and availability.
		It is designed for Docker image management, layer caching, and blob storage distribution at scale.
		Kraken uses BitTorrent protocol for efficient content distribution across a cluster of nodes.
		This BufferReadWriter implementation provides in-memory buffering for read and write operations,
		supporting parallel writes via WriteAt for backends like S3 and GCS that require concurrent chunk uploads.
		The buffer implements io.Reader, io.ReaderAt, io.Writer, io.WriterAt, and io.Seeker interfaces
		to provide comprehensive I/O capabilities for content-addressable storage operations.`,
	)

	buf := NewBufferReadWriter(0)
	_, err := buf.Write(content)
	require.NoError(t, err)

	_, err = buf.Seek(0, io.SeekStart)
	require.NoError(t, err)

	err = iotest.TestReader(buf, content)
	require.NoError(t, err)
}

// TestBufferReadWriter_ConcurrentWriteAt validates that concurrent writes to
// non-overlapping byte ranges on a pre-sized buffer produce correct results.
// Run with -race to confirm no data races.
func TestBufferReadWriter_ConcurrentWriteAt(t *testing.T) {
	const numShards, shardSize = 10, 1024
	data := make([]byte, numShards*shardSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	buf := NewBufferReadWriter(numShards * shardSize)
	var wg sync.WaitGroup
	for i := 0; i < numShards; i++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			off := int64(shard * shardSize)
			_, err := buf.WriteAt(data[off:off+shardSize], off)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
	assert.Equal(t, data, buf.Bytes())
}

// totalMutexContentions returns the total number of mutex contention events
// recorded in the runtime mutex profile since profiling was enabled.
func totalMutexContentions() int64 {
	records := make([]runtime.BlockProfileRecord, 10000)
	n, _ := runtime.MutexProfile(records)
	var total int64
	for i := 0; i < n; i++ {
		total += records[i].Count
	}
	return total
}

// benchmarkWriteAt is the shared helper for all WriteAt benchmarks.
// numShards goroutines each write a non-overlapping 4 MiB shard concurrently,
// matching the transfermanager production workload.
// initSize controls the buffer's initial allocation:
//   - initSize == totalSize: pre-sized fast path (production case)
//   - initSize == 0:         dynamic growth (sequential / wrong-size case)
//   - initSize == totalSize/2: partial pre-allocation, triggers growth mid-download
func benchmarkWriteAt(b *testing.B, numShards int, initSize uint64) {
	b.Helper()
	const shardSize = 4 * 1024 * 1024
	totalSize := uint64(numShards) * shardSize

	shards := make([][]byte, numShards)
	for i := range shards {
		shards[i] = make([]byte, shardSize)
		for j := range shards[i] {
			shards[i][j] = byte(i)
		}
	}

	runtime.SetMutexProfileFraction(1)
	b.ResetTimer()
	b.SetBytes(int64(totalSize))
	b.ReportAllocs()

	startContentions := totalMutexContentions()

	for i := 0; i < b.N; i++ {
		buf := NewBufferReadWriter(initSize)
		var wg sync.WaitGroup
		for shard := 0; shard < numShards; shard++ {
			wg.Add(1)
			go func(s int) {
				defer wg.Done()
				_, err := buf.WriteAt(shards[s], int64(s)*shardSize)
				require.NoError(b, err)
			}(shard)
		}
		wg.Wait()
	}

	b.StopTimer()
	if b.N > 0 {
		b.ReportMetric(float64(totalMutexContentions()-startContentions)/float64(b.N), "mutex-contentions/op")
	}
}

// BenchmarkBufferReadWriter_WriteAt exercises three buffer initialisation
// strategies × three shard counts, giving a full picture of throughput and
// mutex contention under varying concurrency and pre-allocation.
//
// Run before and after the implementation change, then compare with:
//
//	benchstat bench-results/before.txt bench-results/after.txt
func BenchmarkBufferReadWriter_WriteAt(b *testing.B) {
	const shardSize = 4 * 1024 * 1024
	cases := []struct {
		label    string
		initFunc func(total uint64) uint64
	}{
		{"presized", func(total uint64) uint64 { return total }},          // production fast path
		{"half_presized", func(total uint64) uint64 { return total / 2 }}, // partial pre-alloc, growth needed
		{"dynamic", func(total uint64) uint64 { return 0 }},               // no pre-alloc, always grows
	}
	shardCounts := []int{1, 4, 10}

	for _, tc := range cases {
		for _, numShards := range shardCounts {
			totalSize := uint64(numShards) * shardSize
			initSize := tc.initFunc(totalSize)
			b.Run(fmt.Sprintf("%s_%d_shards", tc.label, numShards), func(b *testing.B) {
				benchmarkWriteAt(b, numShards, initSize)
			})
		}
	}
}
