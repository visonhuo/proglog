package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/visonhuo/proglog/api/v1"
)

var want = &api.Record{Value: []byte("hello world")}

func TestSegment(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, segment *segment, dir string){
		"AppendRead": testSegmentAppendRead,
		"Remote":     testSegmentRemove,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "segment_test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			var c Config
			c.Segment.MaxStoreBytes = 1024
			c.Segment.MaxIndexBytes = entryWidth * 3
			s, err := newSegment(dir, 16, c)
			require.NoError(t, err)
			require.Equal(t, uint64(16), s.baseOffset)
			require.Equal(t, uint64(16), s.nextOffset)
			fn(t, s, dir)
		})
	}
}

func testSegmentAppendRead(t *testing.T, segment *segment, _ string) {
	for i := uint64(0); i < 3; i++ {
		offset, err := segment.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, offset)

		got, err := segment.Read(offset)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err := segment.Append(want)
	require.Equal(t, io.EOF, err)
	// maxed index
	require.True(t, segment.IsMaxed())
}

func testSegmentRemove(t *testing.T, segment *segment, dir string) {
	segment.config.Segment.MaxStoreBytes = uint64(23) // 19 bytes (record) + 4 bytes (len width) = 23 bytes
	segment.config.Segment.MaxIndexBytes = 1024

	// insert one record
	offset, err := segment.Append(want)
	require.NoError(t, err)
	require.Equal(t, uint64(16), offset)
	got, err := segment.Read(offset)
	require.NoError(t, err)
	require.Equal(t, want.Value, got.Value)
	require.True(t, segment.IsMaxed()) // maxed store

	// recover state from exist file
	segmentN1, err := newSegment(dir, 16, segment.config)
	require.NoError(t, err)
	require.True(t, segmentN1.IsMaxed()) // maxed store
	require.NoError(t, segmentN1.Remove())

	segmentN2, err := newSegment(dir, 16, segment.config)
	require.NoError(t, err)
	require.False(t, segmentN2.IsMaxed())
}
