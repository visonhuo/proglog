package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/visonhuo/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"AppendReadRecord":         testLogAppendRead,
		"OffsetOutOfRangeErr":      testLogOutOfRangeErr,
		"InitWithExistingSegments": testLogInitExistingSegments,
		"LogReader":                testLogReader,
		"LogTruncate":              testLogTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "log_test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testLogAppendRead(t *testing.T, log *Log) {
	want := &api.Record{Value: []byte("hello world")}
	offset, err := log.Append(want)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	read, err := log.Read(offset)
	require.NoError(t, err)
	require.Equal(t, want.Value, read.Value)
}

func testLogOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
}

func testLogInitExistingSegments(t *testing.T, log *Log) {
	want := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(want)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(0), log.LowestOffset())
	require.Equal(t, uint64(2), log.HighestOffset())
	require.NoError(t, log.Close())

	// create a new log struct with the same dir
	logN, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)
	require.Equal(t, uint64(0), logN.LowestOffset())
	require.Equal(t, uint64(2), logN.HighestOffset())
}

func testLogReader(t *testing.T, log *Log) {
	want := &api.Record{Value: []byte("hello world")}
	offset, err := log.Append(want)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read) // entry[len, data]
	require.NoError(t, err)
	require.Equal(t, want.Value, read.Value)
}

func testLogTruncate(t *testing.T, log *Log) {
	want := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(want)
		require.NoError(t, err)
	}

	// The width of `want` record is 19 bytes (4bytes offset, 15bytes value), plus 4 bytes len width,
	// each record will take 23 bytes in store file.
	// And MaxStoreBytes = 32, so the third record (offset=2) will be stored to another segment,
	// which make truncate method work.
	err := log.Truncate(1)
	require.NoError(t, err)
	_, err = log.Read(0)
	require.Error(t, err)
}
