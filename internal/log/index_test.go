package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var maxIndexBytes = uint64(1024)

func TestIndex(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, index *index){
		"AppendRead":           testIndexAppendRead,
		"ReadOutOfRange":       testIndexReadOutOfRange,
		"InitWithExistingFile": testIndexInitWithExistingFile,
	} {
		t.Run(scenario, func(t *testing.T) {
			f, err := ioutil.TempFile("", "index_test")
			require.NoError(t, err)
			defer os.Remove(f.Name())

			index, err := newIndex(f, maxIndexBytes)
			require.NoError(t, err)
			require.Equal(t, f.Name(), index.Name())
			fn(t, index)
		})
	}
}

type Entry struct {
	Offset uint32
	Pos    uint64
}

func testIndexAppendRead(t *testing.T, index *index) {
	entries := []Entry{
		{Offset: 0, Pos: 0},
		{Offset: 1, Pos: 10},
	}
	for _, want := range entries {
		err := index.Write(want.Offset, want.Pos)
		require.NoError(t, err)

		_, pos, err := index.Read(int64(want.Offset))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}
}

func testIndexReadOutOfRange(t *testing.T, index *index) {
	_, _, err := index.Read(-1)
	require.Equal(t, io.EOF, err)

	want := Entry{Offset: 0, Pos: 0}
	err = index.Write(want.Offset, want.Pos)
	require.NoError(t, err)

	offset, pos, err := index.Read(0)
	require.NoError(t, err)
	require.Equal(t, want.Offset, offset)
	require.Equal(t, want.Pos, pos)

	_, _, err = index.Read(1)
	require.Equal(t, io.EOF, err)
}

func testIndexInitWithExistingFile(t *testing.T, index *index) {
	entries := []Entry{
		{Offset: 0, Pos: 0},
		{Offset: 1, Pos: 10},
	}
	for _, want := range entries {
		err := index.Write(want.Offset, want.Pos)
		require.NoError(t, err)

		_, pos, err := index.Read(int64(want.Offset))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}
	err := index.Close()
	require.NoError(t, err)

	// index should build its state from the existing file
	f, err := os.OpenFile(index.file.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	indexN, err := newIndex(f, maxIndexBytes)
	require.NoError(t, err)
	offset, pos, err := indexN.Read(-1)
	require.NoError(t, err)
	require.Equal(t, entries[len(entries)-1].Offset, offset)
	require.Equal(t, entries[len(entries)-1].Pos, pos)
}
