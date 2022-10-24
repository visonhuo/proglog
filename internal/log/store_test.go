package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write   = []byte("hello world")
	width   = uint64(len(write)) + lenWidth
	dataCnt = uint64(4)
)

func TestStore(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, store *store){
		"AppendRead":   testStoreAppendRead,
		"AppendReadAt": testStoreAppendReadAt,
		"Close":        testStoreClose,
	} {
		t.Run(scenario, func(t *testing.T) {
			f, err := ioutil.TempFile("", "store_test")
			require.NoError(t, err)
			defer os.Remove(f.Name())

			s, err := newStore(f)
			require.NoError(t, err)
			require.Equal(t, f.Name(), s.Name())
			fn(t, s)
		})
	}
}

func testStoreAppendRead(t *testing.T, store *store) {
	for i := uint64(1); i < dataCnt; i++ {
		n, pos, err := store.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}

	for i, pos := uint64(1), uint64(0); i < dataCnt; i++ {
		read, err := store.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testStoreAppendReadAt(t *testing.T, store *store) {
	for i := uint64(1); i < dataCnt; i++ {
		n, pos, err := store.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}

	for i, off := uint64(1), int64(0); i < dataCnt; i++ {
		b := make([]byte, lenWidth)
		n, err := store.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		off += lenWidth

		size := enc.Uint64(b)
		data := make([]byte, size)
		n, err = store.ReadAt(data, off)
		require.NoError(t, err)
		require.Equal(t, write, data)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func testStoreClose(t *testing.T, store *store) {
	n, _, err := store.Append(write)
	require.NoError(t, err)
	require.Equal(t, width, n)

	// buffer still not be flushed yet
	fi, err := os.Stat(store.file.Name())
	require.NoError(t, err)
	beforeSize := fi.Size()

	// flush buffer when close
	err = store.Close()
	require.NoError(t, err)

	fi, err = os.Stat(store.file.Name())
	require.NoError(t, err)
	afterSize := fi.Size()
	t.Logf("afterSize: %v, beforeSize: %v", afterSize, beforeSize)
	require.True(t, afterSize > beforeSize)
}
