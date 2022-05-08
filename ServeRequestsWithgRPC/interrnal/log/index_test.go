package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	/*
		This code sets up the test. We create an index file and make it big enough to
		contain our test entries via the Truncate() call. We have to grow the file before
		we use it because we memory-map the file to a slice of bytes and if we didn’t
		increase the size of the file before we wrote to it, we’d get an out-of-bounds
		error.
	*/
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.NoError(t, err)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	/*
		We iterate over each entry and write it to the index. We check that we can
		read the same entry back via the Read() method. Then we verify that the index
		and scanner error when we try to read beyond the number of entries stored
		in the index. And we check that the index builds its state from the existing
		file, for when our service restarts with existing data.
		We need to configure the max size of a segment’s store and index. Let’s add
		a config struct to centralize the log’s configuration, making it easy to configure
	*/
	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}
	for _, want := range entries {
		err := idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// index and scanner should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
