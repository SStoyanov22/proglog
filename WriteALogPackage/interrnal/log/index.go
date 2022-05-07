package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

/*
Index defines our index file, which comprises a persisted file and a memory mapped file.
The size tells us the size of the index and where to write the nextentry appended to the index.
*/
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

/*
Creates an index for the given file. We create the index and save the current size
of the file so we can track the amount of data in the index file as we add index
entries. We grow the file to the max index size before memory-mapping the file
(The reason we resize them now is that, once they’re memory-mapped, we can’t resize
 them, so it’s now or never) and then return the created index to the caller.
*/
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

/*
Makes sure the memory-mapped file has synced its data to the persisted
file and that the persisted file has flushed its contents to stable storage for safety
(graceful shutdown). Then it truncates the persisted file to the amount of data that’s
actually in it and closes the file.
*/
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}

/*
Takes in an offset and returns the associated record’s position in
the store. The given offset is relative to the segment’s base offset; 0 is always
the offset of the index’s first entry, 1 is the second entry, and so on. We use
relative offsets to reduce the size of the indexes by storing offsets as uint32s.
If we used absolute offsets, we’d have to store the offsets as uint64s and
require four more bytes for each entry. Four bytes doesn’t sound like much,
until you multiply it by the number of records people often use distributed
logs for.
*/
func (i *index) Read(in int64) (off uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	//If -1 then put at the end
	if in == -1 {
		off = uint32(i.size/entWidth - 1)
	} else {
		off = uint32(i.size)
	}

	pos = entWidth * uint64(off)

	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	off = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return off, pos, nil
}

/*
Appends the given offset and position to the index.
First, we validate that we have space to write the entry. If there’s space, we
then encode the offset and position and write them to the memory-mapped
file. Then we increment the position where the next write will go.
*/
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)

	return nil
}
