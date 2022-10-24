package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth   uint64 = 4
	posWidth   uint64 = 8
	entryWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, maxIndexBytes uint64) (*index, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// grow the file to the max index size before memory-mapping the file
	if err = os.Truncate(f.Name(), int64(maxIndexBytes)); err != nil {
		return nil, err
	}

	mmap, err := gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &index{
		file: f,
		mmap: mmap,
		size: uint64(fi.Size()),
	}, nil
}

func (i *index) Write(offset uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entryWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], offset)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entryWidth], pos)
	i.size += entryWidth
	return nil
}

func (i *index) Read(in int64) (offset uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		// represent the last entry
		offset = uint32((i.size / entryWidth) - 1)
	} else {
		offset = uint32(in)
	}
	pos = uint64(offset) * entryWidth // entry start pos (index file)
	if i.size < pos+entryWidth {
		return 0, 0, io.EOF
	}
	offset = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entryWidth]) // record data pos (store file)
	return offset, pos, nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	// sync data to the persisted file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	// truncate file to its actual data size
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
