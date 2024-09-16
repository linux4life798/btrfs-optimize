//go:build amd64 && linux

// Package fstools provides access to low level syscalls for advanced filesystem
// functionality.
package fstools

import (
	"math"
	"unsafe"
)

// https://docs.kernel.org/filesystems/fiemap.html
// https://github.com/torvalds/linux/blob/master/include/uapi/linux/fiemap.h
// https://github.com/torvalds/linux/blob/master/include/uapi/linux/fs.h
// https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/misc/filefrag.c

const (
	FS_IOC_FIEMAP = 0xC020660B
)

// All constacts from uapi/linux/fiemap.h.
// https://github.com/torvalds/linux/blob/master/include/uapi/linux/fiemap.h
const (
	FIEMAP_MAX_OFFSET = math.MaxUint64

	FIEMAP_FLAG_SYNC  = 0x00000001 // sync file data before map
	FIEMAP_FLAG_XATTR = 0x00000002 // map extended attribute tree
	FIEMAP_FLAG_CACHE = 0x00000004 // request caching of the extents

	FIEMAP_FLAGS_COMPAT = (FIEMAP_FLAG_SYNC | FIEMAP_FLAG_XATTR)

	FIEMAP_EXTENT_LAST           = 0x00000001 // Last extent in file.
	FIEMAP_EXTENT_UNKNOWN        = 0x00000002 // Data location unknown.
	FIEMAP_EXTENT_DELALLOC       = 0x00000004 // Location still pending. Sets EXTENT_UNKNOWN.
	FIEMAP_EXTENT_ENCODED        = 0x00000008 // Data can not be read while fs is unmounted
	FIEMAP_EXTENT_DATA_ENCRYPTED = 0x00000080 // Data is encrypted by fs. Sets EXTENT_NO_BYPASS.
	FIEMAP_EXTENT_NOT_ALIGNED    = 0x00000100 // Extent offsets may not be block aligned.
	FIEMAP_EXTENT_DATA_INLINE    = 0x00000200 // Data mixed with metadata. Sets EXTENT_NOT_ALIGNED.
	FIEMAP_EXTENT_DATA_TAIL      = 0x00000400 // Multiple files in block. Sets EXTENT_NOT_ALIGNED.
	FIEMAP_EXTENT_UNWRITTEN      = 0x00000800 // Space allocated, but no data (i.e. zero).
	FIEMAP_EXTENT_MERGED         = 0x00001000 // File does not natively support extents. Result merged for efficiency.
	FIEMAP_EXTENT_SHARED         = 0x00002000 // Space shared with other files.
)

// Constants needed to calculate total ioctl request size.
// The filefrag command uses these to ensure that it only sends a
// 2048*8 (aligned to 64bit) sized buffer.
const (
	SizeofRawFiemap       = 32
	SizeofRawFiemapExtent = 56
)

type rawFiemap struct {
	Start          uint64 // in
	Length         uint64 // in
	Flags          uint32 // in/out
	Mapped_extents uint32 // out
	Extent_count   uint32 // in
	Reserved       uint32
}

type rawFiemapExtent FiemapExtent

type Fiemap struct {
	Start          uint64 // in
	Length         uint64 // in
	Flags          uint32 // in/out
	Mapped_extents uint32 // out
	Reserved       uint32
	Extents        []FiemapExtent // out
}

type FiemapExtent struct {
	Logical    uint64
	Physical   uint64
	Length     uint64
	Reserved64 [2]uint64
	Flags      uint32
	Reserved   [3]uint32
}

// IoctlFiemap performs an FIEMAP ioctl operation on a given fd.
//
// We choose to use the value.Extents field as purley as the output array to
// allow reuse on the calller side.
func IoctlFiemap(fd int, value *Fiemap) error {
	buf := make([]byte, SizeofRawFiemap+len(value.Extents)*SizeofRawFiemapExtent)
	bufPtr := unsafe.Pointer(&buf[0])

	// The make function seems to always allocate 8 byte aligned on amd64.
	if uintptr(bufPtr)%8 != 0 {
		panic("buffer for fiemap ioctl is not 64 bit aligned")
	}

	rawFm := (*rawFiemap)(bufPtr)
	rawFm.Start = value.Start
	rawFm.Length = value.Length
	rawFm.Flags = value.Flags
	rawFm.Mapped_extents = value.Mapped_extents
	rawFm.Extent_count = uint32(len(value.Extents))
	rawFm.Reserved = value.Reserved

	err := ioctlPtr(fd, FS_IOC_FIEMAP, bufPtr)

	// Output
	for i := range value.Extents {
		rawExtent := (*rawFiemapExtent)(unsafe.Add(bufPtr, SizeofRawFiemap+i*SizeofRawFiemapExtent))
		value.Extents[i] = FiemapExtent(*rawExtent)
	}

	value.Flags = rawFm.Flags
	value.Mapped_extents = rawFm.Mapped_extents
	value.Reserved = rawFm.Reserved

	return err
}
