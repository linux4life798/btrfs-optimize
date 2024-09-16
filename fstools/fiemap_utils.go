package fstools

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"text/tabwriter"
)

const (
	// fiemapIoctlBufferSize is the overall buffer size used in the filefrag
	// command.
	fiemapIoctlBufferSize = 2048 * 8
)

// FiemapExtentFlagsToStrings converts FIEMAP extent flags to human-readable strings.
func FiemapExtentFlagsToStrings(flags uint32) []string {
	flagDefs := []struct {
		flag uint32
		name string
	}{
		{FIEMAP_EXTENT_LAST, "last"},
		{FIEMAP_EXTENT_UNKNOWN, "unknown"},
		{FIEMAP_EXTENT_DELALLOC, "delalloc"},
		{FIEMAP_EXTENT_ENCODED, "encoded"},
		{FIEMAP_EXTENT_DATA_ENCRYPTED, "data_encrypted"},
		{FIEMAP_EXTENT_NOT_ALIGNED, "not_aligned"},
		{FIEMAP_EXTENT_DATA_INLINE, "data_inline"},
		{FIEMAP_EXTENT_DATA_TAIL, "data_tail"},
		{FIEMAP_EXTENT_UNWRITTEN, "unwritten"},
		{FIEMAP_EXTENT_MERGED, "merged"},
		{FIEMAP_EXTENT_SHARED, "shared"},
	}

	var result []string
	for _, fd := range flagDefs {
		if flags&fd.flag != 0 {
			result = append(result, fd.name)
		}
		flags &= ^fd.flag
	}

	if flags != 0 {
		// Handle any undocumented flags
		result = append(result, fmt.Sprintf("0x%X", flags))
	}

	return result
}

// FiemapWalkCallback defines the callback signature for FiemapWalk.
type FiemapWalkCallback func(index int, extent *FiemapExtent) (finished bool)

// FiemapWalk iterates over all extents that back the given file,
// calling the provided callback for each extent.
//
// The flags value can 0 as the defualt, otherwise, you can set it to the
// bitwise or of FIEMAP_FLAG_SYNC, FIEMAP_FLAG_XATTR, or FIEMAP_FLAG_CACHE.
func FiemapWalk(file *os.File, flags uint32, callback FiemapWalkCallback) error {
	// Calculate the number of extents based on the overall ioctl request
	// buffer size, specifically as done in filefrag command.
	numExtents := (fiemapIoctlBufferSize - SizeofRawFiemap) / SizeofRawFiemapExtent
	fmExtents := make([]FiemapExtent, numExtents)

	var nextExtentIndexOffset int
	var nextLogicalStart uint64
	for {
		fm := Fiemap{
			Start:   nextLogicalStart,
			Length:  FIEMAP_MAX_OFFSET,
			Flags:   flags,
			Extents: fmExtents,
		}
		if err := IoctlFiemap(int(file.Fd()), &fm); err != nil {
			return err
		}

		if fm.Mapped_extents == 0 {
			return nil
		}

		for i := 0; i < int(fm.Mapped_extents); i++ {
			index := nextExtentIndexOffset + i
			extent := &fm.Extents[i]
			if callback(index, extent) {
				return nil
			}
			if extent.Flags&FIEMAP_EXTENT_LAST != 0 {
				return nil
			}
		}
		nextExtentIndexOffset += int(fm.Mapped_extents)
		nextLogicalStart = fm.Extents[fm.Mapped_extents-1].Logical + fm.Extents[fm.Mapped_extents-1].Length
	}
}

// FileFragDumpExtents prints all extents that compose the given filePath.
// This is very similar to using the "filefrag -v <path>" command.
//
// All options should be false, by default.
// If syncFirst is enabled, the requested file will be synced to disk before
// reading the extents.
// If useBytes is enabled, the units will by in Bytes, which is the default
// unit received by the FIEMAP ioctl.
// If faster is enabled, the pretty printing functionality will be disabled.
//
// See https://docs.kernel.org/filesystems/fiemap.html,
// https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/misc/filefrag.c,
// and https://github.com/torvalds/linux/blob/master/include/uapi/linux/fiemap.h
// for more information.
func FileFragDumpExtents(filePath string, syncFirst bool, useBytes bool, faster bool) error {
	fmt.Println("File:", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	sysStat := new(syscall.Stat_t)
	if err := syscall.Fstat(int(file.Fd()), sysStat); err != nil {
		panic(err)
	}
	blkSize := uint64(sysStat.Blksize)
	fmt.Println("File Size  (Bytes):", sysStat.Size)
	fmt.Println("Block Size (Bytes):", blkSize)
	units := "Blocks"
	if useBytes {
		units = "Bytes"
		blkSize = 1
	}
	fmt.Println("Start/Length Units:", units)

	var w io.Writer
	if faster {
		w = bufio.NewWriter(os.Stdout)
		defer w.(*bufio.Writer).Flush()
	} else {
		w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		defer w.(*tabwriter.Writer).Flush()
	}

	fmt.Fprintln(w, "Extent-Index\tLogical-Start\tPhysical-Start\tLength\tFlags")

	var flags uint32
	if syncFirst {
		flags |= FIEMAP_FLAG_SYNC
	}
	err = FiemapWalk(file, flags, func(index int, extent *FiemapExtent) bool {
		fmt.Fprintf(
			w,
			"%d\t%d\t%d\t%d\t",
			index,
			extent.Logical/blkSize,
			extent.Physical/blkSize,
			extent.Length/blkSize,
		)
		if extent.Logical%blkSize != 0 || extent.Physical%blkSize != 0 || extent.Length%blkSize != 0 {
			panic("logical start, pysical start, or length are not block size aligned")
		}

		flagNames := FiemapExtentFlagsToStrings(extent.Flags)
		fmt.Fprintln(w, strings.Join(flagNames, ","))
		return false
	})

	if err != nil {
		return fmt.Errorf("failed to walk fiemap: %v", err)
	}

	return nil
}
