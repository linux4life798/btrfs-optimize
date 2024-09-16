package fstools

import (
	"fmt"

	"golang.org/x/sys/unix"
)

type FileDedupeRangeFullProgress func(bytesDeduped, bytesLength uint64, exit bool)

// FileDedupeRangeFull is a wrapper around IoctlFileDedupeRange that is able
// to fulfill deduping full file lengths and is resilient to destination file
// dedupe failures.
//
// The main IoctlFileDedupeRange directly calls the FIDEDUPERANGE ioctl,
// which only dedupes up to 1GiB using btrfs and possibly less on other
// filesystem. This wrapper will iterate and continue to deduplicate the
// full source length specified in value.Src_length.
// Given that this can take a very long time for large files, the progress
// callback is provided to show updates.
func FileDedupeRangeFull(
	srcFd int,
	value *unix.FileDedupeRange,
	progress FileDedupeRangeFullProgress,
) error {
	if progress != nil {
		defer progress(0, 0, true)
	}

	if value == nil {
		panic("value is nil")
	}
	if len(value.Info) == 0 {
		panic("value.Info array empty")
	}

	// Copy the value into the local requect variable, since we may need to
	// make multiple subsequent requests to cover the full Src_length and we
	// don't want to alter the caller's copy.
	req := &unix.FileDedupeRange{
		Src_offset: value.Src_offset,
		Src_length: value.Src_length,
		Reserved1:  value.Reserved1,
		Reserved2:  value.Reserved2,
		Info:       make([]unix.FileDedupeRangeInfo, len(value.Info)),
	}
	copy(req.Info, value.Info)

	// Keep track of original indices to allow for removing failed destination
	// files from subsequent requests, but keep the original destination
	// indices for updating the destination status/error.
	indices := make([]int, len(value.Info))
	for i := range indices {
		indices[i] = i
	}

	// These arrays should be rather small, so I don't believe copying will
	// be a major performance issue.
	// The dropList argument must be sorted ascending.
	drop := func(dropList []int) {
		if len(dropList) == 0 {
			return
		}
		dropIndex := 0
		dstIndex := 0
		for srcIndex := range value.Info {
			if srcIndex == dropList[dropIndex] {
				dropIndex++
				continue
			}
			if srcIndex != dstIndex {
				req.Info[dstIndex] = req.Info[srcIndex]
				indices[dstIndex] = indices[srcIndex]
			}
			dstIndex++
		}
		req.Info = req.Info[:dstIndex]
		indices = indices[:dstIndex]
	}

	if progress != nil {
		progress(0, value.Src_length, false)
	}
	for {
		if err := unix.IoctlFileDedupeRange(srcFd, req); err != nil {
			return err
		}

		// Check Assertions
		//
		// Note that this utility assumes that all destination files will
		// dedupe the same amount of bytes on each ioctl, given files that
		// match. As in, this utility will not request dedupe for the
		// same/overlaping region, if the kernel is somehow doesn't want to
		// dedupe the final bytes of one or more particular destination files.

		var dedupeBytes uint64
		var dedupeBytesValid bool
		for _, info := range req.Info {
			if !dedupeBytesValid && info.Status == unix.FILE_DEDUPE_RANGE_SAME {
				dedupeBytes = info.Bytes_deduped
				dedupeBytesValid = true
			}
			if info.Status == unix.FILE_DEDUPE_RANGE_SAME {
				if info.Bytes_deduped != dedupeBytes {
					panic("found a successfully deduped file, but had varying dedupe bytes")
				}
			}
		}
		if dedupeBytes > req.Src_length {
			panic("deduped more bytes than requested")
		}

		req.Src_offset += dedupeBytes
		req.Src_length -= dedupeBytes

		if progress != nil {
			progress(req.Src_offset, value.Src_length, false)
		}

		var dropList = make([]int, 0, len(req.Info))
		for i, info := range req.Info {
			value.Info[indices[i]].Bytes_deduped += info.Bytes_deduped
			value.Info[indices[i]].Status = info.Status

			if info.Status == unix.FILE_DEDUPE_RANGE_SAME {
				req.Info[i].Dest_offset += dedupeBytes
			} else {
				dropList = append(dropList, i)
			}
		}
		if req.Src_length == 0 {
			return nil
		}
		drop(dropList)

		if len(req.Info) == 0 {
			return nil
		}
	}
}

// FileDedupeRangeStatusToString converts a FileDedupeRangeInfo.Status to a
// human-readable string.
func FileDedupeRangeStatusToString(status int32) string {
	if status < 0 {
		return "errno " + unix.Errno(-status).Error()
	}
	switch status {
	case unix.FILE_DEDUPE_RANGE_SAME:
		return "range same"
	case unix.FILE_DEDUPE_RANGE_DIFFERS:
		return "range differs"
	default:
		return fmt.Sprintf("unknown status(%d)", status)
	}
}
