package main

// ioctl_ficlonerange - https://man7.org/linux/man-pages/man2/ioctl_ficlone.2.html
// ioctl_fideduperange - https://www.man7.org/linux/man-pages/man2/ioctl_fideduperange.2.html
//
// You can use "sudo filefrag -v <file_path>" to inspect if extents are shared.
// sudo btrfs filesystem du -s <file_path>

import (
	"fmt"
	"os"

	"github.com/linux4life798/btrfs-optimize/fstools"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

// Future SubCommands:
//
// hashcache build <path>  - Walk the path dir/file structure, hash files, and add them to cache, if needed.
// hashcache prune         - Iterate through all items in cache and check if they exist and if the timestamp is valid.
// hashcache purge         - Delete cache file.
//
// defrag <path>           - Defrag each file, but rebuild the shared/deduped file connections

const (
	Kibibyte uint64 = 1024
	Mebibyte        = 1024 * Kibibyte
	Gibibyte        = 1024 * Mebibyte
	Tebibyte        = 1024 * Gibibyte
)

func fileSize(file *os.File) int64 {
	// Get file stats to determine file size for deduplication
	info, err := file.Stat()
	if err != nil {
		fmt.Printf("failed to get source file stat: %v", err)
	}
	return info.Size()
}

var rootCmd = &cobra.Command{
	Use:   "dedupe-tool",
	Short: "A tool for file deduplication operations",
	Long:  `A CLI tool that performs various file deduplication operations including deduplication and checking.`,
}

var dedupeCmd = &cobra.Command{
	Use:   "dedupe <source-file> <target-file> [target-file...]",
	Short: "Dedupe performs block deduplication between files",
	Long:  `Dedupe is a subcommand that performs block deduplication between a source file and multiple target files.`,
	Args:  cobra.MinimumNArgs(2),
	Run:   runDedupe,
}

var inspectCmd = &cobra.Command{
	Use:   "inspect <file-path> [file-path...]",
	Short: "Inspect deduplication status of files",
	Long:  `Inspect is a subcommand that checks the deduplication status of one or more files.`,
	Args:  cobra.MinimumNArgs(1),
	Run:   runInspect,
}

func init() {
	rootCmd.AddCommand(dedupeCmd)

	inspectCmd.Flags().BoolP("sync", "s", false, "Sync the file to disk before requeting the extents map")
	inspectCmd.Flags().BoolP("bytes", "b", false, "Print offsets and lengths in Bytes instead of Blocks")
	inspectCmd.Flags().BoolP("fast", "f", false, "Disable pretty print features to speed up runtime")
	rootCmd.AddCommand(inspectCmd)
}

func runDedupe(cmd *cobra.Command, args []string) {
	sourceFile := args[0]
	destinationFiles := args[1:]

	// Testing shows that when you call the ioctl teh max deduped file size
	// in bytes is 1GiB, but you can still ask for the whole file.
	// if err := dedupeFiles(sourceFile, destinationFiles, 1*Tebibyte); err != nil {
	//  log.Fatalf("Error: %v\n", err)
	// }

	srcFile, err := os.Open(sourceFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening source file: %v\n", err)
		return
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting source file info: %v\n", err)
		return
	}

	value := &unix.FileDedupeRange{
		Src_offset: 0,
		Src_length: uint64(srcInfo.Size()),
		Info:       make([]unix.FileDedupeRangeInfo, len(destinationFiles)),
	}

	for i, destFile := range destinationFiles {
		destFd, err := unix.Open(destFile, unix.O_RDONLY, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening destination file %s: %v\n", destFile, err)
			return
		}
		defer unix.Close(destFd)

		value.Info[i] = unix.FileDedupeRangeInfo{
			Dest_fd:     int64(destFd),
			Dest_offset: 0,
		}
	}

	progressBar := progressbar.DefaultBytes(
		srcInfo.Size(),
		"deduping",
	)
	progress := func(bytesDeduped, bytesLength uint64, exit bool) {
		if exit {
			progressBar.Exit()
			return
		}
		progressBar.Set64(int64(bytesDeduped))
		// fmt.Printf("Deduped %d of %d bytes (%.2f%%)\n", bytesDeduped, bytesLength, float64(bytesDeduped)/float64(bytesLength)*100)
	}

	err = fstools.FileDedupeRangeFull(int(srcFile.Fd()), value, progress)
	if err == unix.EOPNOTSUPP {
		fmt.Fprintln(
			os.Stderr,
			"deduplication not supported on this filesystem",
		)
		return
	}
	if err == unix.EINVAL {
		fmt.Fprintln(
			os.Stderr,
			// could be that the offsets are not block size aligned
			"arguments are incompatible or deduplication not supported on this filesystem",
		)
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error during deduplication:", err)
		return
	}

	var errorSeen bool
	for i, info := range value.Info {
		if info.Status != unix.FILE_DEDUPE_RANGE_SAME {
			fmt.Fprintf(
				os.Stderr,
				"Destination %s failed with %s.\n",
				destinationFiles[i],
				fstools.FileDedupeRangeStatusToString(info.Status),
			)
			errorSeen = true
		}
	}

	if !errorSeen {
		fmt.Println("Deduplication completed successfully.")
	}
}

func runInspect(cmd *cobra.Command, args []string) {
	syncFirst, _ := cmd.Flags().GetBool("sync")
	useBytes, _ := cmd.Flags().GetBool("bytes")
	faster, _ := cmd.Flags().GetBool("fast")

	for _, filePath := range args {
		err := fstools.FileFragDumpExtents(filePath, syncFirst, useBytes, faster)
		if err != nil {
			fmt.Printf("Error showing extents for %s: %v\n", filePath, err)
		}
		fmt.Println()
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
