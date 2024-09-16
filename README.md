# Optimize BTRFS using `btrfs-optimize`

The `dedupe` subcommand allows you direct control to deduplicate entire files,
effectivley reconnecting divergent snapshots. Additionally, the integrated
`inspect` subcommand allows you to check/verify what extents are shared with
other files.

Other utilites deduplicate at the extent level, which may be a good
best-effort whole filesystem deduplicator, but can cause imperfect
deduplication of identical files.

**Subcommands:**

* `dedupe <src-file-path> <destination-file-path1> [destination-file-path2...]`
* `inspect <file-path1> [file-path2...]`
