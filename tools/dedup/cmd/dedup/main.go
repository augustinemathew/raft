// dedup is a CLI wrapper around the dedup package.
//
//	dedup [flags] <root> [root...]
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/augustinemathew/raft/tools/dedup"
)

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

func main() {
	cfg := dedup.DefaultConfig()

	var excludes stringList
	var (
		action      string
		keeper      string
		output      string
		verbose     bool
		force       bool
		crossDevice bool
		followSym   bool
		includeZero bool
	)
	flag.Var(&excludes, "exclude", "shell glob matched against base name (repeatable)")
	flag.Int64Var(&cfg.MinSize, "min-size", cfg.MinSize, "skip files smaller than N bytes")
	flag.Int64Var(&cfg.MaxSize, "max-size", 0, "skip files larger than N bytes (0 = unlimited)")
	flag.BoolVar(&includeZero, "zero", false, "include zero-byte files")
	flag.BoolVar(&crossDevice, "xdev", false, "cross filesystem boundaries (default false)")
	flag.BoolVar(&followSym, "follow-symlinks", false, "follow symlinks")
	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "concurrent hash workers")
	flag.Int64Var(&cfg.QuickHashBytes, "quick-bytes", cfg.QuickHashBytes, "bytes at head+tail for quick hash")
	flag.IntVar(&cfg.ReadBufferBytes, "buf", cfg.ReadBufferBytes, "read buffer size in bytes")
	flag.StringVar(&action, "action", string(cfg.Action), "report|hardlink|reflink|delete")
	flag.StringVar(&keeper, "keeper", string(cfg.Keeper), "first|shortest|oldest|newest")
	flag.StringVar(&output, "output", "", "write JSON report to path (default stdout)")
	flag.BoolVar(&verbose, "v", false, "verbose logging to stderr")
	flag.BoolVar(&force, "force", false, "apply destructive actions (without this, dry-run only)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <root> [root...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	cfg.Roots = flag.Args()
	cfg.Excludes = excludes
	cfg.Action = dedup.Action(action)
	cfg.Keeper = dedup.Keeper(keeper)
	cfg.Output = output
	cfg.CrossDevice = crossDevice
	cfg.FollowSymlinks = followSym
	cfg.IncludeZero = includeZero
	cfg.DryRun = !force

	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rep, err := dedup.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dedup:", err)
		os.Exit(1)
	}
	// Summary to stderr so piping JSON to another process still works.
	fmt.Fprintf(os.Stderr,
		"scanned=%d hashed=%d dup_groups=%d dup_bytes=%d reclaimed=%d errors=%d\n",
		rep.FilesScanned, rep.FilesHashed, len(rep.Groups),
		rep.TotalDupBytes, rep.ReclaimedBytes, len(rep.Errors))
}
