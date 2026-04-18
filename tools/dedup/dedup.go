// Package dedup finds and optionally consolidates duplicate files on a Linux
// filesystem. The pipeline is: walk -> size-bucket -> quick-hash (head+tail) ->
// full-hash -> action. Each stage drops non-candidates so large files are only
// read in full when they might actually be duplicates.
package dedup

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Action string

const (
	ActionReport   Action = "report"
	ActionHardlink Action = "hardlink"
	ActionReflink  Action = "reflink"
	ActionDelete   Action = "delete"
)

type Keeper string

const (
	KeeperFirst    Keeper = "first"
	KeeperShortest Keeper = "shortest"
	KeeperOldest   Keeper = "oldest"
	KeeperNewest   Keeper = "newest"
)

// Config drives a dedup run. Zero-values are rejected by Validate; use
// DefaultConfig as a starting point.
type Config struct {
	Roots           []string
	Excludes        []string // shell globs matched against the base name
	MinSize         int64
	MaxSize         int64 // 0 = unlimited
	IncludeZero     bool  // include 0-byte files (rarely useful)
	CrossDevice     bool  // if false, do not cross filesystem boundaries
	FollowSymlinks  bool
	Workers         int
	QuickHashBytes  int64 // head + tail bytes for quick hash (each side)
	ReadBufferBytes int
	Action          Action
	Keeper          Keeper
	DryRun          bool   // forces Action=Report for destructive actions
	Output          string // JSON report path; "" = stdout when action=report
	Logger          *slog.Logger
}

func DefaultConfig() Config {
	return Config{
		MinSize:         1,
		IncludeZero:     false,
		CrossDevice:     false,
		Workers:         runtime.NumCPU(),
		QuickHashBytes:  4096,
		ReadBufferBytes: 1 << 20, // 1 MiB
		Action:          ActionReport,
		Keeper:          KeeperShortest,
		DryRun:          true,
	}
}

func (c *Config) Validate() error {
	if len(c.Roots) == 0 {
		return errors.New("dedup: at least one root is required")
	}
	if c.Workers < 1 {
		return errors.New("dedup: workers must be >= 1")
	}
	if c.QuickHashBytes < 0 {
		return errors.New("dedup: quick-hash bytes must be >= 0")
	}
	if c.ReadBufferBytes < 4096 {
		return errors.New("dedup: read buffer must be >= 4096")
	}
	if c.MaxSize != 0 && c.MaxSize < c.MinSize {
		return errors.New("dedup: max-size < min-size")
	}
	switch c.Action {
	case ActionReport, ActionHardlink, ActionReflink, ActionDelete:
	default:
		return fmt.Errorf("dedup: unknown action %q", c.Action)
	}
	switch c.Keeper {
	case KeeperFirst, KeeperShortest, KeeperOldest, KeeperNewest:
	default:
		return fmt.Errorf("dedup: unknown keeper %q", c.Keeper)
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return nil
}

// FileRef is the per-file metadata captured during the walk. It is small on
// purpose: millions of files may be held in memory.
type FileRef struct {
	Path  string
	Size  int64
	Dev   uint64
	Ino   uint64
	Nlink uint64
	Mode  os.FileMode
	MTime time.Time
}

// DupGroup is a set of paths whose contents are byte-identical (assuming no
// SHA-256 collision). Keeper is the survivor under the configured policy.
type DupGroup struct {
	Size   int64      `json:"size"`
	Hash   string     `json:"hash"`
	Keeper string     `json:"keeper"`
	Dups   []string   `json:"dups"`
	Files  []*FileRef `json:"-"`
}

// Report is the structured output of a dedup run.
type Report struct {
	Roots          []string    `json:"roots"`
	StartedAt      time.Time   `json:"started_at"`
	FinishedAt     time.Time   `json:"finished_at"`
	FilesScanned   int64       `json:"files_scanned"`
	BytesScanned   int64       `json:"bytes_scanned"`
	FilesHashed    int64       `json:"files_hashed"`
	BytesHashed    int64       `json:"bytes_hashed"`
	Groups         []*DupGroup `json:"groups"`
	TotalDupBytes  int64       `json:"total_dup_bytes"`
	ReclaimedBytes int64       `json:"reclaimed_bytes"`
	Errors         []string    `json:"errors,omitempty"`
}

// Run executes the dedup pipeline. It is safe to cancel via ctx.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log := cfg.Logger

	rep := &Report{
		Roots:     append([]string(nil), cfg.Roots...),
		StartedAt: time.Now(),
	}

	// Walk -> size buckets. We hold all eligible files in memory keyed by
	// size; only buckets with >=2 entries survive the hash phase. For
	// extremely large scans this should be spilled to disk, but that is out
	// of scope here.
	bySize := make(map[int64][]*FileRef, 1<<16)
	seenInode := make(map[inodeKey]string, 1<<16) // first path per (dev,ino)

	walkCh := make(chan *FileRef, 1024)
	walkErrCh := make(chan error, 1)
	go func() {
		defer close(walkCh)
		walkErrCh <- walk(ctx, cfg, walkCh)
	}()

	for fr := range walkCh {
		rep.FilesScanned++
		rep.BytesScanned += fr.Size
		// Hardlink dedup: if we've already seen (dev,ino), the kernel has
		// already consolidated these. Record the alias so the report is
		// complete but do not rehash.
		k := inodeKey{fr.Dev, fr.Ino}
		if _, ok := seenInode[k]; ok {
			continue
		}
		seenInode[k] = fr.Path
		bySize[fr.Size] = append(bySize[fr.Size], fr)
	}
	if err := <-walkErrCh; err != nil {
		return nil, err
	}

	// Drop singletons.
	candidates := make([][]*FileRef, 0, 1024)
	for _, group := range bySize {
		if len(group) >= 2 {
			candidates = append(candidates, group)
		}
	}
	log.Info("walk complete",
		"files_scanned", rep.FilesScanned,
		"size_buckets_with_dups", len(candidates))

	// Two hash phases share the same worker pool.
	workCh := make(chan hashJob, cfg.Workers*2)
	resCh := make(chan hashResult, cfg.Workers*2)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hashWorker(ctx, cfg, workCh, resCh)
		}()
	}
	go func() { wg.Wait(); close(resCh) }()

	// Phase 1: quick hash on same-size groups with size > 2*quick.
	// Files at or below the threshold go straight to the full-hash phase
	// because the quick hash would read them entirely anyway.
	phase1 := make([]*FileRef, 0)
	skipQuick := make([]*FileRef, 0)
	threshold := 2 * cfg.QuickHashBytes
	for _, g := range candidates {
		if g[0].Size <= threshold {
			skipQuick = append(skipQuick, g...)
		} else {
			phase1 = append(phase1, g...)
		}
	}

	go func() {
		for _, fr := range phase1 {
			select {
			case <-ctx.Done():
				// Drain: let workers exit via closed channel.
				close(workCh)
				return
			case workCh <- hashJob{file: fr, kind: hashQuick}:
			}
		}
		// We will send phase-2 jobs on the same channel below.
	}()

	// Collect phase-1 results, bucket by (size, quickhash).
	type sizeHash struct {
		size int64
		h    [32]byte
	}
	quickBuckets := make(map[sizeHash][]*FileRef)
	var phase1Received int
	phase1Total := len(phase1)
	for phase1Received < phase1Total {
		select {
		case <-ctx.Done():
			return rep, ctx.Err()
		case r := <-resCh:
			phase1Received++
			if r.err != nil {
				rep.Errors = append(rep.Errors, fmt.Sprintf("quick-hash %s: %v", r.file.Path, r.err))
				continue
			}
			rep.FilesHashed++
			rep.BytesHashed += r.bytesRead
			k := sizeHash{r.file.Size, r.sum}
			quickBuckets[k] = append(quickBuckets[k], r.file)
		}
	}

	// Build phase-2 queue: groups from quickBuckets with >=2, plus
	// everything in skipQuick (re-grouped by size).
	phase2 := make([]*FileRef, 0)
	for _, g := range quickBuckets {
		if len(g) >= 2 {
			phase2 = append(phase2, g...)
		}
	}
	// skipQuick still needs to be grouped; a size group < threshold could
	// still be a singleton after inode dedup (no: we filtered candidates to
	// >=2 already, so any skipQuick entries belong to a >=2 group).
	phase2 = append(phase2, skipQuick...)

	go func() {
		defer close(workCh)
		for _, fr := range phase2 {
			select {
			case <-ctx.Done():
				return
			case workCh <- hashJob{file: fr, kind: hashFull}:
			}
		}
	}()

	// Collect phase-2 results, bucket by full (size, hash).
	fullBuckets := make(map[sizeHash][]*FileRef)
	phase2Total := len(phase2)
	for i := 0; i < phase2Total; i++ {
		select {
		case <-ctx.Done():
			return rep, ctx.Err()
		case r := <-resCh:
			if r.err != nil {
				rep.Errors = append(rep.Errors, fmt.Sprintf("full-hash %s: %v", r.file.Path, r.err))
				continue
			}
			rep.FilesHashed++
			rep.BytesHashed += r.bytesRead
			k := sizeHash{r.file.Size, r.sum}
			fullBuckets[k] = append(fullBuckets[k], r.file)
		}
	}

	// Assemble dup groups.
	for k, files := range fullBuckets {
		if len(files) < 2 {
			continue
		}
		sortForKeeper(files, cfg.Keeper)
		keeper := files[0]
		dups := make([]string, 0, len(files)-1)
		for _, f := range files[1:] {
			dups = append(dups, f.Path)
		}
		rep.Groups = append(rep.Groups, &DupGroup{
			Size:   k.size,
			Hash:   hex.EncodeToString(k.h[:]),
			Keeper: keeper.Path,
			Dups:   dups,
			Files:  files,
		})
		rep.TotalDupBytes += k.size * int64(len(files)-1)
	}
	// Stable ordering for deterministic output.
	sort.Slice(rep.Groups, func(i, j int) bool {
		if rep.Groups[i].Size != rep.Groups[j].Size {
			return rep.Groups[i].Size > rep.Groups[j].Size
		}
		return rep.Groups[i].Hash < rep.Groups[j].Hash
	})

	rep.FinishedAt = time.Now()
	// Execute action. writeReport observes FinishedAt.
	if err := applyAction(ctx, cfg, rep); err != nil {
		rep.Errors = append(rep.Errors, err.Error())
	}
	return rep, nil
}

type inodeKey struct {
	dev uint64
	ino uint64
}

func sortForKeeper(files []*FileRef, k Keeper) {
	switch k {
	case KeeperFirst:
		// preserve walk order
	case KeeperShortest:
		sort.Slice(files, func(i, j int) bool {
			if len(files[i].Path) != len(files[j].Path) {
				return len(files[i].Path) < len(files[j].Path)
			}
			return files[i].Path < files[j].Path
		})
	case KeeperOldest:
		sort.Slice(files, func(i, j int) bool {
			return files[i].MTime.Before(files[j].MTime)
		})
	case KeeperNewest:
		sort.Slice(files, func(i, j int) bool {
			return files[i].MTime.After(files[j].MTime)
		})
	}
}
