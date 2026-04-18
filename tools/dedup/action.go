package dedup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// applyAction dispatches on cfg.Action. Destructive actions re-verify each
// duplicate against its keeper immediately before acting, to catch files
// changed between the hash phase and now.
func applyAction(ctx context.Context, cfg Config, rep *Report) error {
	if cfg.DryRun && cfg.Action != ActionReport {
		cfg.Logger.Info("dry-run: skipping destructive action", "action", cfg.Action)
		return writeReport(cfg, rep)
	}
	switch cfg.Action {
	case ActionReport:
		return writeReport(cfg, rep)
	case ActionHardlink:
		return consolidate(ctx, cfg, rep, linkReplace)
	case ActionReflink:
		return consolidate(ctx, cfg, rep, reflinkCopy)
	case ActionDelete:
		return consolidate(ctx, cfg, rep, func(_, dup string) error { return os.Remove(dup) })
	}
	return fmt.Errorf("unreachable action %q", cfg.Action)
}

type replaceFn func(keeper, dup string) error

func consolidate(ctx context.Context, cfg Config, rep *Report, replace replaceFn) error {
	log := cfg.Logger
	for _, g := range rep.Groups {
		keeper := g.Files[0]
		for _, dup := range g.Files[1:] {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := verifyPair(keeper, dup); err != nil {
				rep.Errors = append(rep.Errors, fmt.Sprintf("verify %s vs %s: %v", keeper.Path, dup.Path, err))
				continue
			}
			if cfg.Action == ActionHardlink {
				// Hardlinks require same filesystem (same dev). Not the
				// same as same mount: bind mounts share dev. If different,
				// skip rather than silently fail.
				if dup.Dev != keeper.Dev {
					rep.Errors = append(rep.Errors, fmt.Sprintf("cross-device: skip %s -> %s", dup.Path, keeper.Path))
					continue
				}
				// No-op if already linked to the same inode.
				if dup.Ino == keeper.Ino {
					continue
				}
			}
			if err := replace(keeper.Path, dup.Path); err != nil {
				rep.Errors = append(rep.Errors, fmt.Sprintf("replace %s -> %s: %v", dup.Path, keeper.Path, err))
				continue
			}
			rep.ReclaimedBytes += dup.Size
			log.Info("replaced", "dup", dup.Path, "keeper", keeper.Path, "action", cfg.Action)
		}
	}
	return writeReport(cfg, rep)
}

// verifyPair re-stats and re-hashes both files to confirm they are still
// identical. This window cannot be fully closed without locking, but
// re-verification catches the common case of a file changing between scan
// and action.
func verifyPair(keeper, dup *FileRef) error {
	ks, err := os.Lstat(keeper.Path)
	if err != nil {
		return err
	}
	ds, err := os.Lstat(dup.Path)
	if err != nil {
		return err
	}
	if ks.Size() != ds.Size() || ks.Size() != keeper.Size {
		return fmt.Errorf("size mismatch: keeper=%d dup=%d recorded=%d", ks.Size(), ds.Size(), keeper.Size)
	}
	ksys := ks.Sys().(*syscall.Stat_t)
	if ksys.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return fmt.Errorf("keeper is not a regular file")
	}
	dsys := ds.Sys().(*syscall.Stat_t)
	if dsys.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return fmt.Errorf("dup is not a regular file")
	}
	kh, err := hashWhole(keeper.Path)
	if err != nil {
		return err
	}
	dh, err := hashWhole(dup.Path)
	if err != nil {
		return err
	}
	if kh != dh {
		return fmt.Errorf("hash diverged: keeper=%s dup=%s",
			hex.EncodeToString(kh[:8]), hex.EncodeToString(dh[:8]))
	}
	return nil
}

func hashWhole(path string) ([32]byte, error) {
	f, err := openForRead(path)
	if err != nil {
		return [32]byte{}, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return [32]byte{}, err
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// linkReplace atomically replaces dup with a hardlink to keeper. The
// intermediate name must live in the same directory as dup so rename(2) is
// atomic across the same mount. On error the temp link is removed.
func linkReplace(keeper, dup string) error {
	dir := filepath.Dir(dup)
	tmp, err := os.CreateTemp(dir, ".dedup-link-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	tmp.Close()
	// link(2) requires the target not to exist.
	if err := os.Remove(tmpPath); err != nil {
		return err
	}
	if err := os.Link(keeper, tmpPath); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dup); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return nil
}

func writeReport(cfg Config, rep *Report) error {
	var w io.Writer = os.Stdout
	if cfg.Output != "" {
		f, err := os.Create(cfg.Output)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}
