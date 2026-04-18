package dedup

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// pseudoFS holds absolute path prefixes we never descend into. These are
// kernel-exported filesystems with files that either can't be deduped
// meaningfully or are dangerous to touch.
var pseudoFS = []string{
	"/proc", "/sys", "/dev", "/run", "/var/run", "/var/lock",
}

// walk enumerates regular files under cfg.Roots and emits FileRef on out.
// Errors for individual entries are logged and skipped so a single
// unreadable directory does not abort the whole scan.
func walk(ctx context.Context, cfg Config, out chan<- *FileRef) error {
	log := cfg.Logger

	rootDevs := make(map[uint64]struct{}, len(cfg.Roots))
	if !cfg.CrossDevice {
		for _, r := range cfg.Roots {
			var st syscall.Stat_t
			if err := syscall.Stat(r, &st); err != nil {
				log.Warn("root stat failed", "root", r, "err", err)
				continue
			}
			rootDevs[uint64(st.Dev)] = struct{}{}
		}
	}

	for _, root := range cfg.Roots {
		absRoot, err := filepath.Abs(root)
		if err != nil {
			return err
		}
		walkFn := func(path string, d fs.DirEntry, walkErr error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if walkErr != nil {
				// fs.WalkDir invokes fn with the error for unreadable
				// entries. Skip the subtree and continue.
				log.Warn("walk error", "path", path, "err", walkErr)
				if d != nil && d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			if d.IsDir() {
				if isPseudo(path) {
					return fs.SkipDir
				}
				if shouldSkipDir(cfg, path, d) {
					return fs.SkipDir
				}
				if !cfg.CrossDevice && path != absRoot {
					st, err := lstatRaw(path)
					if err == nil {
						if _, ok := rootDevs[uint64(st.Dev)]; !ok {
							return fs.SkipDir
						}
					}
				}
				return nil
			}

			// fs.DirEntry.Type is cheap (from readdir); only stat when we
			// keep the file.
			typ := d.Type()
			if typ&os.ModeSymlink != 0 && !cfg.FollowSymlinks {
				return nil
			}
			if typ&(os.ModeDevice|os.ModeCharDevice|os.ModeSocket|os.ModeNamedPipe|os.ModeIrregular) != 0 {
				return nil
			}

			if matchesExclude(cfg.Excludes, filepath.Base(path)) {
				return nil
			}

			st, err := lstatRaw(path)
			if err != nil {
				log.Warn("stat failed", "path", path, "err", err)
				return nil
			}
			// Only regular files.
			if st.Mode&syscall.S_IFMT != syscall.S_IFREG {
				return nil
			}
			if !cfg.CrossDevice {
				if _, ok := rootDevs[uint64(st.Dev)]; !ok {
					return nil
				}
			}
			size := int64(st.Size)
			if size == 0 {
				if !cfg.IncludeZero {
					return nil
				}
			} else if size < cfg.MinSize {
				return nil
			}
			if cfg.MaxSize > 0 && size > cfg.MaxSize {
				return nil
			}

			fr := &FileRef{
				Path:  path,
				Size:  size,
				Dev:   uint64(st.Dev),
				Ino:   uint64(st.Ino),
				Nlink: uint64(st.Nlink),
				Mode:  os.FileMode(st.Mode & 0o7777),
				MTime: mtimeFromStat(st),
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- fr:
			}
			return nil
		}
		if err := filepath.WalkDir(absRoot, walkFn); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			log.Warn("walk terminated", "root", absRoot, "err", err)
		}
	}
	return nil
}

func isPseudo(p string) bool {
	for _, prefix := range pseudoFS {
		if p == prefix || strings.HasPrefix(p, prefix+"/") {
			return true
		}
	}
	return false
}

func shouldSkipDir(cfg Config, path string, d fs.DirEntry) bool {
	return matchesExclude(cfg.Excludes, d.Name())
}

func matchesExclude(patterns []string, name string) bool {
	for _, p := range patterns {
		if ok, _ := filepath.Match(p, name); ok {
			return true
		}
	}
	return false
}

// lstatRaw returns the syscall.Stat_t for path without following symlinks.
// We prefer this over os.Lstat so we can access dev/ino directly without a
// platform-specific type assertion on every call.
func lstatRaw(path string) (*syscall.Stat_t, error) {
	var st syscall.Stat_t
	if err := syscall.Lstat(path, &st); err != nil {
		return nil, err
	}
	return &st, nil
}
