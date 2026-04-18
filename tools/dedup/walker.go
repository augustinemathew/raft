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
	if cfg.FollowSymlinks {
		return walkFollow(ctx, cfg, rootDevs, out)
	}
	return walkNoFollow(ctx, cfg, rootDevs, out)
}

// walkNoFollow uses filepath.WalkDir. WalkDir does not traverse directory
// symlinks, so cycle detection is unnecessary here.
func walkNoFollow(ctx context.Context, cfg Config, rootDevs map[uint64]struct{}, out chan<- *FileRef) error {
	log := cfg.Logger
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
				if matchesExclude(cfg.Excludes, d.Name()) {
					return fs.SkipDir
				}
				if !cfg.CrossDevice && path != absRoot {
					if st, err := lstatRaw(path); err == nil {
						if _, ok := rootDevs[uint64(st.Dev)]; !ok {
							return fs.SkipDir
						}
					}
				}
				return nil
			}
			typ := d.Type()
			if typ&os.ModeSymlink != 0 {
				return nil // follow=false path: symlinks ignored
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
			return emitIfRegular(ctx, cfg, rootDevs, path, st, out)
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

// walkFollow is a recursive walker used when cfg.FollowSymlinks is true. It
// resolves symlinks (stat not lstat) and descends into symlinked
// directories, keeping a (dev,ino) visited set to detect self-loops, mutual
// loops, and any larger cycle between directories. A directory reached via
// multiple paths is walked once; files beneath it are emitted under the
// path first traversed.
func walkFollow(ctx context.Context, cfg Config, rootDevs map[uint64]struct{}, out chan<- *FileRef) error {
	log := cfg.Logger
	visited := make(map[inodeKey]struct{}, 1024)
	for _, root := range cfg.Roots {
		absRoot, err := filepath.Abs(root)
		if err != nil {
			return err
		}
		if err := walkFollowDir(ctx, cfg, absRoot, visited, rootDevs, out); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			log.Warn("walk terminated", "root", absRoot, "err", err)
		}
	}
	return nil
}

func walkFollowDir(ctx context.Context, cfg Config, dir string, visited map[inodeKey]struct{}, rootDevs map[uint64]struct{}, out chan<- *FileRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	log := cfg.Logger

	if isPseudo(dir) {
		return nil
	}
	if matchesExclude(cfg.Excludes, filepath.Base(dir)) {
		return nil
	}
	// Stat (follow) so a symlinked dir is classified as a dir.
	var dst syscall.Stat_t
	if err := syscall.Stat(dir, &dst); err != nil {
		log.Warn("dir stat failed", "dir", dir, "err", err)
		return nil
	}
	if dst.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return nil
	}
	if !cfg.CrossDevice {
		if _, ok := rootDevs[uint64(dst.Dev)]; !ok {
			return nil
		}
	}
	k := inodeKey{uint64(dst.Dev), uint64(dst.Ino)}
	if _, seen := visited[k]; seen {
		log.Debug("skip: already visited (symlink cycle or alias)", "dir", dir)
		return nil
	}
	visited[k] = struct{}{}

	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Warn("readdir failed", "dir", dir, "err", err)
		return nil
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := e.Name()
		if matchesExclude(cfg.Excludes, name) {
			continue
		}
		path := filepath.Join(dir, name)
		// Follow-stat to classify the final target; broken symlinks become
		// ENOENT here and are silently skipped.
		var st syscall.Stat_t
		if err := syscall.Stat(path, &st); err != nil {
			log.Warn("stat failed", "path", path, "err", err)
			continue
		}
		switch st.Mode & syscall.S_IFMT {
		case syscall.S_IFDIR:
			if err := walkFollowDir(ctx, cfg, path, visited, rootDevs, out); err != nil {
				return err
			}
		case syscall.S_IFREG:
			if err := emitIfRegular(ctx, cfg, rootDevs, path, &st, out); err != nil {
				return err
			}
		default:
			// devices, sockets, fifos: skip
		}
	}
	return nil
}

// emitIfRegular applies size/device filters to a regular-file stat and
// emits a FileRef. st must be the result of stat/lstat on path; mode type
// is assumed to be S_IFREG.
func emitIfRegular(ctx context.Context, cfg Config, rootDevs map[uint64]struct{}, path string, st *syscall.Stat_t, out chan<- *FileRef) error {
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

func isPseudo(p string) bool {
	for _, prefix := range pseudoFS {
		if p == prefix || strings.HasPrefix(p, prefix+"/") {
			return true
		}
	}
	return false
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
func lstatRaw(path string) (*syscall.Stat_t, error) {
	var st syscall.Stat_t
	if err := syscall.Lstat(path, &st); err != nil {
		return nil, err
	}
	return &st, nil
}
