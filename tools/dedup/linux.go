//go:build linux

package dedup

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const noAtimeFlag = syscall.O_NOATIME

func fadviseSequential(fd int, off, length int64) {
	_ = unix.Fadvise(fd, off, length, unix.FADV_SEQUENTIAL)
}

func fadviseDontNeed(fd int, off, length int64) {
	_ = unix.Fadvise(fd, off, length, unix.FADV_DONTNEED)
}

func mtimeFromStat(st *syscall.Stat_t) time.Time {
	return time.Unix(st.Mtim.Sec, st.Mtim.Nsec)
}

// reflinkCopy replaces dst with a copy-on-write clone of src using the
// FICLONE ioctl. Requires both to live on the same filesystem and that
// filesystem to support reflinks (btrfs, xfs with reflink=1, bcachefs).
// The replacement is atomic from the perspective of other readers: we clone
// into a sibling temp file, then rename over dst.
func reflinkCopy(src, dst string) error {
	dstDir, _ := splitDir(dst)
	tmp, err := os.CreateTemp(dstDir, ".dedup-reflink-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	// On any error path we must not leave tmp behind.
	success := false
	defer func() {
		tmp.Close()
		if !success {
			os.Remove(tmpPath)
		}
	}()

	sf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sf.Close()

	if err := unix.IoctlFileClone(int(tmp.Fd()), int(sf.Fd())); err != nil {
		return fmt.Errorf("FICLONE: %w", err)
	}
	// Match mode and ownership of the source so the replacement is
	// transparent to readers relying on those bits.
	sst, err := sf.Stat()
	if err != nil {
		return err
	}
	raw := sst.Sys().(*syscall.Stat_t)
	if err := os.Chmod(tmpPath, sst.Mode().Perm()); err != nil {
		return err
	}
	// Best-effort chown; ignore EPERM (non-root can't chown to arbitrary uids).
	_ = os.Chown(tmpPath, int(raw.Uid), int(raw.Gid))

	if err := os.Rename(tmpPath, dst); err != nil {
		return err
	}
	success = true
	return nil
}

func splitDir(p string) (string, string) {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i], p[i+1:]
		}
	}
	return ".", p
}
