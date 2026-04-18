package dedup

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
)

type hashKind int

const (
	hashQuick hashKind = iota
	hashFull
)

type hashJob struct {
	file *FileRef
	kind hashKind
}

type hashResult struct {
	file      *FileRef
	sum       [32]byte
	bytesRead int64
	err       error
}

// hashWorker consumes jobs and writes results. It is safe to run N workers
// concurrently; each uses its own buffer and file handles.
func hashWorker(ctx context.Context, cfg Config, in <-chan hashJob, out chan<- hashResult) {
	buf := make([]byte, cfg.ReadBufferBytes)
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-in:
			if !ok {
				return
			}
			r := hashResult{file: j.file}
			switch j.kind {
			case hashQuick:
				r.sum, r.bytesRead, r.err = quickHash(j.file, cfg.QuickHashBytes, buf)
			case hashFull:
				r.sum, r.bytesRead, r.err = fullHash(ctx, j.file, buf)
			}
			select {
			case <-ctx.Done():
				return
			case out <- r:
			}
		}
	}
}

// quickHash mixes file size with the first and last `nBytes` of content.
// Including size guards against short-read truncation aliasing and is free.
// For files smaller than 2*nBytes the caller should route to fullHash.
func quickHash(fr *FileRef, nBytes int64, buf []byte) ([32]byte, int64, error) {
	f, err := openForRead(fr.Path)
	if err != nil {
		return [32]byte{}, 0, err
	}
	defer f.Close()

	// Verify size has not changed since walk; if it has, the file is
	// unstable and should be excluded.
	st, err := f.Stat()
	if err != nil {
		return [32]byte{}, 0, err
	}
	if st.Size() != fr.Size {
		return [32]byte{}, 0, fmt.Errorf("size changed: was %d now %d", fr.Size, st.Size())
	}

	h := sha256.New()
	var sizeBuf [8]byte
	for i := 0; i < 8; i++ {
		sizeBuf[i] = byte(fr.Size >> (8 * i))
	}
	h.Write(sizeBuf[:])

	head := buf
	if int64(len(head)) > nBytes {
		head = head[:nBytes]
	}
	n, err := io.ReadFull(f, head)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return [32]byte{}, int64(n), err
	}
	h.Write(head[:n])
	bytesRead := int64(n)

	// Tail from the absolute offset, size-nBytes. Only if there is a
	// non-overlapping tail.
	if fr.Size > 2*nBytes {
		tailStart := fr.Size - nBytes
		if _, err := f.Seek(tailStart, io.SeekStart); err != nil {
			return [32]byte{}, bytesRead, err
		}
		tail := buf
		if int64(len(tail)) > nBytes {
			tail = tail[:nBytes]
		}
		m, err := io.ReadFull(f, tail)
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return [32]byte{}, bytesRead + int64(m), err
		}
		h.Write(tail[:m])
		bytesRead += int64(m)
	}

	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, bytesRead, nil
}

// fullHash streams the whole file through SHA-256. Hints the kernel to
// prefetch sequentially and to drop pages from cache afterwards so very
// large files do not evict useful data.
func fullHash(ctx context.Context, fr *FileRef, buf []byte) ([32]byte, int64, error) {
	f, err := openForRead(fr.Path)
	if err != nil {
		return [32]byte{}, 0, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return [32]byte{}, 0, err
	}
	if st.Size() != fr.Size {
		return [32]byte{}, 0, fmt.Errorf("size changed: was %d now %d", fr.Size, st.Size())
	}

	fd := int(f.Fd())
	fadviseSequential(fd, 0, fr.Size)
	defer fadviseDontNeed(fd, 0, fr.Size)

	h := sha256.New()
	n, err := copyWithCancel(ctx, h, f, buf)
	if err != nil {
		return [32]byte{}, n, err
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, n, nil
}

// copyWithCancel is io.CopyBuffer with a ctx check between reads. Avoids
// spending minutes hashing a multi-GB file after the caller gave up.
func copyWithCancel(ctx context.Context, dst hash.Hash, src io.Reader, buf []byte) (int64, error) {
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			dst.Write(buf[:nr])
			total += int64(nr)
		}
		if er != nil {
			if errors.Is(er, io.EOF) {
				return total, nil
			}
			return total, er
		}
	}
}

// openForRead opens with O_NOATIME when permitted, falling back to a plain
// read-only open. O_NOATIME avoids bumping atime on every file we touch.
func openForRead(path string) (*os.File, error) {
	f, err := os.OpenFile(path, noAtimeFlag|os.O_RDONLY, 0)
	if err == nil {
		return f, nil
	}
	// EPERM: O_NOATIME requires ownership or CAP_FOWNER.
	return os.OpenFile(path, os.O_RDONLY, 0)
}
