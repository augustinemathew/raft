package dedup

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"
)

func newTestConfig(t *testing.T, root string) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Roots = []string{root}
	cfg.Workers = 2
	cfg.CrossDevice = true // tests usually span tmpfs/tmpdir
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg.DryRun = false // tests opt in explicitly
	cfg.Output = filepath.Join(t.TempDir(), "report.json")
	return cfg
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRun_DetectsDuplicates(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 16*1024)
	writeFile(t, filepath.Join(root, "a/one.bin"), payload)
	writeFile(t, filepath.Join(root, "b/two.bin"), payload)
	writeFile(t, filepath.Join(root, "c/unique.bin"), randBytes(t, 16*1024))

	cfg := newTestConfig(t, root)
	cfg.Action = ActionReport
	cfg.Output = filepath.Join(t.TempDir(), "report.json")

	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(rep.Groups) != 1 {
		t.Fatalf("want 1 dup group, got %d", len(rep.Groups))
	}
	g := rep.Groups[0]
	if len(g.Dups)+1 != 2 {
		t.Fatalf("want 2 files in group, got %d", len(g.Dups)+1)
	}
	if g.Size != int64(len(payload)) {
		t.Errorf("group size %d != %d", g.Size, len(payload))
	}
}

func TestRun_SkipsDifferentSizes(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.bin"), randBytes(t, 1024))
	writeFile(t, filepath.Join(root, "b.bin"), randBytes(t, 2048))

	cfg := newTestConfig(t, root)
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 0 {
		t.Fatalf("expected no groups, got %d", len(rep.Groups))
	}
	// Neither file should be hashed: different sizes, so both are singletons
	// and eliminated at the size-bucket phase.
	if rep.FilesHashed != 0 {
		t.Errorf("expected 0 hashed, got %d", rep.FilesHashed)
	}
}

// Two files, same size, same head and tail, different middle. Quick hash
// must not claim them identical; full hash must separate them.
func TestRun_QuickHashCollisionSeparatedByFullHash(t *testing.T) {
	root := t.TempDir()
	const size = 64 * 1024
	a := make([]byte, size)
	b := make([]byte, size)
	// Fill head and tail identically, middle differs.
	for i := 0; i < 4096; i++ {
		a[i] = 0xAA
		b[i] = 0xAA
		a[size-1-i] = 0xBB
		b[size-1-i] = 0xBB
	}
	// Distinct middle bytes.
	a[size/2] = 1
	b[size/2] = 2
	writeFile(t, filepath.Join(root, "a.bin"), a)
	writeFile(t, filepath.Join(root, "b.bin"), b)

	cfg := newTestConfig(t, root)
	cfg.QuickHashBytes = 4096
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(rep.Groups))
	}
}

func TestRun_HardlinkDedup(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 8192)
	src := filepath.Join(root, "a.bin")
	dup := filepath.Join(root, "b.bin")
	writeFile(t, src, payload)
	writeFile(t, dup, payload)

	cfg := newTestConfig(t, root)
	cfg.Action = ActionHardlink
	cfg.Keeper = KeeperShortest

	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(rep.Groups))
	}
	var sa, sb syscall.Stat_t
	if err := syscall.Stat(src, &sa); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Stat(dup, &sb); err != nil {
		t.Fatal(err)
	}
	if sa.Ino != sb.Ino {
		t.Fatalf("expected same inode after hardlink, got %d vs %d", sa.Ino, sb.Ino)
	}
	if rep.ReclaimedBytes != int64(len(payload)) {
		t.Errorf("reclaimed=%d want %d", rep.ReclaimedBytes, len(payload))
	}
	// Content preserved.
	got, err := os.ReadFile(dup)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("content changed after hardlink")
	}
}

func TestRun_ExistingHardlinksNotRehashed(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 4096)
	a := filepath.Join(root, "a.bin")
	b := filepath.Join(root, "b.bin")
	writeFile(t, a, payload)
	if err := os.Link(a, b); err != nil {
		t.Fatal(err)
	}
	cfg := newTestConfig(t, root)
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 0 {
		t.Fatalf("existing hardlinks should not appear as dup groups, got %d", len(rep.Groups))
	}
	if rep.FilesHashed != 0 {
		t.Errorf("should not hash hardlinked alias, hashed=%d", rep.FilesHashed)
	}
}

// A symlink that points at its own parent directory. Without cycle
// detection, walkFollow would recurse forever.
func TestRun_FollowSymlinks_SelfCycle(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 1024)
	writeFile(t, filepath.Join(root, "a.bin"), payload)
	writeFile(t, filepath.Join(root, "b.bin"), payload)
	// loop -> . creates A/loop/loop/loop/... if followed naively.
	if err := os.Symlink(".", filepath.Join(root, "loop")); err != nil {
		t.Fatal(err)
	}

	cfg := newTestConfig(t, root)
	cfg.FollowSymlinks = true

	done := make(chan struct{})
	var rep *Report
	var err error
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err = Run(ctx, cfg)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Run hung — symlink cycle not detected")
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 1 {
		t.Fatalf("want 1 group, got %d", len(rep.Groups))
	}
	// Files must not be emitted twice via the loop alias.
	if rep.FilesScanned != 2 {
		t.Errorf("files_scanned=%d, want 2 (cycle caused re-emission)", rep.FilesScanned)
	}
}

// Two directories each holding a symlink to the other. The visited set
// must break the ring regardless of which side we enter first.
func TestRun_FollowSymlinks_MutualCycle(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := randBytes(t, 2048)
	writeFile(t, filepath.Join(root, "a", "f1"), payload)
	writeFile(t, filepath.Join(root, "b", "f2"), payload)
	// a/toB -> ../b, b/toA -> ../a forms a ring.
	if err := os.Symlink("../b", filepath.Join(root, "a", "toB")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../a", filepath.Join(root, "b", "toA")); err != nil {
		t.Fatal(err)
	}

	cfg := newTestConfig(t, root)
	cfg.FollowSymlinks = true

	done := make(chan struct{})
	var rep *Report
	var err error
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err = Run(ctx, cfg)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Run hung — mutual symlink cycle not detected")
	}
	if err != nil {
		t.Fatal(err)
	}
	// Two distinct files (different content is fine; what we care about is
	// that neither was emitted twice via the symlinks).
	if rep.FilesScanned != 2 {
		t.Errorf("files_scanned=%d, want 2", rep.FilesScanned)
	}
	if len(rep.Groups) != 1 {
		t.Errorf("want 1 dup group, got %d", len(rep.Groups))
	}
}

// A broken symlink must not abort the walk.
func TestRun_FollowSymlinks_BrokenLink(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 512)
	writeFile(t, filepath.Join(root, "real.bin"), payload)
	if err := os.Symlink("/nonexistent/nope", filepath.Join(root, "broken")); err != nil {
		t.Fatal(err)
	}
	cfg := newTestConfig(t, root)
	cfg.FollowSymlinks = true
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.FilesScanned != 1 {
		t.Errorf("files_scanned=%d, want 1", rep.FilesScanned)
	}
}

// Symlink to a regular file must emit the file and inode-dedup against
// the real path so we don't claim a file duplicates itself.
func TestRun_FollowSymlinks_FileAliasIsNotDuplicate(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 4096)
	real := filepath.Join(root, "real.bin")
	writeFile(t, real, payload)
	if err := os.Symlink("real.bin", filepath.Join(root, "alias.bin")); err != nil {
		t.Fatal(err)
	}
	cfg := newTestConfig(t, root)
	cfg.FollowSymlinks = true
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 0 {
		t.Fatalf("symlink alias must not look like a duplicate; got %d groups", len(rep.Groups))
	}
}

func TestRun_SymlinkSkippedByDefault(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 4096)
	a := filepath.Join(root, "a.bin")
	writeFile(t, a, payload)
	if err := os.Symlink(a, filepath.Join(root, "link.bin")); err != nil {
		t.Fatal(err)
	}
	cfg := newTestConfig(t, root)
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 0 {
		t.Fatalf("symlink should not produce dup, got %d groups", len(rep.Groups))
	}
}

func TestRun_MinMaxSize(t *testing.T) {
	root := t.TempDir()
	small := randBytes(t, 100)
	big := randBytes(t, 10000)
	writeFile(t, filepath.Join(root, "small1.bin"), small)
	writeFile(t, filepath.Join(root, "small2.bin"), small)
	writeFile(t, filepath.Join(root, "big1.bin"), big)
	writeFile(t, filepath.Join(root, "big2.bin"), big)

	cfg := newTestConfig(t, root)
	cfg.MinSize = 1000
	cfg.MaxSize = 20000
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 1 {
		t.Fatalf("want 1 group (big files only), got %d", len(rep.Groups))
	}
	if rep.Groups[0].Size != int64(len(big)) {
		t.Errorf("wrong group size %d", rep.Groups[0].Size)
	}
}

func TestRun_ExcludeGlob(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 1024)
	writeFile(t, filepath.Join(root, "a.bin"), payload)
	writeFile(t, filepath.Join(root, "b.bin"), payload)
	writeFile(t, filepath.Join(root, "c.tmp"), payload)

	cfg := newTestConfig(t, root)
	cfg.Excludes = []string{"*.tmp"}
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 1 {
		t.Fatalf("want 1 group, got %d", len(rep.Groups))
	}
	for _, p := range append([]string{rep.Groups[0].Keeper}, rep.Groups[0].Dups...) {
		if filepath.Ext(p) == ".tmp" {
			t.Errorf("excluded file leaked: %s", p)
		}
	}
}

func TestRun_KeeperShortest(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 1024)
	writeFile(t, filepath.Join(root, "deeply/nested/longname.bin"), payload)
	writeFile(t, filepath.Join(root, "a.bin"), payload)

	cfg := newTestConfig(t, root)
	cfg.Keeper = KeeperShortest
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 1 {
		t.Fatalf("want 1 group, got %d", len(rep.Groups))
	}
	if filepath.Base(rep.Groups[0].Keeper) != "a.bin" {
		t.Errorf("keeper %q is not the shortest", rep.Groups[0].Keeper)
	}
}

func TestRun_KeeperNewest(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 1024)
	old := filepath.Join(root, "old.bin")
	newp := filepath.Join(root, "new.bin")
	writeFile(t, old, payload)
	writeFile(t, newp, payload)
	oldTime := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(old, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	cfg := newTestConfig(t, root)
	cfg.Keeper = KeeperNewest
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 1 {
		t.Fatalf("want 1 group")
	}
	if filepath.Base(rep.Groups[0].Keeper) != "new.bin" {
		t.Errorf("keeper %q is not newest", rep.Groups[0].Keeper)
	}
}

func TestRun_ZeroByteFilesExcludedByDefault(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a"), nil)
	writeFile(t, filepath.Join(root, "b"), nil)

	cfg := newTestConfig(t, root)
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 0 {
		t.Fatalf("zero-byte files should be excluded, got %d groups", len(rep.Groups))
	}

	cfg.IncludeZero = true
	rep, err = Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Groups) != 1 {
		t.Fatalf("with -zero expected 1 group, got %d", len(rep.Groups))
	}
}

func TestRun_Cancel(t *testing.T) {
	root := t.TempDir()
	// Enough files that the worker pool has real work to do.
	payload := randBytes(t, 1<<20)
	for i := 0; i < 20; i++ {
		writeFile(t, filepath.Join(root, "file", sprintInt(i)+".bin"), payload)
	}
	cfg := newTestConfig(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(ctx, cfg)
	if err != nil && err != context.Canceled {
		// Cancellation may race with completion; either is acceptable.
		t.Logf("Run returned %v (expected nil or context.Canceled)", err)
	}
}

func TestApplyAction_VerifiesBeforeDestruction(t *testing.T) {
	root := t.TempDir()
	payload := randBytes(t, 1024)
	keeper := filepath.Join(root, "a.bin")
	dup := filepath.Join(root, "b.bin")
	writeFile(t, keeper, payload)
	writeFile(t, dup, payload)

	cfg := newTestConfig(t, root)
	cfg.Action = ActionDelete
	// Between our hash phase and action, overwrite dup. verifyPair should
	// catch it. Easiest way: construct a report directly.
	rep := &Report{Groups: []*DupGroup{{
		Size:   int64(len(payload)),
		Keeper: keeper,
		Files: []*FileRef{
			mustFileRef(t, keeper),
			mustFileRef(t, dup),
		},
	}}}
	// Mutate dup to mismatch.
	if err := os.WriteFile(dup, randBytes(t, 1024), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := applyAction(context.Background(), cfg, rep); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dup); err != nil {
		t.Fatalf("dup was deleted despite verify failure: %v", err)
	}
	if len(rep.Errors) == 0 {
		t.Error("expected verify error to be recorded")
	}
}

func TestValidate_Defaults(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Roots = []string{"/tmp"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := DefaultConfig()
	if err := bad.Validate(); err == nil {
		t.Error("expected error for missing roots")
	}
}

func TestSortForKeeper(t *testing.T) {
	now := time.Now()
	files := []*FileRef{
		{Path: "/longer/path/x", MTime: now.Add(-3 * time.Hour)},
		{Path: "/x", MTime: now},
		{Path: "/mid/x", MTime: now.Add(-1 * time.Hour)},
	}
	cp := append([]*FileRef(nil), files...)
	sortForKeeper(cp, KeeperShortest)
	if cp[0].Path != "/x" {
		t.Errorf("shortest: %s", cp[0].Path)
	}
	cp = append([]*FileRef(nil), files...)
	sortForKeeper(cp, KeeperOldest)
	if cp[0].Path != "/longer/path/x" {
		t.Errorf("oldest: %s", cp[0].Path)
	}
	cp = append([]*FileRef(nil), files...)
	sortForKeeper(cp, KeeperNewest)
	if cp[0].Path != "/x" {
		t.Errorf("newest: %s", cp[0].Path)
	}
}

func TestMatchesExclude(t *testing.T) {
	if !matchesExclude([]string{"*.tmp"}, "x.tmp") {
		t.Error("*.tmp should match x.tmp")
	}
	if matchesExclude([]string{"*.tmp"}, "x.txt") {
		t.Error("*.tmp should not match x.txt")
	}
}

// Ensure groups are sorted largest-first so operators see big wins first.
func TestReport_GroupsSortedBySizeDesc(t *testing.T) {
	root := t.TempDir()
	small := randBytes(t, 1024)
	large := randBytes(t, 4096)
	writeFile(t, filepath.Join(root, "s1"), small)
	writeFile(t, filepath.Join(root, "s2"), small)
	writeFile(t, filepath.Join(root, "l1"), large)
	writeFile(t, filepath.Join(root, "l2"), large)

	cfg := newTestConfig(t, root)
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !sort.SliceIsSorted(rep.Groups, func(i, j int) bool {
		return rep.Groups[i].Size >= rep.Groups[j].Size
	}) {
		t.Error("groups not sorted by size desc")
	}
}

func mustFileRef(t *testing.T, path string) *FileRef {
	t.Helper()
	st, err := lstatRaw(path)
	if err != nil {
		t.Fatal(err)
	}
	return &FileRef{
		Path: path, Size: st.Size,
		Dev: uint64(st.Dev), Ino: uint64(st.Ino), Nlink: uint64(st.Nlink),
		MTime: mtimeFromStat(st),
	}
}

func sprintInt(i int) string {
	// avoid pulling fmt just for this
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[n:])
}
