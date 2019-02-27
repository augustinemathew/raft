package com.augustine.raft.snapshot;

import com.augustine.raft.RaftConfiguration;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;

public interface SnapshotManager {
    List<Snapshot> listSnapshots() throws IOException;
    void appendToSnapshot(long lsn, long term, long offset, @NonNull byte[] data) throws IOException;
    Snapshot saveSnapshot(long lsn, long term, RaftConfiguration configuration, @NonNull byte[] sha256) throws IOException;
}
