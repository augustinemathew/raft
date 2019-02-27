package com.augustine.raft.wal;

import com.augustine.raft.RaftConfiguration;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class SnapshotLogEntry extends LogEntry{
    private final RaftConfiguration configuration;
    public SnapshotLogEntry(long lsn, long term,
                            @NonNull RaftConfiguration configuration) {
        super(lsn, term, LogEntryType.SNAPSHOT, new byte[0]);
        this.configuration = configuration;
    }
}
