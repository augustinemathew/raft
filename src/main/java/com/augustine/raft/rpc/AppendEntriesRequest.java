package com.augustine.raft.rpc;

import com.augustine.raft.proto.RaftRpc;
import com.augustine.raft.wal.LogEntry;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class AppendEntriesRequest extends RequestBase{
    private final long term;
    private final long leaderId;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final long leaderCommit;
    private final LogEntry[] entries;
    private final long serverId;
}


