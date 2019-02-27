package com.augustine.raft.rpc;

import com.augustine.raft.RaftConfiguration;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class InstallSnapshotRequest extends RequestBase {
    private final long serverId;
    private final long term ;
    private final long requestServerId ;
    private final long leaderId ;
    private final long lastIncludedIndex;
    private final long lastIncludedTerm;
    private final long offset;
    private final byte[] data;
    private final boolean done;
    private final byte[] snaphotCheckum;
    private final RaftConfiguration configuration;
}

