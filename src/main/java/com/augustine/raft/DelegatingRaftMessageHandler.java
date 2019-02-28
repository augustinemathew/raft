package com.augustine.raft;

import com.augustine.raft.rpc.*;

public class DelegatingRaftMessageHandler implements RaftMessageHandler {
    private final RaftMessageHandler handler;

    DelegatingRaftMessageHandler(RaftMessageHandler handler) {
        this.handler = handler;
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        return handler.handleAppendEntries(request);
    }

    @Override
    public VoteResponse handleVoteRequest(VoteRequest request) {
        return handler.handleVoteRequest(request);
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        return handler.handleInstallSnapshot(request);
    }
}
