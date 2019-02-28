package com.augustine.raft;

import com.augustine.raft.rpc.AppendEntriesRequest;
import com.augustine.raft.rpc.AppendEntriesResponse;
import com.augustine.raft.rpc.InstallSnapshotRequest;
import com.augustine.raft.rpc.InstallSnapshotResponse;
import com.augustine.raft.rpc.VoteRequest;
import com.augustine.raft.rpc.VoteResponse;
import lombok.NonNull;

import java.util.concurrent.ConcurrentSkipListSet;

public interface RaftMessageHandler {

    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);

    VoteResponse handleVoteRequest(VoteRequest request);

    InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request);
}

