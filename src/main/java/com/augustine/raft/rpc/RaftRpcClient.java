package com.augustine.raft.rpc;

import com.augustine.raft.RaftMessageHandler;
import com.augustine.raft.proto.RaftGrpc;
import com.augustine.raft.proto.RaftRpc;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;

public interface RaftRpcClient extends AutoCloseable{

    CompletableFuture<AppendEntriesResponse> AppendEntries(AppendEntriesRequest request);

    CompletableFuture<VoteResponse> RequestToVote(VoteRequest request);

    CompletableFuture<InstallSnapshotResponse> InstallSnapshot(InstallSnapshotRequest request);

    void close();
}



