package com.augustine.raft.rpc.impl;

import com.augustine.raft.ListenableFutureAdapter;
import com.augustine.raft.proto.ProtoSerializer;
import com.augustine.raft.proto.RaftGrpc;
import com.augustine.raft.proto.RaftRpc;
import com.augustine.raft.rpc.AppendEntriesRequest;
import com.augustine.raft.rpc.AppendEntriesResponse;
import com.augustine.raft.rpc.InstallSnapshotRequest;
import com.augustine.raft.rpc.InstallSnapshotResponse;
import com.augustine.raft.rpc.RaftRpcClient;
import com.augustine.raft.rpc.VoteRequest;
import com.augustine.raft.rpc.VoteResponse;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GrpcRaftClient implements RaftRpcClient {
    private final ManagedChannel channel;
    private final ProtoSerializer messageSerializer;
    private RaftGrpc.RaftStub stub;
    private final long requestTimeoutInMs;

    public GrpcRaftClient(String hostname, int port, ProtoSerializer messageSerializer, long requestTimeoutInMs) {
        this.requestTimeoutInMs = requestTimeoutInMs;
        this.channel = ManagedChannelBuilder
                        .forAddress(hostname, port)
                        .usePlaintext(true)
                        .executor(Executors.newFixedThreadPool(10))
                        .build();
        this.stub = RaftGrpc.newStub(channel);
        this.messageSerializer = messageSerializer;
    }


    @Override
    public CompletableFuture<VoteResponse> RequestToVote(VoteRequest request) {
        ListenableFuture<RaftRpc.VoteResponse> responseFuture = this.getStub()
                .vote(this.messageSerializer.toProtobuf(request));
        return ListenableFutureAdapter.toCompletable(responseFuture)
                .thenApply((r) ->
                        this.messageSerializer.fromProtobuf(r));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> InstallSnapshot(InstallSnapshotRequest request) {
        ListenableFuture<RaftRpc.InstallSnapshotResponse> responseFuture = this.getStub()
                .installSnapshot(this.messageSerializer.toProtobuf(request));
        return ListenableFutureAdapter.toCompletable(responseFuture)
                .thenApply((r) ->
                        this.messageSerializer.fromProtobuf(r));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> AppendEntries(AppendEntriesRequest request) {

        ListenableFuture<RaftRpc.AppendEntriesResponse> responseFuture = this.getStub()
                .appendEntries(this.messageSerializer.toProtobuf(request));
        return ListenableFutureAdapter.toCompletable(responseFuture)
                .thenApply((r) ->
                        this.messageSerializer.fromProtobuf(r));
    }

    private RaftGrpc.RaftFutureStub getStub(){
       return RaftGrpc.newFutureStub(channel);
    }

    @Override
    public void close() {
        this.channel.shutdown();
        try {
            if(!this.channel.awaitTermination(requestTimeoutInMs * 2, TimeUnit.MILLISECONDS)){
                this.channel.shutdownNow();
            }
        }catch (InterruptedException ie){
            this.channel.shutdownNow();
        }
    }
}
