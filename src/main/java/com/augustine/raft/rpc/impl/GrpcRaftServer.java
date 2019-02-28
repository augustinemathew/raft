package com.augustine.raft.rpc.impl;

import com.augustine.raft.RaftMessageHandler;
import com.augustine.raft.proto.ProtoSerializer;
import com.augustine.raft.proto.RaftGrpc;
import com.augustine.raft.proto.RaftRpc;
import com.augustine.raft.rpc.AppendEntriesResponse;
import com.augustine.raft.rpc.InstallSnapshotResponse;
import com.augustine.raft.rpc.VoteResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

@Slf4j
public class GrpcRaftServer extends RaftGrpc.RaftImplBase {

    private final RaftMessageHandler messageHandler;
    private final ProtoSerializer messageSerializer;
    private final Server server ;

    public GrpcRaftServer(int port,
                          @NonNull RaftMessageHandler server,
                          @NonNull ProtoSerializer messageSerializer) {
        this.messageHandler = server;
        this.server = ServerBuilder.forPort(port)
                .executor(Executors.newFixedThreadPool(10))
                .addService(this)
                .build();
        this.messageSerializer = messageSerializer;
    }

    public void start() throws IOException{
        this.server.start();
    }

    public void stop(long timeToWait, TimeUnit unit) throws InterruptedException{
        this.server.shutdown();
        this.server.awaitTermination(timeToWait, unit);
    }


    @Override
    public void appendEntries(RaftRpc.AppendEntriesRequest request,
                              StreamObserver<RaftRpc.AppendEntriesResponse> responseObserver) {
        this.log.debug("Recvd Append entries request from server: {}", request.getRequestServerId());
        AppendEntriesResponse response = messageHandler.handleAppendEntries(this.messageSerializer.fromProtobuf(request));
        responseObserver.onNext(this.messageSerializer.toProtobuf(response));
        //responseObserver.onNext(RaftRpc.AppendEntriesResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void vote(RaftRpc.VoteRequest request,
                     StreamObserver<RaftRpc.VoteResponse> responseObserver){
        this.log.debug("Recvd vote request from server: {}", request.getRequestServerId());
        try {
            VoteResponse response = messageHandler.handleVoteRequest(this.messageSerializer.fromProtobuf(request));
            responseObserver.onNext(this.messageSerializer.toProtobuf(response));
            responseObserver.onCompleted();
        }catch (Exception e){
            log.error("Exception in handling vote request", e);
        }
    }

    @Override
    public void installSnapshot(RaftRpc.InstallSnapshotRequest request,
                                StreamObserver<RaftRpc.InstallSnapshotResponse> responseObserver) {
        InstallSnapshotResponse response = messageHandler.handleInstallSnapshot(
                this.messageSerializer.fromProtobuf(request));
        responseObserver.onNext(this.messageSerializer.toProtobuf(response));
        responseObserver.onCompleted();
    }
}
