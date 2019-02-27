package com.augustine.raft;


import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.proto.RaftGrpc;
import com.augustine.raft.proto.RaftRpc;
import com.augustine.raft.rpc.AppendEntriesRequest;
import com.augustine.raft.rpc.AppendEntriesResponse;
import com.augustine.raft.rpc.InstallSnapshotRequest;
import com.augustine.raft.rpc.InstallSnapshotResponse;
import com.augustine.raft.rpc.VoteRequest;
import com.augustine.raft.rpc.VoteResponse;
import com.augustine.raft.rpc.impl.GrpcRaftClient;
import com.augustine.raft.rpc.impl.GrpcRaftServer;
import com.augustine.raft.wal.LogEntry;
import com.augustine.raft.wal.LogEntryType;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.util.TimeUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RpcTest extends TestCase {
    public RpcTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( RpcTest.class );
    }

    public void testServerClient() throws IOException, InterruptedException, ExecutionException{
        GrpcRaftServer server = new GrpcRaftServer(8000,
                getDummyServerHandler(), new ProtoSerializerImpl());
        server.start();
        GrpcRaftClient client = new GrpcRaftClient(
                "localhost",8000, new ProtoSerializerImpl(), 100);
        final AppendEntriesRequest request = AppendEntriesRequest.builder()
                .entries(new LogEntry[]{
                        LogEntry.builder()
                                .type(LogEntryType.SNAPSHOT)
                                .array(new byte[10])
                                .term(10)
                                .build()})
                .leaderCommit(3)
                .leaderId(3)
                .prevLogIndex(10)
                .prevLogTerm(100)
                .serverId(10)
                .build();


        client.AppendEntries(request).get();
    }

    private RaftMessageHandler getDummyServerHandler() {
        return new RaftMessageHandler() {

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        return AppendEntriesResponse
                .builder()
                .lastLogIndexOnServer(10)
                .succeeded(true)
                .term(3)
                .build();
    }

    @Override
    public VoteResponse handleVoteRequest(VoteRequest request) {
        return VoteResponse.builder()
                .voteGranted(true)
                .term(3)
                .build();
    }

            @Override
            public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
                return InstallSnapshotResponse.builder().ok(true)
                        .term(request.getTerm())
                        .build();
            }
        };
    }
}
