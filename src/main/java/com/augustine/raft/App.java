package com.augustine.raft;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.proto.RaftGrpc;
import com.augustine.raft.rpc.AppendEntriesRequest;
import com.augustine.raft.rpc.AppendEntriesResponse;
import com.augustine.raft.rpc.InstallSnapshotRequest;
import com.augustine.raft.rpc.InstallSnapshotResponse;
import com.augustine.raft.rpc.VoteRequest;
import com.augustine.raft.rpc.VoteResponse;
import com.augustine.raft.rpc.impl.GrpcRaftClient;
import com.augustine.raft.rpc.impl.GrpcRaftServer;
import com.augustine.raft.snapshot.Snapshot;
import com.augustine.raft.wal.LogEntry;
import com.augustine.raft.wal.LogEntryType;
import com.augustine.raft.wal.PersistentLog;
import com.google.common.util.concurrent.Uninterruptibles;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hello world!
 *
 */
public class App 
{
    private static StateMachine getNoop(){
        return new StateMachine() {
            @Override
            public void apply(long lsn, byte[] input) {

            }

            @Override
            public void installSnapshot(Snapshot snapshot) {

            }

            @Override
            public Snapshot getSnapShot() {
                return null;
            }
        };
    }
    public static void main( String[] args )
            throws IOException, InterruptedException, ExecutionException{
        Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
       verify();
    }

    private static void foo() throws IOException, InterruptedException {
        RaftServer server1 = new RaftServer(ServerConfiguration.fromDirectory("/tmp/raft/server1"), getNoop());

        server1.start();
        RaftServer server2 = new RaftServer(ServerConfiguration.fromDirectory("/tmp/raft/server2"), getNoop());

        server2.start();

        RaftServer server3 = new RaftServer(ServerConfiguration.fromDirectory("/tmp/raft/server3"), getNoop());

        server3.start();
        //Uninterruptibles.sleepUninterruptibly(100000, TimeUnit.MILLISECONDS);

        while (true)
        {
            try {
                ArrayList<byte[]> entries = new ArrayList<>();
                Random r = new Random();
                for(int i=0; i<1000; i++) {
                    byte[] arr = new byte[10];
                    r.nextBytes(arr);
                    entries.add(arr);
                }

                if (server1.getServerRole() == ServerRole.Leader) {
                    server1.propose(entries);
                } else if (server2.getServerRole() == ServerRole.Leader) {
                    server2.propose(entries);
                } else if (server3.getServerRole() == ServerRole.Leader) {
                    server3.propose(entries);
                } else {

                }
            }catch (Exception e){
                System.out.println(e);
            }
            Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
        }
    }

    public static void verify(){

        PersistentLog[] logs = new PersistentLog[] {
            new PersistentLog("/tmp/raft/server1/wal"),
            new PersistentLog("/tmp/raft/server2/wal"),
            new PersistentLog("/tmp/raft/server3/wal"),
        };

        for(PersistentLog log : logs) {
            System.out.println(log.getLastEntryIndex());
        }
        int currentIndex = 0;
        int[] verifiedTillIndex = new int[logs.length];
        while (true){
            LogEntry entry = null;
            for(int i=0; i<logs.length; i++){
                LogEntry currentEntry = null;
                if(currentIndex <= logs[i].getLastEntryIndex()){
                    currentEntry = logs[i].getLogEntry(currentIndex);
                }
                if(entry == null && currentEntry != null) {
                    entry = currentEntry;
                }else if(currentEntry != null) {
                    if(!currentEntry.toByteBuffer().equals(entry.toByteBuffer())) {
                        throw new RuntimeException("Faulty log at "  + currentIndex);
                    }
                }
                if(currentEntry != null){
                    verifiedTillIndex[i] = currentIndex;
                }
            }
            if(entry == null) {

                for(int i=0; i<logs.length; i++){
                    if(currentIndex < logs[i].getLastEntryIndex()){
                        throw new RuntimeException("Faulty log at "  + currentIndex);
                    }
                }

                break;
            }
            currentIndex++;
        }

        System.out.println("Verified logs till index " + verifiedTillIndex[0]
                               + " " + verifiedTillIndex[1]
                               + " " + verifiedTillIndex[2]);
    }

    public static void testServerClient() throws IOException, InterruptedException, ExecutionException{
        GrpcRaftServer server = new GrpcRaftServer(8000,
                new RaftMessageHandler() {

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
                }, new ProtoSerializerImpl());
        server.start();
        GrpcRaftClient client = new GrpcRaftClient(
                "localhost",8000, new ProtoSerializerImpl(), 100);
        final AppendEntriesRequest build = AppendEntriesRequest.builder()
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



        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 8000)
                .usePlaintext(true)
                .executor(Executors.newFixedThreadPool(2))
                .build();
        RaftGrpc.RaftStub stub = RaftGrpc.newStub(channel);


        final int warmuprequestCount = 100000;
        CountDownLatch warmup = new CountDownLatch(warmuprequestCount);
        final int realReqCount = 400000;
        CountDownLatch latch = new CountDownLatch(realReqCount);
        final AtomicLong l = new AtomicLong(0);


//        StreamObserver<RaftRpc.AppendEntriesRequest> req = stub.appendEntries(new StreamObserver<RaftRpc.AppendEntriesResponse>() {
//            @Override
//            public void onNext(RaftRpc.AppendEntriesResponse appendEntriesResponse) {
//                if(l.incrementAndGet()>=warmuprequestCount){
//                    //System.out.println("l " + l.get());
//                    latch.countDown();
//                }
//                warmup.countDown();
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//
//            }
//
//            @Override
//            public void onCompleted() {
//
//            }
//        });
        Executor e  =Executors.newFixedThreadPool(10);
        ProtoSerializerImpl impl = new ProtoSerializerImpl();
        for(int i=0; i<warmuprequestCount; i++){
            //req.onNext(impl.toProtobuf(build));
        }
        Thread.sleep(2000);
        warmup.await();
        long t = System.nanoTime();
        System.out.println("Starting now!!");
        for(int i=0; i<realReqCount;i++) {
            //req.onNext(impl.toProtobuf(build));
        }
        latch.await();
        long end = System.nanoTime();
        System.out.println((end - t) /1000000000.0);
    }
}
