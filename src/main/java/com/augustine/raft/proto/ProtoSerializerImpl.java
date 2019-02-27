package com.augustine.raft.proto;

import com.augustine.raft.RaftConfiguration;
import com.augustine.raft.RaftPeer;
import com.augustine.raft.RaftState;
import com.augustine.raft.rpc.AppendEntriesRequest;
import com.augustine.raft.rpc.AppendEntriesResponse;
import com.augustine.raft.rpc.InstallSnapshotRequest;
import com.augustine.raft.rpc.InstallSnapshotResponse;
import com.augustine.raft.rpc.VoteRequest;
import com.augustine.raft.rpc.VoteResponse;
import com.augustine.raft.wal.LogEntry;
import com.augustine.raft.wal.LogEntryType;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoSerializerImpl implements ProtoSerializer {

    public final static ProtoSerializer INSTANCE = new ProtoSerializerImpl();

    private final static Map<Class<?>, PerClassProtoSerializer> serializers = new HashMap<>();
    private final static Map<Class<? extends Message>, PerClassProtoSerializer> deserializers = new HashMap<>();

    static{
        serializers.put(AppendEntriesRequest.class,new AppendEntriesSerializer());
        serializers.put(AppendEntriesResponse.class,new AppendEntriesResponseSerializer());
        serializers.put(VoteRequest.class, new VoteRequestSerializer());
        serializers.put(VoteResponse.class, new VoteResponseSerializer());
        serializers.put(LogEntry.class, new LogEntrySerializer());
        serializers.put(RaftConfiguration.class, new RaftConfigurationSerializer());
        serializers.put(RaftPeer.class, new RaftPeerSerializer());
        serializers.put(RaftState.class, new RaftStateSerializer());
        serializers.put(InstallSnapshotRequest.class, new InstallSnapshotRequestSerializer());
        serializers.put(AppendEntriesResponse.ConflictDetails.class, new AppendEntriesConflictSerializer());

        deserializers.put(RaftRpc.AppendEntriesRequest.class,new AppendEntriesSerializer());
        deserializers.put(RaftRpc.AppendEntriesResponse.class,new AppendEntriesResponseSerializer());
        deserializers.put(RaftRpc.VoteRequest.class, new VoteRequestSerializer());
        deserializers.put(RaftRpc.VoteResponse.class, new VoteResponseSerializer());
        deserializers.put(RaftRpc.LogEntry.class, new LogEntrySerializer());
        deserializers.put(RaftRpc.RaftConfiguration.class, new RaftConfigurationSerializer());
        deserializers.put(RaftRpc.RaftConfiguration.RaftPeer.class, new RaftPeerSerializer());
        deserializers.put(RaftRpc.PersistentState.class, new RaftStateSerializer());
        deserializers.put(RaftRpc.InstallSnapshotRequest.class, new InstallSnapshotRequestSerializer());
        deserializers.put(RaftRpc.AppendEntriesConflictInfo.class, new AppendEntriesConflictSerializer());
    }

    @Override
    public <T, U extends Message> U toProtobuf(@NonNull T request) {
        PerClassProtoSerializer serializer = serializers.get(request.getClass());
        if(serializer == null){
            throw new IllegalArgumentException("Not supported");
        }
        return (U)serializer.toProtobuf(request);
    }

    @Override
    public ByteBuffer writeToByteBuffer(@NonNull Object object, @NonNull ByteBuffer byteBuffer) {
        return byteBuffer.put(toProtobuf(object).toByteArray());
    }

    @Override
    public <T, U extends Message> T fromProtobuf(@NonNull U request) {
        PerClassProtoSerializer serializer = deserializers.get(request.getClass());
        if(serializer == null){
            throw new IllegalArgumentException("Not supported");
        }
        return (T)serializer.fromProtobuf(request);
    }

    private static class AppendEntriesSerializer implements PerClassProtoSerializer<AppendEntriesRequest, RaftRpc.AppendEntriesRequest> {

        public  RaftRpc.AppendEntriesRequest toProtobuf(AppendEntriesRequest request) {
            LogEntrySerializer serializer = new LogEntrySerializer();
            return RaftRpc.AppendEntriesRequest.newBuilder()
                    .setTerm(request.getTerm())
                    .addAllEntries(Arrays.stream(request.getEntries())
                            .map(l -> serializer.toProtobuf(l)).collect(Collectors.toList()))
                    .setLeaderId(request.getLeaderId())
                    .setPrevLogIndex(request.getPrevLogIndex())
                    .setPrevLogTerm(request.getPrevLogTerm())
                    .setRequestServerId(request.getServerId())
                    .build();
        }

        public  AppendEntriesRequest fromProtobuf(RaftRpc.AppendEntriesRequest request) {
            LogEntrySerializer serializer = new LogEntrySerializer();
            AppendEntriesRequest req = AppendEntriesRequest.builder()
                    .term(request.getTerm())
                    .leaderId(request.getLeaderId())
                    .entries(request.getEntriesList().stream()
                            .map(l->serializer.fromProtobuf(l))
                            .toArray(i -> new LogEntry[i]))
                    .prevLogIndex(request.getPrevLogIndex())
                    .prevLogTerm(request.getPrevLogTerm())
                    .serverId(request.getRequestServerId())
                    .build();
            return req;
        }
    }

    private static class LogEntrySerializer implements PerClassProtoSerializer<LogEntry, RaftRpc.LogEntry>{

        private RaftRpc.LogEntry.Type typeToPbuf(LogEntryType entry) {
            try {
                return RaftRpc.LogEntry.Type.forNumber(entry.ordinal());
            }catch (Exception e){
                throw e;
            }
        }

        private LogEntryType typeFromPbuf(RaftRpc.LogEntry.Type protobufMesage) {
            return Arrays.stream(LogEntryType.values()).filter(f -> f.ordinal() == protobufMesage.getNumber())
                    .findFirst()
                    .get();
        }

        public LogEntry fromProtobuf(RaftRpc.LogEntry logEntry){
            return LogEntry.builder()
                    .array(logEntry.getMessage().toByteArray())
                    .lsn(logEntry.getLsn())
                    .type(typeFromPbuf(logEntry.getType()))
                    .term(logEntry.getTerm())
                    .build();
        }

        public RaftRpc.LogEntry toProtobuf(LogEntry entry){
            return RaftRpc.LogEntry.newBuilder()
                    .setTerm(entry.getTerm())
                    .setType(typeToPbuf(entry.getType()))
                    .setLsn(entry.getLsn())
                    .setMessage(ByteString.copyFrom(entry.getArray()))
                    .build();
        }
    }

    private static class VoteRequestSerializer implements PerClassProtoSerializer<VoteRequest,RaftRpc.VoteRequest>{

        public VoteRequest fromProtobuf(RaftRpc.VoteRequest request){
            return VoteRequest.builder()
                    .term(request.getTerm())
                    .lastLogTerm(request.getLastLogTerm())
                    .lastLogIndex(request.getLastLogIndex())
                    .candidateId(request.getCandidateId())
                    .serverId(request.getRequestServerId())
                    .build();
        }

        public RaftRpc.VoteRequest toProtobuf(VoteRequest request){
            return RaftRpc.VoteRequest.newBuilder()
                    .setTerm(request.getTerm())
                    .setLastLogIndex(request.getLastLogIndex())
                    .setLastLogTerm(request.getLastLogTerm())
                    .setCandidateId(request.getCandidateId())
                    .setRequestServerId(request.getServerId())
                    .build();
        }
    }

    private static class VoteResponseSerializer implements PerClassProtoSerializer<VoteResponse,RaftRpc.VoteResponse> {

        public VoteResponse fromProtobuf(RaftRpc.VoteResponse response){
            return VoteResponse.builder()
                    .term(response.getTerm())
                    .voteGranted(response.getVoteGranted())
                    .build();
        }

        public RaftRpc.VoteResponse toProtobuf(VoteResponse response){
            return RaftRpc.VoteResponse.newBuilder()
                    .setTerm(response.getTerm())
                    .setVoteGranted(response.isVoteGranted())
                    .build();
        }
    }

    private static class AppendEntriesConflictSerializer implements
        PerClassProtoSerializer<AppendEntriesResponse.ConflictDetails,RaftRpc.AppendEntriesConflictInfo> {

        @Override
        public RaftRpc.AppendEntriesConflictInfo toProtobuf(AppendEntriesResponse.ConflictDetails object) {
            return RaftRpc.AppendEntriesConflictInfo.newBuilder()
                .setConflictingTerm(object.getConflictingTerm())
                .setFirstIndexOfConflictingTerm(object.getLogIndexOfFirstEntryInConflictingTerm())
                .build();
        }

        @Override
        public AppendEntriesResponse.ConflictDetails fromProtobuf(RaftRpc.AppendEntriesConflictInfo protobufMesage) {
            return AppendEntriesResponse.ConflictDetails.builder()
                .conflictingTerm(protobufMesage.getConflictingTerm())
                .logIndexOfFirstEntryInConflictingTerm(protobufMesage.getFirstIndexOfConflictingTerm())
                .build();
        }
    }


    private static class AppendEntriesResponseSerializer implements PerClassProtoSerializer<AppendEntriesResponse,RaftRpc.AppendEntriesResponse>{
        static AppendEntriesConflictSerializer appendEntriesConflictSerializer = new AppendEntriesConflictSerializer();

        @Override
        public RaftRpc.AppendEntriesResponse toProtobuf(AppendEntriesResponse object) {
            RaftRpc.AppendEntriesResponse.Builder builder = RaftRpc.AppendEntriesResponse.newBuilder()
                    .setLastLogIndexOnServer(object.getLastLogIndexOnServer())
                    .setSucceeded(object.isSucceeded())
                    .setTerm(object.getTerm());
            if(object.getConflictDetails() != null){
                builder.setConflictInfo(appendEntriesConflictSerializer.toProtobuf(object.getConflictDetails()));
            }
            return builder.build();
        }

        @Override
        public AppendEntriesResponse fromProtobuf(RaftRpc.AppendEntriesResponse protobufMesage) {
            AppendEntriesResponse.AppendEntriesResponseBuilder builder = AppendEntriesResponse.builder()
                    .lastLogIndexOnServer(protobufMesage.getLastLogIndexOnServer())
                    .succeeded(protobufMesage.getSucceeded())
                    .term(protobufMesage.getTerm());

            if(protobufMesage.hasConflictInfo()){
                builder.conflictDetails(appendEntriesConflictSerializer.fromProtobuf(protobufMesage.getConflictInfo()));
            }

            return builder.build();
        }
    }

    private static class RaftConfigurationSerializer implements PerClassProtoSerializer<RaftConfiguration,
            RaftRpc.RaftConfiguration>{
        static RaftPeerSerializer peerSerializer = new RaftPeerSerializer();

        @Override
        public RaftRpc.RaftConfiguration toProtobuf(RaftConfiguration config) {

            RaftRpc.RaftConfiguration.Builder builder = RaftRpc.RaftConfiguration.newBuilder()
                    .setMinElectionTimeoutMs(config.getMinElectionTimeoutInMs())
                    .setMaxElectionTimeoutMs(config.getMaxElectionTimeoutInMs())
                    .setLeaderHeartbeatIntervalMs(config.getLeaderHeartbeatIntervalInMs())
                    .addAllPeers(config.getPeerList().stream()
                        .map(i -> peerSerializer.toProtobuf(i))
                        .collect(Collectors.toList()));

            return builder.build();
        }

        @Override
        public RaftConfiguration fromProtobuf(RaftRpc.RaftConfiguration config) {
            return RaftConfiguration.builder()
                    .maxElectionTimeoutInMs(config.getMaxElectionTimeoutMs())
                    .minElectionTimeoutInMs(config.getMinElectionTimeoutMs())
                    .leaderHeartbeatIntervalInMs(config.getLeaderHeartbeatIntervalMs())
                    .peerList(config.getPeersList().stream()
                            .map(i -> peerSerializer.fromProtobuf(i))
                    .collect(Collectors.toList()))
                    .build();
        }
    }

    private static class RaftPeerSerializer implements PerClassProtoSerializer<RaftPeer,
            RaftRpc.RaftConfiguration.RaftPeer>{

        @Override
        public RaftRpc.RaftConfiguration.RaftPeer toProtobuf(RaftPeer peer) {
            return RaftRpc.RaftConfiguration.RaftPeer.newBuilder()
                    .setDnsName(peer.getDnsname())
                    .setPort(peer.getPort())
                    .setServerId(peer.getId())
                    .build();
        }

        @Override
        public RaftPeer fromProtobuf(RaftRpc.RaftConfiguration.RaftPeer peer) {
            return RaftPeer.builder()
                    .id(peer.getServerId())
                    .dnsname(peer.getDnsName())
                    .port(peer.getPort())
                    .build();
        }
    }

    private static class RaftStateSerializer implements PerClassProtoSerializer<RaftState,
            RaftRpc.PersistentState>
    {

        @Override
        public RaftRpc.PersistentState toProtobuf(RaftState state) {
            return RaftRpc.PersistentState.newBuilder()
                    .setTerm(state.getCurrentTerm())
                    .setVotedFor(state.getVotedFor())
                    .setLastCommittedIndex(state.getLastCommitedIndex())
                    .setLastAppliedIndex(state.getLastAppliedIndex())
                    .setLastGoodConfiguration(new RaftConfigurationSerializer().toProtobuf(state.getLastKnownGoodConfiguration()))
                    .build();
        }

        @Override
        public RaftState fromProtobuf(RaftRpc.PersistentState persistentState) {
            return RaftState.builder()
                    .currentTerm(persistentState.getTerm())
                    .votedFor(persistentState.getVotedFor())
                    .lastCommitedIndex(persistentState.getLastCommittedIndex())
                    .lastAppliedIndex(persistentState.getLastAppliedIndex())
                    .lastknownConfiguration(new RaftConfigurationSerializer().fromProtobuf(persistentState.getLastGoodConfiguration()))
                    .build();
        }
    }

    private static class InstallSnapshotRequestSerializer implements PerClassProtoSerializer<InstallSnapshotRequest,
            RaftRpc.InstallSnapshotRequest>
    {

        @Override
        public RaftRpc.InstallSnapshotRequest toProtobuf(InstallSnapshotRequest state) {
            return RaftRpc.InstallSnapshotRequest.newBuilder()
                    .setTerm(state.getTerm())
                    .setDone(state.isDone())
                    .setLeaderId(state.getLeaderId())
                    .setOffset(state.getOffset())
                    .setRequestServerId(state.getServerId())
                    .setLastIncludedIndex(state.getLastIncludedIndex())
                    .setLastIncludedTerm(state.getLastIncludedTerm())
                    .setData(ByteString.copyFrom(state.getData()))
                    .setConfiguration(new RaftConfigurationSerializer().toProtobuf(state.getConfiguration()))
                    .build();
        }

        @Override
        public InstallSnapshotRequest fromProtobuf(RaftRpc.InstallSnapshotRequest request) {
            return InstallSnapshotRequest.builder()
                    .term(request.getTerm())
                    .done(request.getDone())
                    .leaderId(request.getLeaderId())
                    .offset(request.getOffset())
                    .requestServerId(request.getRequestServerId())
                    .lastIncludedIndex(request.getLastIncludedIndex())
                    .lastIncludedTerm(request.getLastIncludedTerm())
                    .configuration(new RaftConfigurationSerializer().fromProtobuf(request.getConfiguration()))
                    .data(request.toByteArray())
                    .build();
        }
    }

    private static class InstallSnapshotResponseSerializer implements
            PerClassProtoSerializer<InstallSnapshotResponse,
            RaftRpc.InstallSnapshotResponse>
    {

        @Override
        public RaftRpc.InstallSnapshotResponse toProtobuf(InstallSnapshotResponse object) {
            return RaftRpc.InstallSnapshotResponse.newBuilder()
                    .setTerm(object.getTerm())
                    .setSucceeded(object.isOk())
                    .build();
        }

        @Override
        public InstallSnapshotResponse fromProtobuf(RaftRpc.InstallSnapshotResponse protobufMesage) {
            return InstallSnapshotResponse.builder()
                    .term(protobufMesage.getTerm())
                    .build();
        }
    }
}
