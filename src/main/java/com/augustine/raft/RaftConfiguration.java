package com.augustine.raft;

import com.augustine.raft.proto.ProtoSerializer;
import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.proto.RaftRpc;
import com.augustine.raft.rpc.RaftRpcClient;
import com.augustine.raft.rpc.impl.GrpcRaftClient;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Builder
@Getter
@EqualsAndHashCode(doNotUseGetters = true, exclude = "random")
public final class RaftConfiguration {

    private static final ProtoSerializer serializer = new ProtoSerializerImpl();
    private final List<RaftPeer> peerList;
    private final Random random = new Random();
    private final int minElectionTimeoutInMs;
    private final int maxElectionTimeoutInMs;
    private final int leaderHeartbeatIntervalInMs;

    public static RaftConfiguration fromBytes(@NonNull byte[] bytes) throws InvalidProtocolBufferException {
        return serializer.fromProtobuf(RaftRpc.RaftConfiguration.parseFrom(bytes));
    }

    public int getRandomizedElectionTimeout(){
        return (int)(random.nextDouble() *
                (this.maxElectionTimeoutInMs - this.minElectionTimeoutInMs))
                + this.minElectionTimeoutInMs;
    }

    public byte[] toByteArray(){
        return serializer.toProtobuf(this).toByteArray();
    }

    public Map<Long,RaftPeer> getPeers(){
        return peerList.stream().collect(Collectors.toMap(c->c.getId(), c->c));
    }
}
