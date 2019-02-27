package com.augustine.raft;

import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.rpc.RaftRpcClient;
import com.augustine.raft.rpc.impl.GrpcRaftClient;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import lombok.*;

@Builder
@Getter
@EqualsAndHashCode
public final class RaftPeer {
    private final long id;
    private final String dnsname;
    private final int port;

    public RaftRpcClient getClient(int requestTimeoutInMs) {
        return new GrpcRaftClient(dnsname, port,
                new ProtoSerializerImpl(), requestTimeoutInMs);
    }
}

