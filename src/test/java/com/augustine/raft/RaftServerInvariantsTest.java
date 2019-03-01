package com.augustine.raft;

import com.augustine.raft.rpc.VoteResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RaftServerInvariantsTest {

    RaftPeer peer1 = RaftPeer.builder()
            .port(8000)
            .dnsname("localhost")
            .id(0)
            .build();

    RaftPeer peer2 = RaftPeer.builder()
            .port(8001)
            .dnsname("localhost")
            .id(1)
            .build();

    RaftPeer peer3 = RaftPeer.builder()
            .port(8003)
            .dnsname("localhost")
            .id(2)
            .build();

    List<RaftPeer> peers = Arrays.asList(peer1, peer2, peer3);

    public void test() {
        RaftServer server = null;
    }


}
