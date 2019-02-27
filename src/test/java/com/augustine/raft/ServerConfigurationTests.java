package com.augustine.raft;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ServerConfigurationTests extends TestCase {

    public void testRaftInstanceCreation() throws IOException {
        FileUtils.deleteDirectory(new File("/tmp/raft"));

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

        List<RaftPeer> peers = new ArrayList<>();
        peers.add(peer1);
        peers.add(peer2);
        peers.add(peer3);

        RaftConfiguration configuration = RaftConfiguration.builder()
                .peerList(peers)
                .maxElectionTimeoutInMs(6000)
                .minElectionTimeoutInMs(5000)
                .build();

        ServerConfiguration.initializeServer("/tmp/raft/server1", 0, configuration);
        ServerConfiguration.initializeServer("/tmp/raft/server2", 1, configuration);
        ServerConfiguration.initializeServer("/tmp/raft/server3", 2, configuration);

        try {
            ServerConfiguration.initializeServer("/tmp/raft/server1", 0, configuration);
            Assert.fail("Expected failure because of existing state");
        }catch (IllegalStateException ie){

        }
    }
}
