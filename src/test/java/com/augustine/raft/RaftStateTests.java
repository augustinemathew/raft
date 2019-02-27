package com.augustine.raft;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class RaftStateTests extends TestCase {

   static final RaftConfiguration raftConfiguration = RaftConfiguration.builder()
            .maxElectionTimeoutInMs(1000)
            .minElectionTimeoutInMs(500)
            .peerList(Arrays.asList(RaftPeer
                    .builder()
                    .dnsname("foobar")
                    .id(1)
                    .port(5000)
                    .build(),
                    RaftPeer
                    .builder()
                    .dnsname("foo")
                    .id(11)
                    .port(5000)
                    .build()))
            .build();

    public void testPersistence() throws IOException {
        FileUtils.deleteDirectory(new File("/tmp/state"));

        try (RaftState state = new RaftState("/tmp/state")) {
            state.setLastKnownGoodConfiguration(0,raftConfiguration);
            Assert.assertEquals(raftConfiguration, state.getLastKnownGoodConfiguration());
            Assert.assertEquals(0, state.getLastAppliedIndex());
            state.setLastAppliedIndex(10);
            Assert.assertEquals(10, state.getLastAppliedIndex());
            state.setVotedFor(1);
            Assert.assertEquals(1, state.getVotedFor());
            state.setVotedFor(-1);
            Assert.assertEquals(-1, state.getVotedFor());
            state.setCurrentTerm(100);
            Assert.assertEquals(100, state.getCurrentTerm());
            state.setVotedForAndCurrentTerm(11, 101);
            Assert.assertEquals(101, state.getCurrentTerm());
            Assert.assertEquals(11, state.getVotedFor());

            state.setLastCommitedIndex(98);

            RaftState copy = RaftState.builder()
                    .lastknownConfiguration(raftConfiguration)
                    .currentTerm(101).votedFor(11)
                    .lastAppliedIndex(10)
                    .lastCommitedIndex(98)
                    .build();
            Assert.assertEquals(copy, state);
        }

        try (RaftState state = new RaftState("/tmp/state")) {
            Assert.assertEquals(raftConfiguration, state.getLastKnownGoodConfiguration());
            Assert.assertEquals(101, state.getCurrentTerm());
            Assert.assertEquals(11, state.getVotedFor());
            Assert.assertEquals(10, state.getLastAppliedIndex());
            Assert.assertEquals(98, state.getLastCommitedIndex());
        }
    }

    public void testInvalidVoteForSetter(){
        try (RaftState state = new RaftState("/tmp/state")) {
            state.setLastKnownGoodConfiguration(10,raftConfiguration);
            try {
                state.setVotedFor(2);
                Assert.fail("Expected to fail");
            }catch (IllegalArgumentException ie){

            }
        }
    }
}
