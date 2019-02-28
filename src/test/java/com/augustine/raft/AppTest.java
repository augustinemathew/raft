package com.augustine.raft;

import com.augustine.raft.wal.Log;
import com.augustine.raft.wal.LogConsistencyChecker;
import com.augustine.raft.wal.LogEntry;
import com.augustine.raft.wal.LogEntryType;
import com.augustine.raft.wal.PersistentLog;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    public void test2() throws IOException {
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
                    .leaderHeartbeatIntervalInMs(1000)
                    .build();

        ServerConfiguration.initializeServer("/tmp/raft/server1", 0, configuration);
//        ServerConfiguration.initializeServer("/tmp/raft/server2", 1, configuration);
//        ServerConfiguration.initializeServer("/tmp/raft/server3", 2, configuration);
    }

    public void testApp() {
        PersistentLog plog = new PersistentLog("/tmp/lmdb");
        List<LogEntry> le = new ArrayList<>();
        System.out.println(plog.getFirstEntryIndex());
        System.out.println(plog.getLastEntryIndex());
        for(int i=1; i<=100000;i++){
            le.add(new LogEntry(0,10, LogEntryType.NORMAL, BigInteger.valueOf(i).toByteArray()));
        }
        plog.appendLogEntries(le);
        List<LogEntry> logEntries = plog.getLogEntries(1);
        Assert.assertEquals(100000,logEntries.size());
        Assert.assertEquals(100000, plog.getLastEntryIndex());
        System.out.println(plog.getLogEntry(1).getLsn());
        plog.removeEntriesStartingFromIndex(1);

        //Test snapshotting
        plog.removeEntriesUntil(100000, new LogEntry(
                0,10,LogEntryType.SNAPSHOT,new byte[0]));

        long firstEntryIndex = plog.getFirstEntryIndex();
        final LogEntry firstLogEntry = plog.getLogEntry(firstEntryIndex);
        Assert.assertEquals(100000,
                firstLogEntry.getLsn());
        Assert.assertEquals(10, firstLogEntry.getTerm());
        Assert.assertEquals(LogEntryType.SNAPSHOT, firstLogEntry.getType());
    }

    public void testLogConsistency(){
        Log log1 = new PersistentLog("/tmp/server1");
        Log log2 =  new PersistentLog("/tmp/server2");
        LogConsistencyChecker.checkLogs(log1,log2);
        System.out.println(log1.getLastEntryIndex() + " " + log2.getLastEntryIndex());
    }

    public void test0Log(){
        try(PersistentLog plog = new PersistentLog("/tmp/lmdb1")) {
            plog.getLogEntry(0);
            plog.removeEntriesStartingFromIndex(0);
            Assert.assertEquals(0, plog.getFirstEntryIndex());
            Assert.assertEquals(   0, plog.getLastEntryIndex());
            Assert.assertTrue(plog.getLogEntry(0).getTerm() == 0);
        }
    }
}
