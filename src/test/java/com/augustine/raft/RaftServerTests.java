package com.augustine.raft;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RaftServerTests {

    @Test
    public void testCurrentLeaderUpdateEventListener() throws InterruptedException{
        RaftServer server = new RaftServer(ServerConfiguration.fromDirectory("/tmp/raft/server1"),
                StateMachine.NOOP_MACHINE());
        Semaphore fired = new Semaphore(1);
        fired.drainPermits();
        AtomicLong newLeader = new AtomicLong();
        AtomicLong newLeaderTerm = new AtomicLong();
        server.subscribe(new RaftServer.RaftServerEventListener() {
            @Override
            public void onLeaderChange(long newL, long newLeaderT) {
                fired.release();
                newLeader.set(newL);
                newLeaderTerm.set(newLeaderT);
            }
        });


        server.tryUpdateLeader(-1, 10);
        Assert.assertTrue(fired.tryAcquire(10, TimeUnit.MILLISECONDS));
        Assert.assertEquals(-1, server.getCurrentLeaderId());
        Assert.assertEquals(newLeader.get(), server.getCurrentLeaderId());
        Assert.assertEquals(newLeaderTerm.get(), 10);

        fired.drainPermits();
        server.tryUpdateLeader(2, 10);
        Assert.assertTrue(fired.tryAcquire(10, TimeUnit.MILLISECONDS));
        Assert.assertEquals(newLeader.get(), server.getCurrentLeaderId());
        Assert.assertEquals(newLeaderTerm.get(), 10);
        Assert.assertEquals(newLeader.get(), server.getCurrentLeaderId());
        Assert.assertEquals(2, server.getCurrentLeaderId());

        fired.drainPermits();
        server.tryUpdateLeader(3, 10);
        Assert.assertEquals(2, server.getCurrentLeaderId());
        server.tryUpdateLeader(3, 11);
        Assert.assertTrue(fired.tryAcquire(10, TimeUnit.MILLISECONDS));
        Assert.assertEquals(newLeader.get(), server.getCurrentLeaderId());
        Assert.assertEquals(newLeaderTerm.get(), 11);
        Assert.assertEquals(newLeader.get(), server.getCurrentLeaderId());
        Assert.assertEquals(3, server.getCurrentLeaderId());
    }


}
